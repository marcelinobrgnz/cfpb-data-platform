"""
Local Airflow (2.x) — CFPB peak-DE orchestration.

Pipeline:
  validate_local → (optional note) → snowflake_load_gate → dbt_build → dq_gate

Fail-on-red-tests: dbt build exits non-zero → task fails → email/log alert hook.

Run with: docker compose -f airflow/docker-compose.yml up -d
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.environ.get(
    "CFPB_PROJECT_ROOT",
    "/opt/airflow/cfpb-data-platform",
)


def fail_if_contract_red(**_context):
    """Fail DAG if data_quality/contracts.json status is FAIL."""
    import json

    path = Path(PROJECT_ROOT) / "data_quality" / "contracts.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing contract file: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    status = data.get("status")
    if status == "FAIL":
        raise AssertionError(f"Data quality contract FAIL: {data}")
    print(f"Contract status={status} OK")


default_args = {
    "owner": "marcelino",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cfpb_snowflake_dbt_pipeline",
    description="CFPB Parquet → Snowflake → dbt build (fail on test failure)",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cfpb", "snowflake", "dbt", "portfolio"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_parquet",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/02_ingest_to_parquet.py"
        ),
    )

    validate_local = BashOperator(
        task_id="validate_local_rowcounts",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/03_validate_rowcounts.py"
        ),
    )

    # Manual Snowflake PUT/COPY is usually done once in worksheet on portfolio day.
    # This task re-validates warehouse counts when SNOWFLAKE_* env is present.
    validate_warehouse = BashOperator(
        task_id="validate_snowflake_rowcounts",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python scripts/03_validate_rowcounts.py"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt_cfpb && "
            "dbt build --profiles-dir . --project-dir ."
        ),
        env={
            **os.environ,
            # Ensure dbt sees Snowflake secrets from Airflow env
        },
    )

    dq_gate = PythonOperator(
        task_id="dq_contract_gate",
        python_callable=fail_if_contract_red,
    )

    alert_on_failure = BashOperator(
        task_id="alert_if_upstream_failed",
        trigger_rule="one_failed",
        bash_command=(
            'echo "ALERT: CFPB pipeline failed — dbt tests or contracts red. '
            'Check Airflow logs." && exit 1'
        ),
    )

    ingest >> validate_local >> validate_warehouse >> dbt_build >> dq_gate
    [ingest, validate_local, validate_warehouse, dbt_build, dq_gate] >> alert_on_failure
