# SNAPSHOTS checklist (fill during / after run)

Store private images under `docs/snapshots_private/` (gitignored) or OneDrive.  
Do **not** commit secrets / account IDs in screenshots if avoidable (crop).

| ID | Capture | Path / link | Done |
|----|---------|-------------|------|
| A | Local ingest row count + contracts.json | | [ ] |
| B | Snowflake `COUNT(*)` on `COMPLAINTS_RAW` | | [ ] |
| C | `LIST` stage files | | [ ] |
| D | `V_COMPLAINTS` sample rows | | [ ] |
| E | `dbt build` success | | [ ] |
| F | Mart tables in Snowflake | | [ ] |
| G | dbt docs (optional) | | [ ] |
| H | Airflow DAG green | | [ ] |
| I | Airflow fail-on-test alert (optional) | | [ ] |
| J | Databricks Parquet→Delta counts | | [ ] |
| K | Delta table UI | | [ ] |
| L | Architecture diagram | `architecture/architecture.png` | [ ] |
| M | Teardown proof (`SHOW DATABASES` empty of CFPB) | | [ ] |

## Metrics to record

- Local rows: ________
- Snowflake rows: ________
- dbt models built: ________
- dbt tests passed: ________
- Databricks delta rows: ________
- Approx trial credits used: ________
