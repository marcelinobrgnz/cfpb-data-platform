# Architecture diagram

Create `architecture.png` (Gemini / draw.io / PowerPoint) with this exact flow:

```
CFPB CSV
   → Python ingest
   → Parquet (local / optional S3)
   → Snowflake RAW (VARIANT + typed view)
   → dbt staging → marts (+ incremental)
   → Airflow DAG (fail on dbt test red)
   → Databricks Delta (parallel lakehouse path)
```

Title: **CFPB Data Platform — peak DE (portfolio)**  
Footer: **Trial resources · teardown same day · Marcelino Braganza**

Save as `architecture/architecture.png` and attach to LinkedIn / Featured.
