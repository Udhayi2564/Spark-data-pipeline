# Architecture
Oracle SH.SALES -> Spark JDBC -> PySpark ETL -> RAW/HARMONIZED/SERVING Parquet -> PostgreSQL raw/harmonized/serving.
SQL is used only to initialize the PostgreSQL target schemas; PySpark performs ETL.
Airflow is an orchestration layer added after the manual pipeline works.
