# Oracle -> PySpark -> PostgreSQL MDS

This is a local learning project using your existing Oracle SH schema as the source, PySpark as the ETL engine, JDBC for database connectivity, Parquet for MDS layers, and PostgreSQL as the target.

## Run
1. `cd C:\mds-project` and activate `.venv`.
2. Copy `.env.example` to `.env` and set your local passwords.
3. On Windows, validate the project environment: `powershell -ExecutionPolicy Bypass -File .\scripts\setup_windows.ps1`.
4. Start PostgreSQL: `docker compose up -d postgres` (your existing Oracle container remains separate).
5. Download drivers: `powershell -ExecutionPolicy Bypass -File .\scripts\setup_drivers.ps1`
6. Test Oracle: `python .\spark\jobs\test_oracle_connection.py`
7. Test PostgreSQL: `python .\spark\jobs\test_postgres_connection.py`
8. Run sample MDS: `python .\spark\jobs\sh_sales_mds.py`
9. Only after sample succeeds, full load: `$env:SAMPLE_LIMIT="0"; python .\spark\jobs\sh_sales_mds.py`

The first load uses 10,000 rows. `SAMPLE_LIMIT=0` means no row limit. For very large tables, later configure JDBC partitioning using real source bounds rather than invented values.

Do not modify Oracle; the pipeline reads the existing SH.SALES table.
