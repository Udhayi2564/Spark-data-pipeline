import os
from pathlib import Path

import psycopg2

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    LongType,
    TimestampType
)

from common import (
    PROJECT_ROOT,
    create_spark,
    get_oracle_url,
    oracle_properties,
    require_credentials
)


SOURCE_COLUMNS: list[str] = [
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "EMAIL",
    "PHONE_NUMBER",
    "HIRE_DATE",
    "JOB_ID",
    "SALARY",
    "COMMISSION_PCT",
    "MANAGER_ID",
    "DEPARTMENT_ID",
    "UPDATED_AT"
]


WATERMARK_FILE: Path = (
    PROJECT_ROOT
    / "storage"
    / "watermark"
    / "hr_employees_watermark.txt"
)


def read_watermark() -> str:
    if not WATERMARK_FILE.exists():
        raise FileNotFoundError(
            "Watermark file does not exist: "
            f"{WATERMARK_FILE}"
        )

    watermark: str = WATERMARK_FILE.read_text(
        encoding="utf-8"
    ).strip()

    if not watermark:
        raise ValueError(
            "Watermark file is empty."
        )

    return watermark


def write_watermark(watermark: str) -> None:
    WATERMARK_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    WATERMARK_FILE.write_text(
        watermark,
        encoding="utf-8"
    )


def read_changed_employees(
    spark: SparkSession,
    watermark: str
) -> DataFrame:

    query: str = f"""
    (
        SELECT
            EMPLOYEE_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            PHONE_NUMBER,
            HIRE_DATE,
            JOB_ID,
            SALARY,
            COMMISSION_PCT,
            MANAGER_ID,
            DEPARTMENT_ID,
            UPDATED_AT
        FROM HR.EMPLOYEES
        WHERE UPDATED_AT > TO_TIMESTAMP(
            '{watermark}',
            'YYYY-MM-DD HH24:MI:SS.FF6'
        )
    ) employees_incremental
    """

    changed_df: DataFrame = (
        spark.read
        .jdbc(
            url=get_oracle_url(),
            table=query,
            properties=oracle_properties()
        )
    )

    return changed_df


def build_harmonized(
    df: DataFrame
) -> DataFrame:

    harmonized_df: DataFrame = (
        df
        .select(*SOURCE_COLUMNS)
        .withColumn(
            "EMPLOYEE_ID",
            F.col("EMPLOYEE_ID").cast(LongType())
        )
        .withColumn(
            "MANAGER_ID",
            F.col("MANAGER_ID").cast(LongType())
        )
        .withColumn(
            "DEPARTMENT_ID",
            F.col("DEPARTMENT_ID").cast(LongType())
        )
        .withColumn(
            "SALARY",
            F.col("SALARY").cast(
                DecimalType(8, 2)
            )
        )
        .withColumn(
            "COMMISSION_PCT",
            F.col("COMMISSION_PCT").cast(
                DecimalType(2, 2)
            )
        )
        .withColumn(
            "HIRE_DATE",
            F.col("HIRE_DATE").cast(
                TimestampType()
            )
        )
    )

    return harmonized_df


def get_max_updated_at(
    df: DataFrame
) -> str:

    max_row = (
        df
        .select(
            F.max("UPDATED_AT").alias(
                "MAX_UPDATED_AT"
            )
        )
        .collect()[0]
    )

    max_updated_at = max_row["MAX_UPDATED_AT"]

    if max_updated_at is None:
        raise ValueError(
            "Could not determine the new watermark."
        )

    new_watermark: str = (
        max_updated_at.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
    )

    return new_watermark


def get_postgres_connection():
    postgres_host: str = os.getenv(
        "POSTGRES_HOST",
        "localhost"
    )

    postgres_port: int = int(
        os.getenv(
            "POSTGRES_PORT",
            "5433"
        )
    )

    postgres_database: str = os.getenv(
        "POSTGRES_DB",
        "etl_target"
    )

    postgres_user: str = os.getenv(
        "POSTGRES_USER",
        "postgres"
    )

    postgres_password: str = os.getenv(
        "POSTGRES_PASSWORD",
        ""
    )

    connection = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
    )

    return connection


def upsert_postgres(
    df: DataFrame
) -> None:

    rows: list = df.collect()

    connection = get_postgres_connection()

    cursor = connection.cursor()

    try:

        for row in rows:

            cursor.execute(
                """
                INSERT INTO harmonized.hr_employees (
                    "EMPLOYEE_ID",
                    "FIRST_NAME",
                    "LAST_NAME",
                    "EMAIL",
                    "PHONE_NUMBER",
                    "HIRE_DATE",
                    "JOB_ID",
                    "SALARY",
                    "COMMISSION_PCT",
                    "MANAGER_ID",
                    "DEPARTMENT_ID",
                    "LOADED_AT"
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT ("EMPLOYEE_ID")
                DO UPDATE SET
                    "FIRST_NAME" = EXCLUDED."FIRST_NAME",
                    "LAST_NAME" = EXCLUDED."LAST_NAME",
                    "EMAIL" = EXCLUDED."EMAIL",
                    "PHONE_NUMBER" = EXCLUDED."PHONE_NUMBER",
                    "HIRE_DATE" = EXCLUDED."HIRE_DATE",
                    "JOB_ID" = EXCLUDED."JOB_ID",
                    "SALARY" = EXCLUDED."SALARY",
                    "COMMISSION_PCT" = EXCLUDED."COMMISSION_PCT",
                    "MANAGER_ID" = EXCLUDED."MANAGER_ID",
                    "DEPARTMENT_ID" = EXCLUDED."DEPARTMENT_ID",
                    "LOADED_AT" = CURRENT_TIMESTAMP
                """,
                (
                    row["EMPLOYEE_ID"],
                    row["FIRST_NAME"],
                    row["LAST_NAME"],
                    row["EMAIL"],
                    row["PHONE_NUMBER"],
                    row["HIRE_DATE"],
                    row["JOB_ID"],
                    row["SALARY"],
                    row["COMMISSION_PCT"],
                    row["MANAGER_ID"],
                    row["DEPARTMENT_ID"]
                )
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


def main() -> None:

    require_credentials()

    spark: SparkSession = create_spark()

    try:

        print(
            "\n=== HR INCREMENTAL BATCH START ==="
        )

        previous_watermark: str = read_watermark()

        print(
            "Previous watermark: "
            f"{previous_watermark}"
        )

        changed_df: DataFrame = (
            read_changed_employees(
                spark,
                previous_watermark
            )
        )

        changed_count: int = changed_df.count()

        print(
            "Changed rows received from Oracle: "
            f"{changed_count}"
        )

        if changed_count == 0:

            print(
                "No new or updated records found."
            )

            print(
                "=== HR INCREMENTAL BATCH COMPLETE ==="
            )

            return

        print(
            "\nChanged Oracle records:"
        )

        changed_df.orderBy(
            "EMPLOYEE_ID"
        ).show(
            50,
            truncate=False
        )

        harmonized_df: DataFrame = (
            build_harmonized(
                changed_df
            )
        )

        print(
            "\nHarmonized schema:"
        )

        harmonized_df.printSchema()

        print(
            "\nWriting changed rows "
            "to PostgreSQL..."
        )

        upsert_postgres(
            harmonized_df
        )

        new_watermark: str = (
            get_max_updated_at(
                changed_df
            )
        )

        write_watermark(
            new_watermark
        )

        print(
            "\nNew watermark: "
            f"{new_watermark}"
        )

        print(
            "Successfully processed "
            f"{changed_count} changed rows."
        )

        print(
            "=== HR INCREMENTAL BATCH COMPLETE ==="
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()