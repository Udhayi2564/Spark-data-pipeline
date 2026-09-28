import os
from typing import Any, Dict, List

import psycopg2
from dotenv import load_dotenv


load_dotenv()


class TargetSchemaError(Exception):
    def __init__(
        self,
        error_code: str,
        message: str,
        details: Dict[str, Any] | None = None,
    ) -> None:
        self.error_code: str = error_code
        self.message: str = message
        self.details: Dict[str, Any] = details or {}

        super().__init__(message)


def get_postgres_connection() -> Any:

    host: str = os.getenv(
        "POSTGRES_HOST",
        "localhost",
    )

    port: int = int(
        os.getenv(
            "POSTGRES_PORT",
            "5433",
        )
    )

    database: str = os.getenv(
        "POSTGRES_DB",
        "etl_target",
    )

    user: str = os.getenv(
        "POSTGRES_USER",
        "postgres",
    )

    password: str = os.getenv(
        "POSTGRES_PASSWORD",
        "",
    )

    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def get_target_columns(
    schema_name: str,
    table_name: str,
) -> List[Dict[str, Any]]:

    connection: Any = get_postgres_connection()

    try:
        cursor: Any = connection.cursor()

        cursor.execute(
            """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                numeric_precision,
                numeric_scale,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (
                schema_name,
                table_name,
            ),
        )

        rows: List[Any] = cursor.fetchall()

        cursor.close()

        columns: List[Dict[str, Any]] = []

        for row in rows:

            columns.append(
                {
                    "name": str(row[0]),
                    "data_type": str(row[1]),
                    "udt_name": str(row[2]),
                    "nullable": str(row[3]) == "YES",
                    "numeric_precision": row[4],
                    "numeric_scale": row[5],
                    "character_maximum_length": row[6],
                }
            )

        return columns

    finally:
        connection.close()


def target_table_exists(
    schema_name: str,
    table_name: str,
) -> bool:

    columns: List[Dict[str, Any]] = get_target_columns(
        schema_name=schema_name,
        table_name=table_name,
    )

    return len(columns) > 0


def validate_target_columns(
    schema_name: str,
    table_name: str,
    required_columns: List[str],
) -> Dict[str, Any]:

    actual_columns: List[Dict[str, Any]] = get_target_columns(
        schema_name=schema_name,
        table_name=table_name,
    )

    if len(actual_columns) == 0:

        raise TargetSchemaError(
            error_code="TARGET_TABLE_NOT_FOUND",
            message=(
                f"Target table {schema_name}.{table_name} "
                "does not exist."
            ),
            details={
                "target_table": f"{schema_name}.{table_name}",
            },
        )

    actual_names: Dict[str, str] = {}

    for column in actual_columns:

        actual_name: str = str(column["name"])

        actual_names[
            actual_name.lower()
        ] = actual_name

    missing_columns: List[str] = []

    for required_column in required_columns:

        normalized_required: str = required_column.lower()

        if normalized_required not in actual_names:
            missing_columns.append(
                required_column
            )

    if len(missing_columns) > 0:

        raise TargetSchemaError(
            error_code="TARGET_SCHEMA_MISMATCH",
            message=(
                f"Target table {schema_name}.{table_name} "
                "is missing required columns."
            ),
            details={
                "target_table": f"{schema_name}.{table_name}",
                "missing_columns": missing_columns,
                "actual_columns": [
                    column["name"]
                    for column in actual_columns
                ],
            },
        )

    return {
        "target_table": f"{schema_name}.{table_name}",
        "columns": actual_columns,
        "status": "compatible",
    }