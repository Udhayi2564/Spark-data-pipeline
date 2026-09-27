import os
import re

from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from dotenv import load_dotenv


load_dotenv()


class TargetManagerError(Exception):

    def __init__(
        self,
        error_code: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
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

    try:

        connection: Any = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

        return connection

    except Exception as error:

        raise TargetManagerError(
            error_code="TARGET_CONNECTION_FAILED",
            message="Unable to connect to PostgreSQL.",
            details={
                "database": database,
                "host": host,
                "port": port,
                "error": str(error),
            },
        ) from error


def validate_create_statements(
    statements: List[str],
) -> None:

    allowed_pattern: re.Pattern[str] = re.compile(
        r"^\s*(CREATE\s+TABLE|ALTER\s+TABLE)\b",
        re.IGNORECASE,
    )

    destructive_pattern: re.Pattern[str] = re.compile(
        r"^\s*(DROP|TRUNCATE|DELETE)\b",
        re.IGNORECASE,
    )

    for statement in statements:

        if destructive_pattern.search(statement):

            raise TargetManagerError(
                error_code="DESTRUCTIVE_DDL_BLOCKED",
                message=(
                    "DROP, TRUNCATE and DELETE operations are blocked "
                    "by the target management API."
                ),
                details={
                    "statement": statement,
                },
            )

        if not allowed_pattern.search(statement):

            raise TargetManagerError(
                error_code="UNSUPPORTED_DDL",
                message=(
                    "Only CREATE TABLE and ALTER TABLE statements "
                    "are supported."
                ),
                details={
                    "statement": statement,
                },
            )


def normalize_identifier(
    identifier: str,
) -> str:

    value: str = identifier.strip()

    if value.startswith('"') and value.endswith('"'):

        value = value[1:-1]

    return value


def split_table_name(
    table_name: str,
) -> Tuple[str, str]:

    normalized_name: str = table_name.strip()

    parts: List[str] = [
        normalize_identifier(part)
        for part in normalized_name.split(".")
    ]

    if len(parts) == 1:

        return (
            "public",
            parts[0],
        )

    if len(parts) == 2:

        return (
            parts[0],
            parts[1],
        )

    raise TargetManagerError(
        error_code="INVALID_TABLE_NAME",
        message="Invalid PostgreSQL table name.",
        details={
            "table": table_name,
        },
    )


def extract_table_name_from_ddl(
    statement: str,
) -> Optional[str]:

    create_match: Optional[re.Match[str]] = re.search(
        r"CREATE\s+TABLE\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)",
        statement,
        re.IGNORECASE,
    )

    if create_match is not None:

        return create_match.group(1)

    alter_match: Optional[re.Match[str]] = re.search(
        r"ALTER\s+TABLE\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)",
        statement,
        re.IGNORECASE,
    )

    if alter_match is not None:

        return alter_match.group(1)

    return None


def target_table_exists(
    schema_name: str,
    table_name: str,
    connection: Optional[Any] = None,
) -> bool:

    own_connection: bool = connection is None

    active_connection: Any = (
        connection
        if connection is not None
        else get_postgres_connection()
    )

    try:

        cursor: Any = active_connection.cursor()

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
            );
            """,
            (
                schema_name,
                table_name,
            ),
        )

        row: Any = cursor.fetchone()

        exists: bool = bool(row[0])

        cursor.close()

        return exists

    finally:

        if own_connection:

            active_connection.close()


def target_column_exists(
    schema_name: str,
    table_name: str,
    column_name: str,
    connection: Any,
) -> bool:

    cursor: Any = connection.cursor()

    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
        );
        """,
        (
            schema_name,
            table_name,
            column_name,
        ),
    )

    row: Any = cursor.fetchone()

    cursor.close()

    return bool(row[0])


def get_column_type(
    schema_name: str,
    table_name: str,
    column_name: str,
    connection: Any,
) -> Optional[str]:

    cursor: Any = connection.cursor()

    cursor.execute(
        """
        SELECT
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s;
        """,
        (
            schema_name,
            table_name,
            column_name,
        ),
    )

    row: Any = cursor.fetchone()

    cursor.close()

    if row is None:

        return None

    data_type: str = str(row[0]).lower()

    character_length: Optional[int] = row[1]

    numeric_precision: Optional[int] = row[2]

    numeric_scale: Optional[int] = row[3]

    if data_type in {
        "character varying",
        "character",
    }:

        if character_length is not None:

            return (
                f"{data_type}"
                f"({character_length})"
            )

    if data_type in {
        "numeric",
        "decimal",
    }:

        if (
            numeric_precision is not None
            and numeric_scale is not None
        ):

            return (
                f"{data_type}"
                f"({numeric_precision},{numeric_scale})"
            )

    return data_type


def normalize_sql_type(
    sql_type: str,
) -> str:

    normalized: str = sql_type.strip().lower()

    normalized = re.sub(
        r"\s+",
        " ",
        normalized,
    )

    normalized = normalized.replace(
        "character varying",
        "varchar",
    )

    normalized = normalized.replace(
        "character",
        "char",
    )

    normalized = normalized.replace(
        "numeric",
        "decimal",
    )

    normalized = re.sub(
        r"\s*,\s*",
        ",",
        normalized,
    )

    normalized = re.sub(
        r"\s*\(\s*",
        "(",
        normalized,
    )

    normalized = re.sub(
        r"\s*\)",
        ")",
        normalized,
    )

    return normalized


def types_match(
    actual_type: str,
    requested_type: str,
) -> bool:

    normalized_actual: str = normalize_sql_type(
        actual_type,
    )

    normalized_requested: str = normalize_sql_type(
        requested_type,
    )

    aliases: Dict[str, str] = {
        "integer": "integer",
        "int": "integer",
        "int4": "integer",
        "bigint": "bigint",
        "int8": "bigint",
        "smallint": "smallint",
        "int2": "smallint",
        "varchar": "varchar",
        "text": "text",
        "decimal": "decimal",
        "numeric": "decimal",
    }

    actual_base: str = aliases.get(
        normalized_actual,
        normalized_actual,
    )

    requested_base: str = aliases.get(
        normalized_requested,
        normalized_requested,
    )

    return actual_base == requested_base


def extract_add_column_details(
    statement: str,
) -> Optional[Tuple[str, str, str]]:

    match: Optional[re.Match[str]] = re.search(
        r"ALTER\s+TABLE\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)"
        r"\s+ADD\s+COLUMN\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*)"
        r"\s+"
        r"([A-Za-z][A-Za-z0-9_]*(?:\s*\([^;]+\))?)",
        statement,
        re.IGNORECASE,
    )

    if match is None:

        return None

    table_name: str = match.group(1)

    column_name: str = match.group(2)

    column_type: str = match.group(3).strip()

    return (
        table_name,
        column_name,
        column_type,
    )


def extract_alter_column_type_details(
    statement: str,
) -> Optional[Tuple[str, str, str]]:
    
    match: Optional[re.Match[str]] = re.search(
        r"ALTER\s+TABLE\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)"
        r"\s+ALTER\s+COLUMN\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*)"
        r"\s+TYPE\s+"
        r"([A-Za-z][A-Za-z0-9_]*(?:\s*\([^;]+?\))?)"
        r"(?:\s+USING\s+.+)?"
        r"\s*;?\s*$",
        statement,
        re.IGNORECASE,
    )

    if match is None:

        return None

    table_name: str = match.group(1)

    column_name: str = match.group(2)

    requested_type: str = match.group(3).strip()

    return (
        table_name,
        column_name,
        requested_type,
    )


def execute_create_table(
    statement: str,
    connection: Any,
) -> Dict[str, Any]:

    table_name: Optional[str] = (
        extract_table_name_from_ddl(
            statement=statement,
        )
    )

    if table_name is None:

        raise TargetManagerError(
            error_code="INVALID_CREATE_TABLE",
            message="Unable to determine target table name.",
            details={
                "statement": statement,
            },
        )

    schema_name: str
    actual_table_name: str

    schema_name, actual_table_name = split_table_name(
        table_name=table_name,
    )

    exists: bool = target_table_exists(
        schema_name=schema_name,
        table_name=actual_table_name,
        connection=connection,
    )

    if exists:

        return {
            "table": f"{schema_name}.{actual_table_name}",
            "operation": "CREATE",
            "status": "already_exists",
        }

    cursor: Any = connection.cursor()

    cursor.execute(statement)

    cursor.close()

    return {
        "table": f"{schema_name}.{actual_table_name}",
        "operation": "CREATE",
        "status": "created",
    }


def execute_add_column(
    statement: str,
    connection: Any,
) -> Dict[str, Any]:

    details: Optional[Tuple[str, str, str]] = (
        extract_add_column_details(
            statement=statement,
        )
    )

    if details is None:

        return {
            "operation": "ALTER",
            "status": "executed",
            "statement": statement,
        }

    (
        table_name,
        column_name,
        column_type,
    ) = details

    schema_name: str
    actual_table_name: str

    schema_name, actual_table_name = split_table_name(
        table_name=table_name,
    )

    table_exists: bool = target_table_exists(
        schema_name=schema_name,
        table_name=actual_table_name,
        connection=connection,
    )

    if not table_exists:

        raise TargetManagerError(
            error_code="TARGET_TABLE_NOT_FOUND",
            message="Target table does not exist.",
            details={
                "table": f"{schema_name}.{actual_table_name}",
                "column": column_name,
            },
        )

    column_exists: bool = target_column_exists(
        schema_name=schema_name,
        table_name=actual_table_name,
        column_name=column_name,
        connection=connection,
    )

    if column_exists:

        return {
            "table": f"{schema_name}.{actual_table_name}",
            "column": column_name,
            "operation": "ADD COLUMN",
            "status": "already_exists",
        }

    cursor: Any = connection.cursor()

    cursor.execute(statement)

    cursor.close()

    return {
        "table": f"{schema_name}.{actual_table_name}",
        "column": column_name,
        "operation": "ADD COLUMN",
        "status": "added",
    }


def execute_alter_column_type(
    statement: str,
    connection: Any,
) -> Dict[str, Any]:

    details: Optional[Tuple[str, str, str]] = (
        extract_alter_column_type_details(
            statement=statement,
        )
    )

    if details is None:

        cursor: Any = connection.cursor()

        cursor.execute(statement)

        cursor.close()

        return {
            "operation": "ALTER",
            "status": "executed",
            "statement": statement,
        }

    (
        table_name,
        column_name,
        requested_type,
    ) = details

    schema_name: str
    actual_table_name: str

    schema_name, actual_table_name = split_table_name(
        table_name=table_name,
    )

    table_exists: bool = target_table_exists(
        schema_name=schema_name,
        table_name=actual_table_name,
        connection=connection,
    )

    if not table_exists:

        raise TargetManagerError(
            error_code="TARGET_TABLE_NOT_FOUND",
            message="Target table does not exist.",
            details={
                "table": f"{schema_name}.{actual_table_name}",
                "column": column_name,
            },
        )

    column_exists: bool = target_column_exists(
        schema_name=schema_name,
        table_name=actual_table_name,
        column_name=column_name,
        connection=connection,
    )

    if not column_exists:

        raise TargetManagerError(
            error_code="TARGET_COLUMN_NOT_FOUND",
            message="Target column does not exist.",
            details={
                "table": f"{schema_name}.{actual_table_name}",
                "column": column_name,
            },
        )

    actual_type: Optional[str] = get_column_type(
        schema_name=schema_name,
        table_name=actual_table_name,
        column_name=column_name,
        connection=connection,
    )

    if actual_type is None:

        raise TargetManagerError(
            error_code="TARGET_COLUMN_METADATA_FAILED",
            message="Unable to determine target column type.",
            details={
                "table": f"{schema_name}.{actual_table_name}",
                "column": column_name,
            },
        )

    if types_match(
        actual_type=actual_type,
        requested_type=requested_type,
    ):

        return {
            "table": f"{schema_name}.{actual_table_name}",
            "column": column_name,
            "operation": "ALTER COLUMN TYPE",
            "status": "already_matches",
            "actual_type": actual_type,
            "requested_type": requested_type,
        }

    cursor: Any = connection.cursor()

    cursor.execute(statement)

    cursor.close()

    return {
        "table": f"{schema_name}.{actual_table_name}",
        "column": column_name,
        "operation": "ALTER COLUMN TYPE",
        "status": "altered",
        "previous_type": actual_type,
        "new_type": requested_type,
    }


def execute_ddl_statement(
    statement: str,
    connection: Any,
) -> Dict[str, Any]:

    normalized_statement: str = statement.strip()

    if re.match(
        r"^\s*CREATE\s+TABLE\b",
        normalized_statement,
        re.IGNORECASE,
    ):

        return execute_create_table(
            statement=normalized_statement,
            connection=connection,
        )

    if re.match(
        r"^\s*ALTER\s+TABLE\b",
        normalized_statement,
        re.IGNORECASE,
    ):

        if re.search(
            r"\bADD\s+COLUMN\b",
            normalized_statement,
            re.IGNORECASE,
        ):

            return execute_add_column(
                statement=normalized_statement,
                connection=connection,
            )

        if re.search(
            r"\bALTER\s+COLUMN\b.*\bTYPE\b",
            normalized_statement,
            re.IGNORECASE | re.DOTALL,
        ):

            return execute_alter_column_type(
                statement=normalized_statement,
                connection=connection,
            )

        cursor: Any = connection.cursor()

        cursor.execute(normalized_statement)

        cursor.close()

        return {
            "operation": "ALTER",
            "status": "executed",
            "statement": normalized_statement,
        }

    raise TargetManagerError(
        error_code="UNSUPPORTED_DDL",
        message="Unsupported target management statement.",
        details={
            "statement": normalized_statement,
        },
    )


def create_target_table(
    statements: List[str],
) -> List[Dict[str, Any]]:

    validate_create_statements(
        statements=statements,
    )

    connection: Any = get_postgres_connection()

    results: List[Dict[str, Any]] = []

    try:

        for statement in statements:

            result: Dict[str, Any] = execute_ddl_statement(
                statement=statement,
                connection=connection,
            )

            results.append(result)

        connection.commit()

        return results

    except TargetManagerError:

        connection.rollback()

        raise

    except Exception as error:

        connection.rollback()

        raise TargetManagerError(
            error_code="TARGET_DDL_FAILED",
            message="Target table management operation failed.",
            details={
                "error": str(error),
                "operations_attempted": len(results) + 1,
            },
        ) from error

    finally:

        connection.close()