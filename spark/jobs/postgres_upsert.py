from __future__ import annotations

import os
from typing import Any, Dict, List, Sequence, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from dotenv import load_dotenv


load_dotenv()


class PostgreSQLUpsertError(Exception):
    pass


def get_postgres_connection() -> Any:
    host: str = os.getenv("POSTGRES_HOST", "localhost")

    port_value: str = os.getenv(
        "POSTGRES_PORT",
        "5432",
    )

    port: int = int(port_value)

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

    connection: Any = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )

    return connection


def get_primary_key_columns(
    connection: Any,
    schema_name: str,
    table_name: str,
) -> List[str]:

    query: str = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """

    cursor: Any = connection.cursor()

    try:
        cursor.execute(
            query,
            (
                schema_name,
                table_name,
            ),
        )

        rows: List[Tuple[str]] = cursor.fetchall()

        columns: List[str] = [
            row[0]
            for row in rows
        ]

        return columns

    finally:
        cursor.close()


def _split_table_name(
    full_table_name: str,
) -> Tuple[str, str]:

    parts: List[str] = [
        part.strip()
        for part in full_table_name.split(".")
    ]

    if len(parts) != 2:
        raise PostgreSQLUpsertError(
            "Target table must be in schema.table format."
        )

    schema_name: str = parts[0]
    table_name: str = parts[1]

    return schema_name, table_name


def _quote_identifier(
    identifier: str,
) -> str:
    return sql.Identifier(identifier).as_string(
        get_postgres_connection()
    )


def get_existing_rows(
    connection: Any,
    schema_name: str,
    table_name: str,
    primary_key_columns: Sequence[str],
    target_columns: Sequence[str],
) -> Dict[Tuple[Any, ...], Tuple[Any, ...]]:

    if not primary_key_columns:
        raise PostgreSQLUpsertError(
            "Target table must have a primary key."
        )

    columns_sql: sql.SQL = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in target_columns
    )

    query: sql.SQL = sql.SQL(
        "SELECT {columns} FROM {schema}.{table}"
    ).format(
        columns=columns_sql,
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(table_name),
    )

    cursor: Any = connection.cursor()

    try:
        cursor.execute(query)

        rows: List[Tuple[Any, ...]] = cursor.fetchall()

        column_indexes: Dict[str, int] = {
            column: index
            for index, column in enumerate(target_columns)
        }

        primary_key_indexes: List[int] = [
            column_indexes[column]
            for column in primary_key_columns
        ]

        existing_rows: Dict[
            Tuple[Any, ...],
            Tuple[Any, ...]
        ] = {}

        for row in rows:
            key: Tuple[Any, ...] = tuple(
                row[index]
                for index in primary_key_indexes
            )

            existing_rows[key] = row

        return existing_rows

    finally:
        cursor.close()


def find_changed_rows(
    dataframe: Any,
    connection: Any,
    schema_name: str,
    table_name: str,
    target_columns: Sequence[str],
    primary_key_columns: Sequence[str],
) -> Tuple[List[Tuple[Any, ...]], int, int]:

    existing_rows: Dict[
        Tuple[Any, ...],
        Tuple[Any, ...]
    ] = get_existing_rows(
        connection=connection,
        schema_name=schema_name,
        table_name=table_name,
        primary_key_columns=primary_key_columns,
        target_columns=target_columns,
    )

    column_indexes: Dict[str, int] = {
        column: index
        for index, column in enumerate(target_columns)
    }

    primary_key_indexes: List[int] = [
        column_indexes[column]
        for column in primary_key_columns
    ]

    changed_rows: List[Tuple[Any, ...]] = []
    new_rows_count: int = 0
    updated_rows_count: int = 0

    for spark_row in dataframe.collect():

        row_values: Tuple[Any, ...] = tuple(
            spark_row[column]
            for column in target_columns
        )

        primary_key: Tuple[Any, ...] = tuple(
            row_values[index]
            for index in primary_key_indexes
        )

        existing_row: Tuple[Any, ...] | None = existing_rows.get(
            primary_key
        )

        if existing_row is None:
            changed_rows.append(row_values)
            new_rows_count += 1
            continue

        if existing_row != row_values:
            changed_rows.append(row_values)
            updated_rows_count += 1

    return changed_rows, new_rows_count, updated_rows_count


def upsert_rows(
    connection: Any,
    schema_name: str,
    table_name: str,
    target_columns: Sequence[str],
    primary_key_columns: Sequence[str],
    rows: Sequence[Tuple[Any, ...]],
) -> int:

    if not rows:
        return 0

    if not primary_key_columns:
        raise PostgreSQLUpsertError(
            "Target table must have a primary key for upsert."
        )

    primary_key_set: set[str] = set(
        primary_key_columns
    )

    update_columns: List[str] = [
        column
        for column in target_columns
        if column not in primary_key_set
    ]

    columns_sql: sql.SQL = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in target_columns
    )

    conflict_sql: sql.SQL = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in primary_key_columns
    )

    if update_columns:

        update_sql: sql.SQL = sql.SQL(", ").join(
            sql.SQL("{column} = EXCLUDED.{column}").format(
                column=sql.Identifier(column)
            )
            for column in update_columns
        )

        statement: sql.SQL = sql.SQL(
            """
            INSERT INTO {schema}.{table}
            ({columns})
            VALUES %s
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET
                {updates}
            """
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=columns_sql,
            conflict_columns=conflict_sql,
            updates=update_sql,
        )

    else:

        statement = sql.SQL(
            """
            INSERT INTO {schema}.{table}
            ({columns})
            VALUES %s
            ON CONFLICT ({conflict_columns})
            DO NOTHING
            """
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            columns=columns_sql,
            conflict_columns=conflict_sql,
        )

    cursor: Any = connection.cursor()

    try:
        execute_values(
            cursor,
            statement.as_string(connection),
            list(rows),
            page_size=1000,
        )

        connection.commit()

        return len(rows)

    except Exception as error:

        connection.rollback()

        raise PostgreSQLUpsertError(
            f"PostgreSQL upsert failed: {error}"
        ) from error

    finally:
        cursor.close()