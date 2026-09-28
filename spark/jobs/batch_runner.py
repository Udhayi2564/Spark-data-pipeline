from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession

from spark.jobs.common import (
    create_spark,
    get_oracle_url,
    get_postgres_url,
    oracle_properties,
    postgres_properties,
)

from spark.jobs.schema_harmonizer import (
    harmonize_dataframe,
)

from spark.jobs.target_schema_inspector import (
    TargetSchemaError,
    get_target_columns,
    validate_target_columns,
)

from spark.jobs.transformation_engine import (
    apply_transformations,
)

from spark.jobs.migrate_state import (
    is_initial_migration,
    save_migration_state,
)

from spark.jobs.postgres_upsert import (
    PostgreSQLUpsertError,
    find_changed_rows,
    get_postgres_connection,
    get_primary_key_columns,
    upsert_rows,
)


class BatchPipelineError(Exception):

    def __init__(
        self,
        error_code: str,
        message: str,
        stage: str,
        details: Dict[str, Any] | None = None,
    ) -> None:

        self.error_code: str = error_code

        self.message: str = message

        self.stage: str = stage

        self.details: Dict[str, Any] = (
            details or {}
        )

        super().__init__(message)


def read_from_oracle(
    spark: SparkSession,
    query: str,
) -> DataFrame:

    try:

        normalized_query: str = query.strip()

        if normalized_query.endswith(";"):

            normalized_query = (
                normalized_query[:-1]
            )

        dataframe: DataFrame = (
            spark.read
            .format("jdbc")
            .option(
                "url",
                get_oracle_url(),
            )
            .option(
                "query",
                normalized_query,
            )
            .options(
                **oracle_properties()
            )
            .load()
        )

        return dataframe

    except Exception as error:

        raise BatchPipelineError(
            error_code="SOURCE_QUERY_FAILED",
            message="Oracle extraction query failed.",
            stage="source_extraction",
            details={
                "error": str(error),
            },
        ) from error


def write_to_postgres(
    dataframe: DataFrame,
    schema_name: str,
    table_name: str,
) -> int:

    rows_to_write: int = dataframe.count()

    if rows_to_write == 0:

        return 0

    full_table_name: str = (
        f"{schema_name}.{table_name}"
    )

    try:

        (
            dataframe.write
            .mode("append")
            .format("jdbc")
            .option(
                "url",
                get_postgres_url(),
            )
            .option(
                "dbtable",
                full_table_name,
            )
            .options(
                **postgres_properties()
            )
            .save()
        )

        return rows_to_write

    except Exception as error:

        error_message: str = str(error)

        error_lower: str = (
            error_message.lower()
        )

        if (
            "duplicate key value"
            in error_lower
            or "unique constraint"
            in error_lower
        ):

            raise BatchPipelineError(
                error_code="DUPLICATE_KEY",
                message=(
                    f"Target table "
                    f"{full_table_name} "
                    "already contains a conflicting key."
                ),
                stage="target_write",
                details={
                    "target_table": full_table_name,
                    "error": error_message,
                },
            ) from error

        if (
            "column" in error_lower
            and (
                "not defined" in error_lower
                or "does not exist"
                in error_lower
            )
        ):

            raise BatchPipelineError(
                error_code="TARGET_COLUMN_ERROR",
                message=(
                    f"Target table "
                    f"{full_table_name} "
                    "does not contain a compatible column."
                ),
                stage="target_write",
                details={
                    "target_table": full_table_name,
                    "error": error_message,
                },
            ) from error

        raise BatchPipelineError(
            error_code="TARGET_WRITE_FAILED",
            message=(
                f"Unable to write data to "
                f"{full_table_name}."
            ),
            stage="target_write",
            details={
                "target_table": full_table_name,
                "error": error_message,
            },
        ) from error


def extract_target_information(
    insert_statement: str,
) -> Tuple[str, str, List[str]]:

    from api.utils.ai_response_parser import (
        parse_insert_target,
    )

    return parse_insert_target(
        insert_sql=insert_statement
    )


def run_initial_full_load(
    dataframe: DataFrame,
    schema_name: str,
    table_name: str,
) -> int:

    rows_written: int = write_to_postgres(
        dataframe=dataframe,
        schema_name=schema_name,
        table_name=table_name,
    )

    return rows_written


def run_incremental_upsert(
    dataframe: DataFrame,
    schema_name: str,
    table_name: str,
    target_columns: List[str],
) -> Dict[str, int]:

    connection: Any = (
        get_postgres_connection()
    )

    try:

        primary_key_columns: List[str] = (
            get_primary_key_columns(
                connection=connection,
                schema_name=schema_name,
                table_name=table_name,
            )
        )

        if len(primary_key_columns) == 0:

            raise BatchPipelineError(
                error_code="TARGET_PRIMARY_KEY_NOT_FOUND",
                message=(
                    f"Target table "
                    f"{schema_name}.{table_name} "
                    "must have a primary key "
                    "for incremental upsert."
                ),
                stage="target_validation",
                details={
                    "target_table": (
                        f"{schema_name}.{table_name}"
                    ),
                },
            )

        (
            changed_rows,
            new_rows_count,
            updated_rows_count,
        ) = find_changed_rows(
            dataframe=dataframe,
            connection=connection,
            schema_name=schema_name,
            table_name=table_name,
            target_columns=target_columns,
            primary_key_columns=primary_key_columns,
        )

        rows_detected: int = len(
            changed_rows
        )

        rows_written: int = 0

        if rows_detected > 0:

            rows_written = upsert_rows(
                connection=connection,
                schema_name=schema_name,
                table_name=table_name,
                target_columns=target_columns,
                primary_key_columns=primary_key_columns,
                rows=changed_rows,
            )

        return {
            "rows_detected": rows_detected,
            "new_rows": new_rows_count,
            "updated_rows": updated_rows_count,
            "rows_written": rows_written,
        }

    except PostgreSQLUpsertError as error:

        raise BatchPipelineError(
            error_code="UPSERT_FAILED",
            message=str(error),
            stage="target_upsert",
            details={
                "target_table": (
                    f"{schema_name}.{table_name}"
                ),
            },
        ) from error

    finally:

        connection.close()


def run_transform_pipeline(
    data_extraction: List[str],
    data_management: List[str],
) -> List[Dict[str, Any]]:

    if len(data_extraction) != len(
        data_management
    ):

        raise BatchPipelineError(
            error_code="PAIR_COUNT_MISMATCH",
            message=(
                "The number of extraction queries "
                "and management statements must be equal."
            ),
            stage="contract_validation",
            details={
                "data_extraction_count": (
                    len(data_extraction)
                ),
                "data_management_count": (
                    len(data_management)
                ),
            },
        )

    spark: SparkSession = create_spark()

    results: List[Dict[str, Any]] = []

    try:

        for index in range(
            len(data_extraction)
        ):

            pair_index: int = index + 1

            extraction_query: str = (
                data_extraction[index]
            )

            management_statement: str = (
                data_management[index]
            )

            (
                target_schema,
                target_table,
                target_columns,
            ) = extract_target_information(
                insert_statement=management_statement
            )

            full_target_table: str = (
                f"{target_schema}.{target_table}"
            )

            print()
            print("============================================================")
            print(f"STARTING TRANSFORM PAIR {pair_index}")
            print("============================================================")
            print(f"Source target : {full_target_table}")
            print("Pipeline      : Oracle -> Spark -> PostgreSQL")
            print("------------------------------------------------------------")

            try:

                target_column_metadata: List[
                    Dict[str, Any]
                ] = get_target_columns(
                    schema_name=target_schema,
                    table_name=target_table,
                )

            except Exception as error:

                raise BatchPipelineError(
                    error_code=(
                        "TARGET_SCHEMA_INSPECTION_FAILED"
                    ),
                    message=(
                        f"Unable to inspect target table "
                        f"{full_target_table}."
                    ),
                    stage="target_validation",
                    details={
                        "target_table": (
                            full_target_table
                        ),
                        "error": str(error),
                    },
                ) from error

            if len(
                target_column_metadata
            ) == 0:

                raise BatchPipelineError(
                    error_code=(
                        "TARGET_TABLE_NOT_FOUND"
                    ),
                    message=(
                        f"Target table "
                        f"{full_target_table} "
                        "does not exist."
                    ),
                    stage="target_validation",
                    details={
                        "target_table": (
                            full_target_table
                        ),
                        "action_required": (
                            "Execute /pipeline/create "
                            "before /pipeline/transform."
                        ),
                    },
                )

            try:

                validate_target_columns(
                    schema_name=target_schema,
                    table_name=target_table,
                    required_columns=target_columns,
                )

            except TargetSchemaError as error:

                raise BatchPipelineError(
                    error_code=error.error_code,
                    message=error.message,
                    stage="target_validation",
                    details=error.details,
                ) from error

            source_dataframe: DataFrame = (
                read_from_oracle(
                    spark=spark,
                    query=extraction_query,
                )
            )

            rows_extracted: int = (
                source_dataframe.count()
            )

            print()
            print("RAW LAYER")
            print("------------------------------------------------------------")
            print(f"RAW processing started  : Oracle -> Spark")
            print(f"RAW rows extracted     : {rows_extracted}")
            print("RAW schema:")
            source_dataframe.printSchema()
            print("RAW layer processed    : SUCCESS")

            transformed_dataframe: DataFrame = (
                source_dataframe
            )

            print()
            print("HARMONIZED LAYER")
            print("------------------------------------------------------------")
            print("HARMONIZED processing  : Started")

            transformed_dataframe = (
                harmonize_dataframe(
                    dataframe=transformed_dataframe,
                    target_columns=target_column_metadata,
                )
            )

            print("HARMONIZED processing  : Completed")

            missing_after_harmonization: List[
                str
            ] = []

            actual_columns_lower: Dict[
                str,
                str,
            ] = {}

            for source_column in (
                transformed_dataframe.columns
            ):

                actual_columns_lower[
                    source_column.lower()
                ] = source_column

            for target_column in (
                target_columns
            ):

                if (
                    target_column.lower()
                    not in actual_columns_lower
                ):

                    missing_after_harmonization.append(
                        target_column
                    )

            if len(
                missing_after_harmonization
            ) > 0:

                raise BatchPipelineError(
                    error_code=(
                        "TRANSFORM_COLUMN_MISMATCH"
                    ),
                    message=(
                        f"Transformation result for "
                        f"{full_target_table} "
                        "is missing required columns."
                    ),
                    stage="schema_harmonization",
                    details={
                        "target_table": (
                            full_target_table
                        ),
                        "missing_columns": (
                            missing_after_harmonization
                        ),
                        "available_columns": (
                            transformed_dataframe.columns
                        ),
                    },
                )

            selected_columns: List[str] = []

            for target_column in (
                target_columns
            ):

                actual_column: str = (
                    actual_columns_lower[
                        target_column.lower()
                    ]
                )

                if (
                    actual_column
                    != target_column
                ):

                    transformed_dataframe = (
                        transformed_dataframe.withColumnRenamed(
                            actual_column,
                            target_column,
                        )
                    )

                selected_columns.append(
                    target_column
                )

            transformed_dataframe = (
                transformed_dataframe.select(
                    *selected_columns
                )
            )

            rows_after_transformation: int = (
                transformed_dataframe.count()
            )

            print(f"HARMONIZED rows processed : {rows_after_transformation}")
            print("HARMONIZED schema:")
            transformed_dataframe.printSchema()
            print("HARMONIZED layer processed : SUCCESS")

            print()
            print("SERVING LAYER")
            print("------------------------------------------------------------")
            print("SERVING processing      : Started")
            print(f"SERVING rows prepared   : {rows_after_transformation}")
            print(f"SERVING columns         : {', '.join(selected_columns)}")
            print("SERVING layer processed : SUCCESS")

            source_table: str = (
                extract_source_table_from_query(
                    extraction_query
                )
            )

            initial_migration: bool = (
                is_initial_migration(
                    source_table=source_table,
                    target_table=(
                        full_target_table
                    ),
                )
            )

            if initial_migration:

                print(
                    "Migration state: "
                    "INITIAL FULL LOAD"
                )

                rows_written: int = (
                    run_initial_full_load(
                        dataframe=(
                            transformed_dataframe
                        ),
                        schema_name=(
                            target_schema
                        ),
                        table_name=(
                            target_table
                        ),
                    )
                )

                save_migration_state(
                    source_table=source_table,
                    target_table=(
                        full_target_table
                    ),
                    rows_processed=(
                        rows_written
                    ),
                )

                result_status: str = (
                    "initial_load"
                )

                rows_detected: int = (
                    rows_written
                )

                new_rows: int = (
                    rows_written
                )

                updated_rows: int = 0

            else:

                print(
                    "Migration state: "
                    "INCREMENTAL UPSERT"
                )

                incremental_result: Dict[
                    str,
                    int,
                ] = run_incremental_upsert(
                    dataframe=(
                        transformed_dataframe
                    ),
                    schema_name=(
                        target_schema
                    ),
                    table_name=(
                        target_table
                    ),
                    target_columns=(
                        target_columns
                    ),
                )

                rows_detected: int = (
                    incremental_result[
                        "rows_detected"
                    ]
                )

                new_rows: int = (
                    incremental_result[
                        "new_rows"
                    ]
                )

                updated_rows: int = (
                    incremental_result[
                        "updated_rows"
                    ]
                )

                rows_written: int = (
                    incremental_result[
                        "rows_written"
                    ]
                )

                save_migration_state(
                    source_table=source_table,
                    target_table=(
                        full_target_table
                    ),
                    rows_processed=(
                        rows_written
                    ),
                )

                result_status = (
                    "incremental_upsert"
                )

            print()
            print("TARGET LOAD")
            print("------------------------------------------------------------")
            print(f"Target table            : {full_target_table}")
            print(f"Rows detected           : {rows_detected}")
            print(f"New rows                : {new_rows}")
            print(f"Updated rows            : {updated_rows}")
            print(f"Rows written            : {rows_written}")
            print("Target load             : SUCCESS")
            print("------------------------------------------------------------")
            print(
                f"TRANSFORM PAIR {pair_index} COMPLETED SUCCESSFULLY"
            )

            results.append(
                {
                    "pair_index": pair_index - 1,
                    "source_table": source_table,
                    "target_table": (
                        full_target_table
                    ),
                    "target_status": "exists",
                    "rows_extracted": (
                        rows_extracted
                    ),
                    "rows_written": (
                        rows_written
                    ),
                    "status": result_status,
                    "rows_detected": (
                        rows_detected
                    ),
                    "new_rows": new_rows,
                    "updated_rows": updated_rows,
                }
            )

        total_rows_extracted: int = sum(
            int(result["rows_extracted"])
            for result in results
        )

        total_rows_detected: int = sum(
            int(result["rows_detected"])
            for result in results
        )

        total_new_rows: int = sum(
            int(result["new_rows"])
            for result in results
        )

        total_updated_rows: int = sum(
            int(result["updated_rows"])
            for result in results
        )

        total_rows_written: int = sum(
            int(result["rows_written"])
            for result in results
        )

        print()
        print()
        print("============================================================")
        print("ORACLE -> POSTGRESQL MIGRATION COMPLETED SUCCESSFULLY")
        print("============================================================")
        print(f"Layers processed      : RAW -> HARMONIZED -> SERVING")
        print(f"Tables processed      : {len(results)}")
        print(f"Total rows extracted  : {total_rows_extracted}")
        print(f"Total rows detected   : {total_rows_detected}")
        print(f"Total new rows        : {total_new_rows}")
        print(f"Total updated rows    : {total_updated_rows}")
        print(f"Total rows written    : {total_rows_written}")
        print("Pipeline status       : SUCCESS")
        print("============================================================")

        return results

    finally:

        spark.stop()


def extract_source_table_from_query(
    query: str,
) -> str:

    import re

    pattern: str = (
        r"\bFROM\s+"
        r"([A-Za-z_][A-Za-z0-9_$#]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$#]*)?)"
    )

    match: Any = re.search(
        pattern,
        query,
        re.IGNORECASE,
    )

    if match is None:

        raise BatchPipelineError(
            error_code="SOURCE_TABLE_NOT_FOUND",
            message=(
                "Unable to identify the source table "
                "from the extraction query."
            ),
            stage="source_validation",
            details={
                "query": query,
            },
        )

    source_table: str = match.group(1)

    return source_table