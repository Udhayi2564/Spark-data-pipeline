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

from spark.jobs.target_manager import (
    TargetManagerError,
)

from spark.jobs.transformation_engine import (
    apply_transformations,
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
        self.details: Dict[str, Any] = details or {}

        super().__init__(message)


def read_from_oracle(
    spark: SparkSession,
    query: str,
) -> DataFrame:

    try:

        normalized_query: str = query.strip()

        if normalized_query.endswith(";"):
            normalized_query = normalized_query[:-1]

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

        error_lower: str = error_message.lower()

        if (
            "duplicate key value" in error_lower
            or "unique constraint" in error_lower
        ):

            raise BatchPipelineError(
                error_code="DUPLICATE_KEY",
                message=(
                    f"Target table {full_table_name} "
                    "already contains a conflicting key."
                ),
                stage="target_write",
                details={
                    "target_table": full_table_name,
                    "error": error_message,
                    "action_required": (
                        "Use an upsert strategy for repeated loads "
                        "or clear the target rows for a clean full load."
                    ),
                },
            ) from error

        if (
            "column" in error_lower
            and (
                "not defined" in error_lower
                or "does not exist" in error_lower
            )
        ):

            raise BatchPipelineError(
                error_code="TARGET_COLUMN_ERROR",
                message=(
                    f"Target table {full_table_name} "
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


def run_transform_pipeline(
    data_extraction: List[str],
    data_management: List[str],
) -> List[Dict[str, Any]]:

    if len(data_extraction) != len(data_management):

        raise BatchPipelineError(
            error_code="PAIR_COUNT_MISMATCH",
            message=(
                "The number of extraction queries and "
                "management statements must be equal."
            ),
            stage="contract_validation",
            details={
                "data_extraction_count": len(data_extraction),
                "data_management_count": len(data_management),
            },
        )

    spark: SparkSession = create_spark()

    results: List[Dict[str, Any]] = []

    try:

        for index in range(len(data_extraction)):

            pair_index: int = index + 1

            extraction_query: str = data_extraction[index]

            management_statement: str = data_management[index]

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

            print(
                f"Starting transform pair {pair_index}: "
                f"{full_target_table}"
            )

            try:

                target_column_metadata: List[
                    Dict[str, Any]
                ] = get_target_columns(
                    schema_name=target_schema,
                    table_name=target_table,
                )

            except Exception as error:

                raise BatchPipelineError(
                    error_code="TARGET_SCHEMA_INSPECTION_FAILED",
                    message=(
                        f"Unable to inspect target table "
                        f"{full_target_table}."
                    ),
                    stage="target_validation",
                    details={
                        "target_table": full_target_table,
                        "error": str(error),
                    },
                ) from error

            if len(target_column_metadata) == 0:

                raise BatchPipelineError(
                    error_code="TARGET_TABLE_NOT_FOUND",
                    message=(
                        f"Target table {full_target_table} "
                        "does not exist."
                    ),
                    stage="target_validation",
                    details={
                        "target_table": full_target_table,
                        "action_required": (
                            "Execute /pipeline/create before "
                            "/pipeline/transform."
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

            source_dataframe: DataFrame = read_from_oracle(
                spark=spark,
                query=extraction_query,
            )

            rows_extracted: int = source_dataframe.count()

            print(
                f"Oracle rows read for pair "
                f"{pair_index}: {rows_extracted}"
            )

            print(
                "Oracle source schema:"
            )

            source_dataframe.printSchema()

            transformed_dataframe: DataFrame = (
                source_dataframe
            )

            transformed_dataframe = (
                harmonize_dataframe(
                    dataframe=transformed_dataframe,
                    target_columns=target_column_metadata,
                )
            )

            # Only write columns required by the target INSERT.
            missing_after_harmonization: List[str] = []

            actual_columns_lower: Dict[str, str] = {}

            for source_column in transformed_dataframe.columns:

                actual_columns_lower[
                    source_column.lower()
                ] = source_column

            for target_column in target_columns:

                if (
                    target_column.lower()
                    not in actual_columns_lower
                ):
                    missing_after_harmonization.append(
                        target_column
                    )

            if len(missing_after_harmonization) > 0:

                raise BatchPipelineError(
                    error_code="TRANSFORM_COLUMN_MISMATCH",
                    message=(
                        f"Transformation result for "
                        f"{full_target_table} is missing "
                        "required columns."
                    ),
                    stage="schema_harmonization",
                    details={
                        "target_table": full_target_table,
                        "missing_columns": (
                            missing_after_harmonization
                        ),
                        "available_columns": (
                            transformed_dataframe.columns
                        ),
                    },
                )

            selected_columns: List[str] = []

            for target_column in target_columns:

                actual_column: str = actual_columns_lower[
                    target_column.lower()
                ]

                if actual_column != target_column:

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

            print(
                f"Rows after transformation: "
                f"{rows_after_transformation}"
            )

            print(
                "Transformed schema:"
            )

            transformed_dataframe.printSchema()

            rows_written: int = write_to_postgres(
                dataframe=transformed_dataframe,
                schema_name=target_schema,
                table_name=target_table,
            )

            print(
                f"Completed pair {pair_index}: "
                f"{full_target_table}"
            )

            results.append(
                {
                    "pair_index": pair_index - 1,
                    "source_table": "Oracle query",
                    "target_table": full_target_table,
                    "target_status": "exists",
                    "rows_extracted": rows_extracted,
                    "rows_written": rows_written,
                    "status": "success",
                }
            )

        return results

    finally:

        spark.stop()