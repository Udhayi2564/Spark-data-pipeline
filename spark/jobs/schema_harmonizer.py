from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class SchemaHarmonizationError(Exception):
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


def normalize_column_mapping(
    dataframe: DataFrame,
    target_columns: List[Dict[str, Any]],
) -> DataFrame:

    source_columns: Dict[str, str] = {}

    for source_column in dataframe.columns:

        source_columns[
            source_column.lower()
        ] = source_column

    result_dataframe: DataFrame = dataframe

    for target_column in target_columns:

        target_name: str = str(
            target_column["name"]
        )

        source_name: str | None = source_columns.get(
            target_name.lower()
        )

        if source_name is None:
            continue

        if source_name != target_name:

            result_dataframe = result_dataframe.withColumnRenamed(
                source_name,
                target_name,
            )

    return result_dataframe


def cast_column_for_postgres(
    dataframe: DataFrame,
    column_definition: Dict[str, Any],
) -> DataFrame:

    column_name: str = str(
        column_definition["name"]
    )

    postgres_type: str = str(
        column_definition["data_type"]
    ).lower()

    if column_name not in dataframe.columns:
        return dataframe

    target_type: Any = StringType()

    if postgres_type in {
        "integer",
        "smallint",
    }:
        target_type = IntegerType()

    elif postgres_type == "bigint":
        target_type = LongType()

    elif postgres_type in {
        "numeric",
        "decimal",
    }:

        precision_value: Any = column_definition.get(
            "numeric_precision"
        )

        scale_value: Any = column_definition.get(
            "numeric_scale"
        )

        precision: int = int(
            precision_value
            if precision_value is not None
            else 38
        )

        scale: int = int(
            scale_value
            if scale_value is not None
            else 18
        )

        if precision > 38:
            precision = 38

        target_type = DecimalType(
            precision=precision,
            scale=scale,
        )

    elif postgres_type == "double precision":
        target_type = DoubleType()

    elif postgres_type == "real":
        target_type = FloatType()

    elif postgres_type in {
        "character varying",
        "character",
        "text",
    }:
        target_type = StringType()

    elif postgres_type == "boolean":
        target_type = BooleanType()

    elif postgres_type == "date":
        target_type = DateType()

    elif postgres_type in {
        "timestamp without time zone",
        "timestamp with time zone",
    }:
        target_type = TimestampType()

    else:
        target_type = StringType()

    return dataframe.withColumn(
        column_name,
        col(column_name).cast(target_type),
    )


def harmonize_dataframe(
    dataframe: DataFrame,
    target_columns: List[Dict[str, Any]],
) -> DataFrame:

    result_dataframe: DataFrame = normalize_column_mapping(
        dataframe=dataframe,
        target_columns=target_columns,
    )

    target_column_names: List[str] = [
        str(column["name"])
        for column in target_columns
    ]

    source_column_names_lower: Dict[str, str] = {}

    for source_column in result_dataframe.columns:

        source_column_names_lower[
            source_column.lower()
        ] = source_column

    for target_name in target_column_names:

        if target_name.lower() not in source_column_names_lower:
            continue

        actual_source_name: str = source_column_names_lower[
            target_name.lower()
        ]

        if actual_source_name != target_name:

            result_dataframe = result_dataframe.withColumnRenamed(
                actual_source_name,
                target_name,
            )

    for target_column in target_columns:

        target_name: str = str(
            target_column["name"]
        )

        if target_name not in result_dataframe.columns:
            continue

        result_dataframe = cast_column_for_postgres(
            dataframe=result_dataframe,
            column_definition=target_column,
        )

    return result_dataframe