from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def apply_select(
    dataframe: DataFrame,
    columns: List[str],
) -> DataFrame:

    if len(columns) == 0:
        return dataframe

    return dataframe.select(
        *columns
    )


def apply_rename(
    dataframe: DataFrame,
    rename_mapping: Dict[str, str],
) -> DataFrame:

    result_dataframe: DataFrame = dataframe

    for old_name, new_name in rename_mapping.items():

        if old_name in result_dataframe.columns:

            result_dataframe = result_dataframe.withColumnRenamed(
                old_name,
                new_name,
            )

    return result_dataframe


def apply_cast(
    dataframe: DataFrame,
    cast_mapping: Dict[str, str],
) -> DataFrame:

    result_dataframe: DataFrame = dataframe

    for column_name, target_type in cast_mapping.items():

        if column_name in result_dataframe.columns:

            result_dataframe = result_dataframe.withColumn(
                column_name,
                col(column_name).cast(target_type),
            )

    return result_dataframe


def apply_filter(
    dataframe: DataFrame,
    filters: List[str],
) -> DataFrame:

    result_dataframe: DataFrame = dataframe

    for filter_expression in filters:

        result_dataframe = result_dataframe.filter(
            filter_expression
        )

    return result_dataframe


def apply_deduplication(
    dataframe: DataFrame,
    deduplicate_columns: List[str],
) -> DataFrame:

    if len(deduplicate_columns) == 0:
        return dataframe

    return dataframe.dropDuplicates(
        deduplicate_columns
    )


def apply_transformations(
    dataframe: DataFrame,
    select_columns: List[str],
    rename_mapping: Dict[str, str],
    cast_mapping: Dict[str, str],
    filters: List[str],
    deduplicate_columns: List[str],
) -> DataFrame:

    result_dataframe: DataFrame = dataframe

    result_dataframe = apply_select(
        dataframe=result_dataframe,
        columns=select_columns,
    )

    result_dataframe = apply_rename(
        dataframe=result_dataframe,
        rename_mapping=rename_mapping,
    )

    result_dataframe = apply_cast(
        dataframe=result_dataframe,
        cast_mapping=cast_mapping,
    )

    result_dataframe = apply_filter(
        dataframe=result_dataframe,
        filters=filters,
    )

    result_dataframe = apply_deduplication(
        dataframe=result_dataframe,
        deduplicate_columns=deduplicate_columns,
    )

    return result_dataframe