import re

from typing import Any, Dict, List, Optional, Tuple


def extract_source_table(
    specification: Dict[str, Any],
) -> str:
    source: Dict[str, Any] = specification["source"]
    table: str = source["table"]

    return table


def extract_source_query(
    specification: Dict[str, Any],
) -> Optional[str]:
    source: Dict[str, Any] = specification["source"]
    query: Optional[str] = source.get("query")

    return query


def extract_target_schema(
    specification: Dict[str, Any],
) -> str:
    target: Dict[str, Any] = specification["target"]
    schema: str = target["schema"]

    return schema


def extract_target_table(
    specification: Dict[str, Any],
) -> str:
    target: Dict[str, Any] = specification["target"]
    table: str = target["table"]

    return table


def extract_target_mode(
    specification: Dict[str, Any],
) -> str:
    target: Dict[str, Any] = specification["target"]
    mode: str = target["mode"]

    return mode


def extract_execution_mode(
    specification: Dict[str, Any],
) -> str:
    execution: Dict[str, Any] = specification["execution"]
    mode: str = execution["mode"]

    return mode


def extract_incremental(
    specification: Dict[str, Any],
) -> bool:
    execution: Dict[str, Any] = specification["execution"]
    incremental: bool = execution["incremental"]

    return incremental


def extract_watermark_column(
    specification: Dict[str, Any],
) -> Optional[str]:
    execution: Dict[str, Any] = specification["execution"]

    watermark_column: Optional[str] = execution.get(
        "watermark_column"
    )

    return watermark_column


def normalize_sql(
    sql: str,
) -> str:
    normalized_sql: str = sql.strip()

    if normalized_sql.endswith(";"):
        normalized_sql = normalized_sql[:-1].strip()

    return normalized_sql


def normalize_sql_list(
    statements: List[str],
) -> List[str]:
    normalized_statements: List[str] = []

    for statement in statements:
        normalized_statement: str = normalize_sql(
            statement
        )

        if normalized_statement != "":
            normalized_statements.append(
                normalized_statement
            )

    return normalized_statements

def extract_create_execution_data(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract only the fields required by the backend
    from the finalized Groq CREATE response.

    Required fields:

        result.source
        result.target
        result.table_management

    Ignored fields:

        provider
        files
        diff
        summary
        layout
    """

    result: Dict[str, Any] = payload.get(
        "result",
        {},
    )

    source: str = str(
        result.get("source", "")
    ).strip().lower()

    target: str = str(
        result.get("target", "")
    ).strip().lower()

    raw_table_management: Any = result.get(
        "table_management",
        [],
    )

    if not isinstance(
        raw_table_management,
        list,
    ):
        raise ValueError(
            "result.table_management must be a list."
        )

    table_management: List[str] = []

    for statement in raw_table_management:
        if not isinstance(statement, str):
            raise ValueError(
                "Every table_management statement "
                "must be a string."
            )

        normalized_statement: str = normalize_sql(
            statement
        )

        if normalized_statement != "":
            table_management.append(
                normalized_statement
            )

    if source == "":
        raise ValueError(
            "result.source is required."
        )

    if target == "":
        raise ValueError(
            "result.target is required."
        )

    if len(table_management) == 0:
        raise ValueError(
            "result.table_management cannot be empty."
        )

    return {
        "source": source,
        "target": target,
        "table_management": table_management,
    }


def extract_transform_execution_data(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract only the fields required by the backend
    from the finalized Gemini TRANSFORM response.

    Required fields:

        result.source
        result.target
        result.data_extraction
        result.data_management

    Ignored fields:

        provider
        files
        placeholder
        summary
        layout
    """

    result: Dict[str, Any] = payload.get(
        "result",
        {},
    )

    source: str = str(
        result.get("source", "")
    ).strip().lower()

    target: str = str(
        result.get("target", "")
    ).strip().lower()

    raw_data_extraction: Any = result.get(
        "data_extraction",
        [],
    )

    raw_data_management: Any = result.get(
        "data_management",
        [],
    )

    if not isinstance(
        raw_data_extraction,
        list,
    ):
        raise ValueError(
            "result.data_extraction must be a list."
        )

    if not isinstance(
        raw_data_management,
        list,
    ):
        raise ValueError(
            "result.data_management must be a list."
        )

    data_extraction: List[str] = []

    for statement in raw_data_extraction:
        if not isinstance(statement, str):
            raise ValueError(
                "Every data_extraction statement "
                "must be a string."
            )

        normalized_statement: str = normalize_sql(
            statement
        )

        if normalized_statement != "":
            data_extraction.append(
                normalized_statement
            )

    data_management: List[str] = []

    for statement in raw_data_management:
        if not isinstance(statement, str):
            raise ValueError(
                "Every data_management statement "
                "must be a string."
            )

        normalized_statement: str = normalize_sql(
            statement
        )

        if normalized_statement != "":
            data_management.append(
                normalized_statement
            )

    if source == "":
        raise ValueError(
            "result.source is required."
        )

    if target == "":
        raise ValueError(
            "result.target is required."
        )

    if len(data_extraction) == 0:
        raise ValueError(
            "result.data_extraction cannot be empty."
        )

    if len(data_management) == 0:
        raise ValueError(
            "result.data_management cannot be empty."
        )

    if len(data_extraction) != len(data_management):
        raise ValueError(
            "result.data_extraction and "
            "result.data_management must contain "
            "the same number of statements."
        )

    return {
        "source": source,
        "target": target,
        "data_extraction": data_extraction,
        "data_management": data_management,
    }


def extract_source_table_from_query(
    query: str,
) -> str:
    normalized_query: str = normalize_sql(
        query
    )

    pattern: str = (
        r"\bFROM\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)"
    )

    match: Optional[re.Match[str]] = re.search(
        pattern,
        normalized_query,
        re.IGNORECASE,
    )

    if match is None:
        raise ValueError(
            "Unable to extract source table "
            "from SELECT query."
        )

    source_table: str = match.group(1)

    return source_table


def extract_insert_target(
    insert_query: str,
) -> Tuple[str, List[str]]:
    normalized_query: str = normalize_sql(
        insert_query
    )

    pattern: str = (
        r"\bINSERT\s+INTO\s+"
        r"([A-Za-z_][A-Za-z0-9_$]*"
        r"(?:\.[A-Za-z_][A-Za-z0-9_$]*)?)"
        r"\s*\("
        r"(.*?)"
        r"\)"
        r"\s*VALUES\s+"
        r"\{VALUES_PLACEHOLDER\}"
        r"\s*$"
    )

    match: Optional[re.Match[str]] = re.search(
        pattern,
        normalized_query,
        re.IGNORECASE | re.DOTALL,
    )

    if match is None:
        raise ValueError(
            "Invalid INSERT template. "
            "Expected format: "
            "INSERT INTO schema.table "
            "(column1, column2) "
            "VALUES {VALUES_PLACEHOLDER}"
        )

    target_table: str = match.group(1)

    columns_text: str = match.group(2)

    target_columns: List[str] = []

    raw_columns: List[str] = columns_text.split(",")

    for raw_column in raw_columns:
        column: str = raw_column.strip()

        column = column.strip('"')

        if column != "":
            target_columns.append(column)

    if len(target_columns) == 0:
        raise ValueError(
            "INSERT template does not contain "
            "any target columns."
        )

    return (
        target_table,
        target_columns,
    )


def extract_insert_target_details(
    insert_query: str,
) -> Tuple[str, str, List[str]]:
    target_table: str
    target_columns: List[str]

    (
        target_table,
        target_columns,
    ) = extract_insert_target(
        insert_query
    )

    if "." in target_table:
        parts: List[str] = target_table.split(
            ".",
            1,
        )

        target_schema: str = parts[0].strip('"')

        table_name: str = parts[1].strip('"')

    else:
        target_schema = "public"

        table_name = target_table.strip('"')

    return (
        target_schema,
        table_name,
        target_columns,
    )


def validate_select_query(
    query: str,
) -> None:
    normalized_query: str = normalize_sql(
        query
    )

    if normalized_query == "":
        raise ValueError(
            "Source query cannot be empty."
        )

    select_pattern: str = (
        r"^\s*SELECT\b"
    )

    if re.search(
        select_pattern,
        normalized_query,
        re.IGNORECASE,
    ) is None:
        raise ValueError(
            "Source extraction query must "
            "start with SELECT."
        )



def validate_insert_template(
    insert_query: str,
) -> None:
    extract_insert_target(
        insert_query
    )



def validate_create_statements(
    table_management: List[str],
) -> None:
    if len(table_management) == 0:
        raise ValueError(
            "table_management cannot be empty."
        )

    for statement in table_management:
        normalized_statement: str = normalize_sql(
            statement
        )

        if normalized_statement == "":
            continue

        create_pattern: str = (
            r"^\s*CREATE\s+TABLE\b"
        )

        alter_pattern: str = (
            r"^\s*ALTER\s+TABLE\b"
        )

        drop_pattern: str = (
            r"^\s*DROP\s+TABLE\b"
        )

        if re.search(
            drop_pattern,
            normalized_statement,
            re.IGNORECASE,
        ) is not None:
            raise ValueError(
                "DROP TABLE operations are not "
                "allowed through the create pipeline."
            )

        is_create: bool = (
            re.search(
                create_pattern,
                normalized_statement,
                re.IGNORECASE,
            )
            is not None
        )

        is_alter: bool = (
            re.search(
                alter_pattern,
                normalized_statement,
                re.IGNORECASE,
            )
            is not None
        )

        if not is_create and not is_alter:
            raise ValueError(
                "Only CREATE TABLE and ALTER TABLE "
                "statements are allowed in "
                "table_management."
            )


def validate_transform_pairs(
    data_extraction: List[str],
    data_management: List[str],
) -> None:
    if len(data_extraction) != len(
        data_management
    ):
        raise ValueError(
            "data_extraction and "
            "data_management must contain "
            "the same number of statements."
        )

    if len(data_extraction) == 0:
        raise ValueError(
            "Transform response cannot contain "
            "empty data_extraction."
        )

    for extraction_query in data_extraction:
        validate_select_query(
            extraction_query
        )

    for management_query in data_management:
        validate_insert_template(
            management_query
        )


def build_transform_pairs(
    data_extraction: List[str],
    data_management: List[str],
) -> List[Dict[str, Any]]:
    validate_transform_pairs(
        data_extraction=data_extraction,
        data_management=data_management,
    )

    pairs: List[Dict[str, Any]] = []

    for index in range(
        len(data_extraction)
    ):
        extraction_query: str = normalize_sql(
            data_extraction[index]
        )

        management_query: str = normalize_sql(
            data_management[index]
        )

        source_table: str = (
            extract_source_table_from_query(
                extraction_query
            )
        )

        target_schema: str
        target_table: str
        target_columns: List[str]

        (
            target_schema,
            target_table,
            target_columns,
        ) = extract_insert_target_details(
            management_query
        )

        pair: Dict[str, Any] = {
            "pair_index": index,
            "source_table": source_table,
            "source_query": extraction_query,
            "target_schema": target_schema,
            "target_table": target_table,
            "target_columns": target_columns,
            "target_insert": management_query,
        }

        pairs.append(pair)

    return pairs