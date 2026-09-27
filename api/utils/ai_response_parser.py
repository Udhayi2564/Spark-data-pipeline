from typing import Any, Dict

from api.utils.request_utils import (
    build_transform_pairs,
    extract_create_execution_data,
    extract_insert_target_details,
    extract_transform_execution_data,
)


class PipelineContractError(ValueError):
    """
    Raised when the AI response does not match
    the expected pipeline contract.
    """

    def __init__(
        self,
        message: str,
        error_code: str = "INVALID_PIPELINE_CONTRACT",
        stage: str = "contract_validation",
        details: Dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.stage = stage
        self.details = details or {}


def validate_ai_response(
    payload: Dict[str, Any],
) -> None:
    if not isinstance(
        payload,
        dict,
    ):
        raise PipelineContractError(
            "AI response must be a JSON object."
        )

    if "result" not in payload:
        raise PipelineContractError(
            "AI response must contain a result object."
        )

    result: Any = payload.get(
        "result"
    )

    if not isinstance(
        result,
        dict,
    ):
        raise PipelineContractError(
            "AI response result must be a JSON object."
        )


def parse_create_response(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    validate_ai_response(
        payload
    )

    try:
        execution_data: Dict[str, Any] = (
            extract_create_execution_data(
                payload
            )
        )
    except ValueError as error:
        raise PipelineContractError(str(error)) from error

    source: str = execution_data["source"]

    target: str = execution_data["target"]

    table_management: list[str] = (
        execution_data["table_management"]
    )

    if source == "":
        raise PipelineContractError(
            "Create response source cannot be empty."
        )

    if target == "":
        raise PipelineContractError(
            "Create response target cannot be empty."
        )

    if source != "oracle":
        raise PipelineContractError(
            "Unsupported create source. "
            "Expected oracle."
        )

    if target != "postgresql":
        raise PipelineContractError(
            "Unsupported create target. "
            "Expected postgresql."
        )

    if len(table_management) == 0:
        raise PipelineContractError(
            "Create response table_management "
            "cannot be empty."
        )

    return {
        "source": source,
        "target": target,
        "table_management": table_management,
    }


def parse_transform_response(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    validate_ai_response(
        payload
    )

    try:
        execution_data: Dict[str, Any] = (
            extract_transform_execution_data(
                payload
            )
        )
    except ValueError as error:
        raise PipelineContractError(str(error)) from error

    source: str = execution_data["source"]

    target: str = execution_data["target"]

    data_extraction: list[str] = (
        execution_data["data_extraction"]
    )

    data_management: list[str] = (
        execution_data["data_management"]
    )

    if source == "":
        raise PipelineContractError(
            "Transform response source cannot be empty."
        )

    if target == "":
        raise PipelineContractError(
            "Transform response target cannot be empty."
        )

    if source != "oracle":
        raise PipelineContractError(
            "Unsupported transform source. "
            "Expected oracle."
        )

    if target != "postgresql":
        raise PipelineContractError(
            "Unsupported transform target. "
            "Expected postgresql."
        )

    if len(data_extraction) == 0:
        raise PipelineContractError(
            "Transform response data_extraction "
            "cannot be empty."
        )

    if len(data_management) == 0:
        raise PipelineContractError(
            "Transform response data_management "
            "cannot be empty."
        )

    if len(data_extraction) != len(
        data_management
    ):
        raise PipelineContractError(
            "data_extraction and data_management "
            "must contain the same number of statements."
        )

    try:
        pairs: list[Dict[str, Any]] = (
            build_transform_pairs(
                data_extraction=data_extraction,
                data_management=data_management,
            )
        )

    except ValueError as error:
        raise PipelineContractError(
            str(error)
        ) from error

    return {
        "source": source,
        "target": target,
        "data_extraction": data_extraction,
        "data_management": data_management,
        "pairs": pairs,
    }


def parse_create_request(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    return parse_create_response(payload)


def parse_transform_request(
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    return parse_transform_response(payload)


def parse_insert_target(
    insert_sql: str,
) -> tuple[str, str, list[str]]:
    try:
        return extract_insert_target_details(insert_sql)
    except ValueError as error:
        raise PipelineContractError(str(error)) from error