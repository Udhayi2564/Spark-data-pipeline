from typing import Any, Dict, List

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.schemas.create import (
    CreateRequest,
    CreateResponse,
)

from api.utils.ai_response_parser import (
    PipelineContractError,
    parse_create_request,
)

from spark.jobs.target_manager import (
    TargetManagerError,
    create_target_table,
)


router: APIRouter = APIRouter(
    prefix="/pipeline",
    tags=["Pipeline"],
)


@router.post(
    "/create",
    response_model=CreateResponse,
)
def create_pipeline(
    request: CreateRequest,
) -> Any:

    try:

        payload: Dict[str, Any] = request.model_dump()

        execution_spec: Dict[str, Any] = (
            parse_create_request(
                payload=payload,
            )
        )

        statements: List[str] = execution_spec[
            "table_management"
        ]

        execution_results: List[Dict[str, Any]] = (
            create_target_table(
                statements=statements,
            )
        )

        results: List[Dict[str, Any]] = []

        for execution_result in execution_results:

            results.append(
                {
                    "table": execution_result["table"],
                    "status": execution_result["status"],
                    "operations_executed": 1,
                }
            )

        response: Dict[str, Any] = {
            "status": "success",
            "message": (
                "Target table management completed successfully."
            ),
            "tables_processed": len(results),
            "results": results,
        }

        return response

    except PipelineContractError as error:

        return JSONResponse(
            status_code=400,
            content={
                "status": "failed",
                "error_code": error.error_code,
                "message": error.message,
                "stage": error.stage,
                "details": error.details,
            },
        )

    except TargetManagerError as error:

        return JSONResponse(
            status_code=409,
            content={
                "status": "failed",
                "error_code": error.error_code,
                "message": error.message,
                "stage": "target_management",
                "details": error.details,
            },
        )

    except Exception as error:

        return JSONResponse(
            status_code=500,
            content={
                "status": "failed",
                "error_code": "INTERNAL_ERROR",
                "message": "Unexpected pipeline error.",
                "stage": "create",
                "details": {
                    "error": str(error),
                },
            },
        )