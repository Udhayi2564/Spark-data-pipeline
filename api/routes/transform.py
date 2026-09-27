from typing import Any, Dict, List

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.schemas.transform import (
    TransformRequest,
    TransformResponse,
)

from api.utils.ai_response_parser import (
    PipelineContractError,
    parse_transform_request,
)

from spark.jobs.batch_runner import (
    BatchPipelineError,
    run_transform_pipeline,
)


router: APIRouter = APIRouter(
    prefix="/pipeline",
    tags=["Pipeline"],
)


@router.post(
    "/transform",
    response_model=TransformResponse,
)
def transform_pipeline(
    request: TransformRequest,
) -> Any:

    try:

        payload: Dict[str, Any] = request.model_dump()

        execution_spec: Dict[str, Any] = (
            parse_transform_request(
                payload=payload,
            )
        )

        data_extraction: List[str] = (
            execution_spec["data_extraction"]
        )

        data_management: List[str] = (
            execution_spec["data_management"]
        )

        results: List[Dict[str, Any]] = (
            run_transform_pipeline(
                data_extraction=data_extraction,
                data_management=data_management,
            )
        )

        return {
            "status": "success",
            "message": (
                "Transformation completed successfully."
            ),
            "tables_processed": len(results),
            "results": results,
        }

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

    except BatchPipelineError as error:

        status_code: int = 422

        if error.error_code == "TARGET_TABLE_NOT_FOUND":
            status_code = 409

        elif error.error_code == "DUPLICATE_KEY":
            status_code = 409

        return JSONResponse(
            status_code=status_code,
            content={
                "status": "failed",
                "error_code": error.error_code,
                "message": error.message,
                "stage": error.stage,
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
                "stage": "transform",
                "details": {
                    "error": str(error),
                },
            },
        )