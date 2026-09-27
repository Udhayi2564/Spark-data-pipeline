from typing import Dict

from fastapi import FastAPI

from api.routes.create import router as create_router
from api.routes.transform import router as transform_router


app: FastAPI = FastAPI(
    title="Oracle to PostgreSQL MDS Pipeline",
    version="1.0.0",
)


app.include_router(
    create_router
)

app.include_router(
    transform_router
)


@app.get("/")
def health_check() -> Dict[str, str]:

    return {
        "status": "success",
        "message": "Pipeline API is running",
    }