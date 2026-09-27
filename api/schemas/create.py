from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CreateResult(BaseModel):
    source: str
    target: str
    table_management: List[str] = Field(default_factory=list)


class CreateRequest(BaseModel):
    provider: Optional[str] = None
    result: CreateResult
    layout: Optional[Dict[str, Any]] = None


class CreateTableResult(BaseModel):
    table: str
    status: str
    operations_executed: int


class CreateResponse(BaseModel):
    status: str
    message: str
    tables_processed: int
    results: List[CreateTableResult]