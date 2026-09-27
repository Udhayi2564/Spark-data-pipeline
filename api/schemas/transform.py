from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TransformResult(BaseModel):
    source: str
    target: str
    data_extraction: List[str] = Field(default_factory=list)
    data_management: List[str] = Field(default_factory=list)


class TransformRequest(BaseModel):
    provider: Optional[str] = None
    result: TransformResult
    layout: Optional[Dict[str, Any]] = None


class TransformTableResult(BaseModel):
    pair_index: int
    source_table: str
    target_table: str
    target_status: str
    rows_extracted: int
    rows_written: int
    status: str


class TransformResponse(BaseModel):
    status: str
    message: str
    tables_processed: int
    results: List[TransformTableResult]