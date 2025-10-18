from typing import Dict, Any, Optional
from pydantic import BaseModel


class MappingConfig(BaseModel):
    table_name: str
    schema: Dict[str, str]  # column_name: sql_type
    mappings: Dict[str, str]  # output_column: input_field
    rules: Optional[Dict[str, Any]] = {}


class MapDataRequest(BaseModel):
    mapping: MappingConfig


class MapDataResponse(BaseModel):
    success: bool
    message: str
    records_processed: int
    table_name: str
