from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class MappingConfig(BaseModel):
    table_name: str
    db_schema: Dict[str, str]  # column_name: sql_type
    mappings: Dict[str, str]  # output_column: input_field
    rules: Optional[Dict[str, Any]] = {}


class MapDataRequest(BaseModel):
    mapping: MappingConfig


class MapB2DataRequest(BaseModel):
    file_name: str  # B2 file name/key
    mapping: MappingConfig


class MapDataResponse(BaseModel):
    success: bool
    message: str
    records_processed: int
    table_name: str


class ExtractB2ExcelRequest(BaseModel):
    file_name: str  # B2 file name/key
    rows: Optional[int] = Field(default=100, ge=1, description="Number of rows to extract from each sheet")


class ExtractExcelCsvResponse(BaseModel):
    success: bool
    sheets: Dict[str, str]  # sheet_name: csv_string


class DetectB2MappingRequest(BaseModel):
    file_name: str  # B2 file name/key


class DetectB2MappingResponse(BaseModel):
    success: bool
    file_type: str  # 'csv' or 'excel'
    detected_mapping: MappingConfig
    columns_found: List[str]
    rows_sampled: int
