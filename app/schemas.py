from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class DuplicateCheckConfig(BaseModel):
    enabled: bool = True
    check_file_level: bool = True  # Check if entire file was already imported
    uniqueness_columns: Optional[List[str]] = None  # Columns to check for uniqueness (None = all columns)
    allow_duplicates: bool = False  # If true, skip duplicate checking entirely
    force_import: bool = False  # If true, force import even with duplicates
    error_message: Optional[str] = "Duplicate data detected. The uploaded data overlaps with existing records."


class MappingConfig(BaseModel):
    table_name: str
    db_schema: Dict[str, str]  # column_name: sql_type
    mappings: Dict[str, str]  # output_column: input_field
    duplicate_check: Optional[DuplicateCheckConfig] = DuplicateCheckConfig()
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


# Frontend Query Endpoints
class TableInfo(BaseModel):
    table_name: str
    created_at: Optional[str] = None
    row_count: Optional[int] = None


class TablesListResponse(BaseModel):
    success: bool
    tables: List[TableInfo]


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool = True


class TableSchemaResponse(BaseModel):
    success: bool
    table_name: str
    columns: List[ColumnInfo]


class TableDataResponse(BaseModel):
    success: bool
    table_name: str
    data: List[Dict[str, Any]]
    total_rows: int
    limit: int
    offset: int


class TableStatsResponse(BaseModel):
    success: bool
    table_name: str
    total_rows: int
    columns_count: int
    data_types: Dict[str, str]


# Async Processing
class MapB2DataAsyncRequest(BaseModel):
    file_name: str
    mapping: MappingConfig


class AsyncTaskStatus(BaseModel):
    task_id: str
    status: str  # 'pending', 'processing', 'completed', 'failed'
    progress: Optional[int] = None  # percentage 0-100
    message: Optional[str] = None
    result: Optional[MapDataResponse] = None


# Natural Language Query Endpoints
class QueryDatabaseRequest(BaseModel):
    prompt: str = Field(..., description="Natural language query to execute against the database")
    max_rows: Optional[int] = Field(default=1000, ge=1, le=10000, description="Maximum number of rows to return")


class QueryDatabaseResponse(BaseModel):
    success: bool
    response: str = Field(..., description="Natural language explanation of the query and results")
    executed_sql: Optional[str] = Field(None, description="The SQL query that was executed")
    data_csv: Optional[str] = Field(None, description="Query results formatted as CSV string")
    execution_time_seconds: Optional[float] = Field(None, description="Time taken to execute the query")
    rows_returned: Optional[int] = Field(None, description="Number of rows returned by the query")
    error: Optional[str] = Field(None, description="Error message if the query failed")
