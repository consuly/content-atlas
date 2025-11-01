from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime


class DuplicateCheckConfig(BaseModel):
    """Configuration for duplicate checking behavior"""
    enabled: bool = True
    check_file_level: bool = True  # Check if entire file was already imported
    allow_duplicates: bool = False  # If True, skip row-level duplicate checking
    force_import: bool = False  # If True, skip all duplicate checks
    uniqueness_columns: Optional[List[str]] = None  # Columns to check for uniqueness
    error_message: Optional[str] = None  # Custom error message for duplicates


class MappingConfig(BaseModel):
    table_name: str
    db_schema: Dict[str, str]
    mappings: Dict[str, str]  # Maps target_column -> source_column
    rules: Dict[str, Any] = {}
    unique_columns: Optional[List[str]] = None
    check_duplicates: bool = True  # Legacy field for backward compatibility
    duplicate_check: DuplicateCheckConfig = DuplicateCheckConfig()  # New structured config


class MapDataRequest(BaseModel):
    mapping: MappingConfig


class MapDataResponse(BaseModel):
    success: bool
    message: str
    records_processed: int
    duplicates_skipped: int = 0
    table_name: str


class MapB2DataRequest(BaseModel):
    file_name: str
    mapping: MappingConfig


class ExtractB2ExcelRequest(BaseModel):
    file_name: str
    rows: int = 10


class SheetCSV(BaseModel):
    sheet_name: str
    csv_content: str


class ExtractExcelCsvResponse(BaseModel):
    success: bool
    sheets: List[SheetCSV]


class DetectB2MappingRequest(BaseModel):
    file_name: str


class DetectB2MappingResponse(BaseModel):
    success: bool
    file_type: str
    detected_mapping: MappingConfig
    columns_found: List[str]
    rows_sampled: int


class TableInfo(BaseModel):
    table_name: str
    row_count: int


class TablesListResponse(BaseModel):
    success: bool
    tables: List[TableInfo]


class ColumnInfo(BaseModel):
    name: str
    type: str
    nullable: bool


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


class MapB2DataAsyncRequest(BaseModel):
    file_name: str
    mapping: MappingConfig


class AsyncTaskStatus(BaseModel):
    task_id: str
    status: str  # pending, processing, completed, failed
    progress: int = 0
    message: str
    result: Optional[MapDataResponse] = None


class QueryDatabaseRequest(BaseModel):
    prompt: str
    max_rows: int = Field(default=100, ge=1, le=10000)
    thread_id: Optional[str] = None


class QueryDatabaseResponse(BaseModel):
    success: bool
    response: str
    executed_sql: Optional[str] = None
    data_csv: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    rows_returned: Optional[int] = None
    error: Optional[str] = None


class AnalysisMode(str, Enum):
    """Controls auto-execution behavior"""
    MANUAL = "manual"
    AUTO_HIGH_CONFIDENCE = "auto_high"
    AUTO_ALWAYS = "auto_always"


class ConflictResolutionMode(str, Enum):
    """How to handle schema conflicts"""
    ASK_USER = "ask_user"
    LLM_DECIDE = "llm_decide"
    PREFER_FLEXIBLE = "prefer_flexible"


class SchemaConflict(BaseModel):
    """Represents a conflict that needs resolution"""
    conflict_type: str
    description: str
    options: List[str]
    recommended_option: str
    reasoning: str


class AnalyzeFileResponse(BaseModel):
    """Response from file analysis"""
    success: bool
    llm_response: str
    suggested_mapping: Optional[MappingConfig] = None
    conflicts: Optional[List[SchemaConflict]] = None
    confidence_score: Optional[float] = None
    can_auto_execute: bool = False
    iterations_used: int = 0
    max_iterations: int = 5
    error: Optional[str] = None


class AnalyzeB2FileRequest(BaseModel):
    """Request to analyze a B2 file"""
    file_name: str
    sample_size: Optional[int] = None
    analysis_mode: AnalysisMode = AnalysisMode.MANUAL
    conflict_resolution: ConflictResolutionMode = ConflictResolutionMode.ASK_USER
    auto_execute_confidence_threshold: float = Field(default=0.9, ge=0.0, le=1.0)
    max_iterations: int = Field(default=5, ge=1, le=10)


class ExecuteRecommendedImportRequest(BaseModel):
    """Request to execute a recommended import"""
    analysis_id: str
    confirmed_mapping: Optional[MappingConfig] = None
    force_execute: bool = False


class ImportHistoryRecord(BaseModel):
    """Single import history record"""
    import_id: str
    import_timestamp: datetime
    file_name: str
    file_hash: str
    table_name: str
    source_type: str
    source_path: Optional[str] = None
    user_id: Optional[str] = None
    status: str
    rows_inserted: Optional[int] = None
    duplicates_found: Optional[int] = None
    duration_seconds: Optional[float] = None
    parsing_time_seconds: Optional[float] = None
    insert_time_seconds: Optional[float] = None
    error_message: Optional[str] = None


class ImportHistoryListResponse(BaseModel):
    """Response for import history list"""
    success: bool
    imports: List[ImportHistoryRecord]
    total_count: int
    limit: int
    offset: int


class ImportHistoryDetailResponse(BaseModel):
    """Response for single import detail"""
    success: bool
    import_record: ImportHistoryRecord


class ImportStatisticsResponse(BaseModel):
    """Response for import statistics"""
    success: bool
    total_imports: int
    successful_imports: int
    failed_imports: int
    total_rows_inserted: int
    total_duplicates_found: int
    avg_duration_seconds: float
    tables_affected: int
    unique_users: int
    period_days: int


class TableLineageResponse(BaseModel):
    """Response for table import lineage"""
    success: bool
    table_name: str
    imports: List[ImportHistoryRecord]
    total_imports: int
    total_rows_contributed: int


class UploadedFileInfo(BaseModel):
    """Information about an uploaded file"""
    id: str
    file_name: str
    b2_file_id: str
    b2_file_path: str
    file_size: int
    content_type: Optional[str] = None
    upload_date: Optional[datetime] = None
    status: str
    mapped_table_name: Optional[str] = None
    mapped_date: Optional[datetime] = None
    mapped_rows: Optional[int] = None
    error_message: Optional[str] = None


class UploadFileResponse(BaseModel):
    """Response from file upload"""
    success: bool
    message: str
    files: List[UploadedFileInfo]


class FileExistsResponse(BaseModel):
    """Response when file already exists"""
    success: bool
    exists: bool
    message: str
    existing_file: Optional[UploadedFileInfo] = None
    can_upload: bool = False
    upload_authorization: Optional[Dict[str, Any]] = None


class UploadedFilesListResponse(BaseModel):
    """Response for uploaded files list"""
    success: bool
    files: List[UploadedFileInfo]
    total_count: int
    limit: int
    offset: int


class UploadedFileDetailResponse(BaseModel):
    """Response for single uploaded file detail"""
    success: bool
    file: UploadedFileInfo


class DeleteFileResponse(BaseModel):
    """Response from file deletion"""
    success: bool
    message: str


class CheckDuplicateRequest(BaseModel):
    """Request to check if file is duplicate"""
    file_name: str
    file_hash: str
    file_size: int


class CheckDuplicateResponse(BaseModel):
    """Response from duplicate check"""
    success: bool
    is_duplicate: bool
    message: str
    existing_file: Optional[UploadedFileInfo] = None
    can_upload: bool = False
    upload_authorization: Optional[Dict[str, Any]] = None


class CompleteUploadRequest(BaseModel):
    """Request to complete upload after direct B2 upload"""
    file_name: str
    file_hash: str
    file_size: int
    content_type: str
    b2_file_id: str
    b2_file_path: str


class CompleteUploadResponse(BaseModel):
    """Response from upload completion"""
    success: bool
    message: str
    file: UploadedFileInfo


class AnalyzeFileInteractiveRequest(BaseModel):
    """Request for interactive file analysis with conversation"""
    file_id: str
    user_message: Optional[str] = None
    thread_id: Optional[str] = None
    max_iterations: int = Field(default=5, ge=1, le=10)


class AnalyzeFileInteractiveResponse(BaseModel):
    """Response from interactive file analysis"""
    success: bool
    thread_id: str
    llm_message: str
    needs_user_input: bool
    question: Optional[str] = None
    options: Optional[List[str]] = None
    can_execute: bool = False
    llm_decision: Optional[Dict[str, Any]] = None
    iterations_used: int = 0
    max_iterations: int = 5
    error: Optional[str] = None


class ExecuteInteractiveImportRequest(BaseModel):
    """Request to execute import from interactive session"""
    file_id: str
    thread_id: str
