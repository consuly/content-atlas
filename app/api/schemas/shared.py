import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field, field_validator


RESERVED_SYSTEM_TABLES = {
    # Import + mapping infrastructure
    "file_imports",
    "table_metadata",
    "import_history",
    "mapping_errors",
    "uploaded_files",
    "import_duplicates",
    "import_jobs",
    # Query + conversation storage
    "query_threads",
    "query_messages",
    # Core platform tables
    "users",
    "api_keys",
}

_RESERVED_TABLES_LOWER = {name.lower() for name in RESERVED_SYSTEM_TABLES}
_RESERVED_TABLE_SUFFIX = "_user_data"
logger = logging.getLogger(__name__)


def ensure_safe_table_name(requested_name: str) -> str:
    """
    Return a safe table name that won't collide with system tables.

    - Trims whitespace
    - Auto-renames reserved names using a deterministic suffix
    """
    normalized = requested_name.strip()
    if not normalized:
        return normalized

    candidate = normalized
    if candidate.lower() not in _RESERVED_TABLES_LOWER:
        return candidate

    # Append suffix until we find a safe option (rarely loops more than once)
    base = normalized
    counter = 1
    while candidate.lower() in _RESERVED_TABLES_LOWER:
        suffix = f"{_RESERVED_TABLE_SUFFIX}{counter if counter > 1 else ''}"
        candidate = f"{base}{suffix}"
        counter += 1

    logger.info(
        "Table name '%s' is reserved; automatically remapped to '%s'",
        requested_name,
        candidate,
    )
    return candidate


def is_reserved_system_table(table_name: str) -> bool:
    """Return True when the supplied table name collides with a reserved system table."""
    if not table_name:
        return False
    return table_name.strip().lower() in _RESERVED_TABLES_LOWER


class DuplicateCheckConfig(BaseModel):
    """Configuration for duplicate checking behavior"""
    enabled: bool = True
    check_file_level: bool = True  # Check if entire file was already imported
    allow_duplicates: bool = False  # If True, skip row-level duplicate checking
    force_import: bool = False  # If True, skip all duplicate checks
    uniqueness_columns: Optional[List[str]] = None  # Columns to check for uniqueness
    error_message: Optional[str] = None  # Custom error message for duplicates


class MappingErrorDetail(BaseModel):
    """Structured information about mapping errors surfaced during import."""
    type: str
    message: str
    column: Optional[str] = None
    expected_type: Optional[str] = None
    value: Optional[Any] = None
    source_field: Optional[str] = None
    target_field: Optional[str] = None


class TypeMismatchSummary(BaseModel):
    """Aggregated summary of type mismatch errors for remediation planning."""
    column: str
    expected_type: Optional[str] = None
    occurrences: int = 0
    samples: List[str] = Field(default_factory=list)


class MappingConfig(BaseModel):
    table_name: str
    db_schema: Dict[str, str]
    mappings: Dict[str, str]  # Maps target_column -> source_column
    rules: Dict[str, Any] = {}
    unique_columns: Optional[List[str]] = None
    check_duplicates: bool = True  # Legacy field for backward compatibility
    duplicate_check: DuplicateCheckConfig = DuplicateCheckConfig()  # New structured config

    @field_validator("table_name")
    def validate_table_name(cls, value: str) -> str:
        """Disallow mapping into system tables or blank names."""
        if not value:
            raise ValueError("table_name is required")
        normalized = value.strip()
        if not normalized:
            raise ValueError("table_name cannot be blank")
        return ensure_safe_table_name(normalized)


class MapDataRequest(BaseModel):
    mapping: MappingConfig


class DuplicateExistingRow(BaseModel):
    row_id: int
    record: Dict[str, Any]


class DuplicateRow(BaseModel):
    """Represents a row that was skipped because it was a duplicate."""
    id: int
    record_number: Optional[int] = None
    record: Dict[str, Any]
    existing_row: Optional[DuplicateExistingRow] = None
    detected_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolution_details: Optional[Dict[str, Any]] = None


class DuplicateDetailResponse(BaseModel):
    success: bool
    duplicate: DuplicateRow
    existing_row: Optional[DuplicateExistingRow] = None
    table_name: str
    uniqueness_columns: List[str]


class DuplicateMergeRequest(BaseModel):
    updates: Dict[str, Any] = Field(default_factory=dict)
    resolved_by: Optional[str] = None
    note: Optional[str] = None


class DuplicateMergeResponse(BaseModel):
    success: bool
    duplicate: DuplicateRow
    updated_columns: List[str]
    existing_row: Optional[DuplicateExistingRow] = None
    resolution_details: Optional[Dict[str, Any]] = None


class MapDataResponse(BaseModel):
    success: bool
    message: str
    records_processed: int
    duplicates_skipped: int = 0
    table_name: str
    import_id: Optional[str] = None
    duplicate_rows: Optional[List[DuplicateRow]] = None
    duplicate_rows_count: Optional[int] = None
    llm_followup: Optional[str] = None
    needs_user_input: Optional[bool] = None
    can_execute: Optional[bool] = None
    llm_decision: Optional[Dict[str, Any]] = None
    thread_id: Optional[str] = None
    mapping_errors: Optional[List[MappingErrorDetail]] = None
    type_mismatch_summary: Optional[List[TypeMismatchSummary]] = None
    job_id: Optional[str] = None


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
    thread_id: Optional[str] = None
    executed_sql: Optional[str] = None
    data_csv: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    rows_returned: Optional[int] = None
    error: Optional[str] = None


class QueryConversationMessage(BaseModel):
    role: str
    content: str
    timestamp: Optional[datetime] = None
    executed_sql: Optional[str] = None
    data_csv: Optional[str] = None
    execution_time_seconds: Optional[float] = None
    rows_returned: Optional[int] = None
    error: Optional[str] = None


class QueryConversation(BaseModel):
    thread_id: str
    messages: List[QueryConversationMessage]
    updated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class QueryConversationResponse(BaseModel):
    success: bool
    conversation: Optional[QueryConversation] = None
    error: Optional[str] = None


class QueryConversationSummary(BaseModel):
    thread_id: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    first_user_prompt: Optional[str] = None


class QueryConversationListResponse(BaseModel):
    success: bool
    conversations: List[QueryConversationSummary]


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
    job_id: Optional[str] = None
    auto_execution_result: Optional["AutoExecutionResult"] = None
    auto_retry_execution_result: Optional[MapDataResponse] = None
    auto_execution_error: Optional[str] = None
    auto_retry_attempted: bool = False
    auto_retry_error: Optional[str] = None


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
    import_timestamp: Optional[datetime] = None
    file_name: Optional[str] = None
    file_hash: Optional[str] = None
    table_name: str
    source_type: Optional[str] = None
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


class ImportDuplicateRowsResponse(BaseModel):
    """Response containing duplicate rows for an import."""
    success: bool
    duplicates: List[DuplicateRow]
    total_count: int
    limit: int
    offset: int


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
    active_job_id: Optional[str] = None
    active_job_status: Optional[str] = None
    active_job_stage: Optional[str] = None
    active_job_progress: Optional[int] = None
    active_job_started_at: Optional[datetime] = None


class UploadFileResponse(BaseModel):
    """Response from file upload"""
    success: bool
    message: str
    files: List[UploadedFileInfo]


class AutoExecutionResult(BaseModel):
    """Structured summary of automatic execution attempts."""
    success: bool
    strategy_executed: Optional[str] = None
    strategy_attempted: Optional[str] = None
    table_name: Optional[str] = None
    target_table: Optional[str] = None
    records_processed: Optional[int] = None
    duplicates_skipped: Optional[int] = 0
    duplicate_rows: Optional[List[DuplicateRow]] = None
    duplicate_rows_count: Optional[int] = None
    import_id: Optional[str] = None
    mapping_errors: Optional[List[MappingErrorDetail]] = None
    type_mismatch_summary: Optional[List[TypeMismatchSummary]] = None
    llm_followup: Optional[str] = None
    schema_migration_results: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


class ArchiveAutoProcessFileResult(BaseModel):
    """Per-file outcome when processing an archive."""
    archive_path: str
    stored_file_name: Optional[str] = None
    uploaded_file_id: Optional[str] = None
    status: Literal["processed", "failed", "skipped"]
    table_name: Optional[str] = None
    records_processed: Optional[int] = None
    duplicates_skipped: Optional[int] = None
    import_id: Optional[str] = None
    auto_retry_used: bool = False
    message: Optional[str] = None


class ArchiveAutoProcessResponse(BaseModel):
    """Aggregate result after processing an archive automatically."""
    success: bool
    total_files: int
    processed_files: int
    failed_files: int
    skipped_files: int
    results: List[ArchiveAutoProcessFileResult]
    job_id: Optional[str] = None


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


class ImportJobInfo(BaseModel):
    """Metadata about a long-running import job."""
    id: str
    file_id: str
    status: str
    stage: Optional[str] = None
    progress: Optional[int] = None
    retry_attempt: int = 1
    error_message: Optional[str] = None
    trigger_source: Optional[str] = None
    analysis_mode: Optional[AnalysisMode] = None
    conflict_mode: Optional[ConflictResolutionMode] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    result_metadata: Optional[Dict[str, Any]] = None


class ImportJobResponse(BaseModel):
    """Response wrapper for a single import job."""
    success: bool
    job: ImportJobInfo


class ImportJobListResponse(BaseModel):
    """Response wrapper for a list of import jobs."""
    success: bool
    jobs: List[ImportJobInfo]
    total_count: int
    limit: int
    offset: int


class AnalyzeFileInteractiveRequest(BaseModel):
    """Request for interactive file analysis with conversation"""
    file_id: str
    user_message: Optional[str] = None
    thread_id: Optional[str] = None
    max_iterations: int = Field(default=5, ge=1, le=10)
    previous_error_message: Optional[str] = None


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
    job_id: Optional[str] = None


class ExecuteInteractiveImportRequest(BaseModel):
    """Request to execute import from interactive session"""
    file_id: str
    thread_id: str
