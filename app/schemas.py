from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from enum import Enum


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
    thread_id: Optional[str] = Field(None, description="Optional thread ID for conversation continuity. If not provided, each query starts fresh.")


class QueryDatabaseResponse(BaseModel):
    success: bool
    response: str = Field(..., description="Natural language explanation of the query and results")
    executed_sql: Optional[str] = Field(None, description="The SQL query that was executed")
    data_csv: Optional[str] = Field(None, description="Query results formatted as CSV string")
    execution_time_seconds: Optional[float] = Field(None, description="Time taken to execute the query")
    rows_returned: Optional[int] = Field(None, description="Number of rows returned by the query")
    error: Optional[str] = Field(None, description="Error message if the query failed")


# AI-Powered File Analysis Endpoints
class ImportStrategy(str, Enum):
    """Strategies for importing data into the database"""
    NEW_TABLE = "new_table"           # Create fresh table
    MERGE_EXACT = "merge_exact"       # Exact schema match
    EXTEND_TABLE = "extend_table"     # Add columns to existing
    ADAPT_DATA = "adapt_data"         # Transform to fit existing


class AnalysisMode(str, Enum):
    """Controls auto-execution behavior"""
    MANUAL = "manual"                      # User reviews and approves
    AUTO_HIGH_CONFIDENCE = "auto_high"     # Auto-execute if confidence > threshold
    AUTO_ALWAYS = "auto_always"            # Always auto-execute


class ConflictResolutionMode(str, Enum):
    """How to handle schema conflicts"""
    ASK_USER = "ask_user"              # Stop and ask for clarification
    LLM_DECIDE = "llm_decide"          # Let LLM resolve conflicts
    PREFER_FLEXIBLE = "prefer_flexible" # Use most flexible data type


class TableMatchInfo(BaseModel):
    """Information about a potential table match"""
    table_name: str
    similarity_score: float = Field(..., ge=0.0, le=1.0, description="Similarity score from 0.0 to 1.0")
    matching_columns: List[str]
    missing_columns: List[str]
    extra_columns: List[str]
    reasoning: str


class SchemaConflictInfo(BaseModel):
    """Information about a schema conflict"""
    conflict_type: str
    description: str
    options: List[str]
    recommended_option: str
    reasoning: str


class AnalyzeFileRequest(BaseModel):
    """Request to analyze a file for import strategy"""
    sample_size: Optional[int] = Field(
        default=None,
        description="Number of rows to sample. If None, auto-calculated based on file size"
    )
    analysis_mode: AnalysisMode = Field(
        default=AnalysisMode.MANUAL,
        description="Whether to require user approval before executing"
    )
    conflict_resolution: ConflictResolutionMode = Field(
        default=ConflictResolutionMode.ASK_USER,
        description="How to handle schema conflicts"
    )
    auto_execute_confidence_threshold: float = Field(
        default=0.9,
        ge=0.0,
        le=1.0,
        description="Minimum confidence for auto-execution (only used if analysis_mode is AUTO_HIGH_CONFIDENCE)"
    )
    max_iterations: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum number of LLM iterations for analysis"
    )


class AnalyzeB2FileRequest(BaseModel):
    """Request to analyze a B2 file for import strategy"""
    file_name: str = Field(..., description="B2 file name/key")
    sample_size: Optional[int] = Field(
        default=None,
        description="Number of rows to sample. If None, auto-calculated based on file size"
    )
    analysis_mode: AnalysisMode = Field(
        default=AnalysisMode.MANUAL,
        description="Whether to require user approval before executing"
    )
    conflict_resolution: ConflictResolutionMode = Field(
        default=ConflictResolutionMode.ASK_USER,
        description="How to handle schema conflicts"
    )
    auto_execute_confidence_threshold: float = Field(
        default=0.9,
        ge=0.0,
        le=1.0,
        description="Minimum confidence for auto-execution"
    )
    max_iterations: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum number of LLM iterations for analysis"
    )


class AnalyzeFileResponse(BaseModel):
    """Response from file analysis"""
    success: bool
    recommended_strategy: Optional[ImportStrategy] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence score from 0.0 to 1.0")
    reasoning: Optional[str] = None
    
    # Table matches
    table_matches: List[TableMatchInfo] = []
    selected_table: Optional[str] = None
    
    # Suggested mapping
    suggested_mapping: Optional[MappingConfig] = None
    
    # Issues and conflicts
    data_quality_issues: List[str] = []
    conflicts: List[SchemaConflictInfo] = []
    
    # Execution info
    requires_user_input: bool = False
    can_auto_execute: bool = False
    iterations_used: int = 0
    max_iterations: int = 5
    
    # Raw LLM response
    llm_response: Optional[str] = None
    
    # Error info
    error: Optional[str] = None


class ExecuteRecommendedImportRequest(BaseModel):
    """Request to execute a recommended import"""
    analysis_id: str = Field(..., description="ID of the analysis result to execute")
    confirmed_mapping: Optional[MappingConfig] = Field(
        None,
        description="User can optionally modify the suggested mapping before execution"
    )
    force_execute: bool = Field(
        default=False,
        description="Force execution even if conflicts exist"
    )


# Import History Endpoints
class ImportHistoryRecord(BaseModel):
    """Single import history record"""
    import_id: str
    import_timestamp: Optional[str] = None
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    source_type: str
    source_path: Optional[str] = None
    file_name: Optional[str] = None
    file_size_bytes: Optional[int] = None
    file_type: Optional[str] = None
    file_hash: Optional[str] = None
    table_name: str
    import_strategy: Optional[str] = None
    mapping_config: Optional[Dict[str, Any]] = None
    duplicate_check_enabled: Optional[bool] = None
    status: str
    error_message: Optional[str] = None
    warnings: Optional[List[str]] = None
    total_rows_in_file: Optional[int] = None
    rows_processed: Optional[int] = None
    rows_inserted: Optional[int] = None
    rows_skipped: Optional[int] = None
    duplicates_found: Optional[int] = None
    validation_errors: Optional[int] = None
    duration_seconds: Optional[float] = None
    parsing_time_seconds: Optional[float] = None
    duplicate_check_time_seconds: Optional[float] = None
    insert_time_seconds: Optional[float] = None
    analysis_id: Optional[str] = None
    task_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ImportHistoryListResponse(BaseModel):
    """Response for listing import history"""
    success: bool
    imports: List[ImportHistoryRecord]
    total_count: int
    limit: int
    offset: int


class ImportHistoryDetailResponse(BaseModel):
    """Response for single import history detail"""
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


# File Upload Endpoints
class UploadedFileInfo(BaseModel):
    """Information about an uploaded file"""
    id: str
    file_name: str
    b2_file_id: str
    b2_file_path: str
    file_size: int
    content_type: Optional[str] = None
    upload_date: Optional[str] = None
    status: str  # 'uploaded', 'mapping', 'mapped', 'failed'
    mapped_table_name: Optional[str] = None
    mapped_date: Optional[str] = None
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


class UploadedFilesListResponse(BaseModel):
    """Response for listing uploaded files"""
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
