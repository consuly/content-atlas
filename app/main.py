from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.orm import Session
import json
import uuid
from typing import Dict, Optional, List, Any, Tuple
from contextlib import asynccontextmanager
from .database import get_db, get_engine
from .schemas import (
    MapDataRequest, MapDataResponse, MappingConfig, MapB2DataRequest, 
    ExtractB2ExcelRequest, ExtractExcelCsvResponse, DetectB2MappingRequest, 
    DetectB2MappingResponse, TablesListResponse, TableInfo, TableSchemaResponse, 
    ColumnInfo, TableDataResponse, TableStatsResponse, MapB2DataAsyncRequest, 
    AsyncTaskStatus, QueryDatabaseRequest, QueryDatabaseResponse,
    AnalyzeFileResponse, AnalyzeB2FileRequest,
    ExecuteRecommendedImportRequest, AnalysisMode, ConflictResolutionMode,
    CheckDuplicateRequest, CheckDuplicateResponse, CompleteUploadRequest, CompleteUploadResponse,
    AnalyzeFileInteractiveRequest, AnalyzeFileInteractiveResponse, ExecuteInteractiveImportRequest
)
from .processors.csv_processor import process_csv, process_excel, process_large_excel, extract_excel_sheets_to_csv
from .processors.json_processor import process_json
from .processors.xml_processor import process_xml
from .mapper import map_data, detect_mapping_from_file
from .models import create_table_if_not_exists, insert_records, DuplicateDataException, FileAlreadyImportedException
from .config import settings
from .b2_utils import download_file_from_b2
from .query_agent import query_database_with_agent
from .file_analyzer import (
    analyze_file_for_import, sample_file_data, ImportStrategy
)
from .auto_import import execute_llm_import_decision
from .table_metadata import create_table_metadata_table
from .import_history import create_import_history_table
from .auth import (
    authenticate_user, create_access_token, get_current_user, 
    create_user, init_auth_tables, User
)
from .auth_schemas import UserLogin, UserRegister, AuthResponse, Token, UserResponse
from .api_key_auth import ApiKey, get_api_key_from_header
from .api_key_schemas import (
    CreateApiKeyRequest, CreateApiKeyResponse, ListApiKeysResponse, ApiKeyInfo,
    RevokeApiKeyResponse, UpdateApiKeyRequest, UpdateApiKeyResponse
)
from datetime import timedelta
import time


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown events."""
    # Startup: Initialize database tables
    try:
        from .uploaded_files import create_uploaded_files_table
        from .api_key_auth import init_api_key_tables
        
        print("Initializing database tables...")
        create_table_metadata_table()
        print("✓ table_metadata table ready")
        
        create_import_history_table()
        print("✓ import_history table ready")
        
        create_uploaded_files_table()
        # Success message printed inside function
        
        init_auth_tables()
        print("✓ auth tables ready")
        
        init_api_key_tables()
        print("✓ api_keys table ready")
        
        print("✓ All database tables initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize database tables: {e}")
        print("The application cannot start without proper database setup.")
        import traceback
        traceback.print_exc()
        raise  # Re-raise to prevent app from starting with broken database
    
    yield  # Application runs here
    
    # Shutdown: Add cleanup logic here if needed in future
    pass


app = FastAPI(
    title="Data Mapper API",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative frontend port
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, PUT, DELETE, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers including Authorization
)

# Global task storage (in production, use Redis or database)
task_storage: Dict[str, AsyncTaskStatus] = {}
analysis_storage: Dict[str, AnalyzeFileResponse] = {}
# Cache for parsed file records to avoid double processing
# Key: file_hash, Value: dict with 'raw_records', 'mapped_records', 'config_hash', 'timestamp'
records_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 300  # 5 minutes


def detect_file_type(filename: str) -> str:
    """Detect file type from filename."""
    if filename.endswith('.csv'):
        return 'csv'
    elif filename.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif filename.endswith('.json'):
        return 'json'
    elif filename.endswith('.xml'):
        return 'xml'
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")


@app.post("/map-data", response_model=MapDataResponse)
async def map_data_endpoint(
    file: UploadFile = File(...),
    mapping_json: str = Form(...),
    db: Session = Depends(get_db)
):
    from .import_orchestrator import execute_data_import
    from .models import FileAlreadyImportedException, DuplicateDataException
    import hashlib
    
    try:
        # Parse mapping config
        if not mapping_json:
            raise HTTPException(status_code=400, detail="Mapping configuration required")
        mapping_data = json.loads(mapping_json)
        config = MappingConfig(**mapping_data)

        # Read file content
        file_content = await file.read()
        
        # Calculate file hash to check cache
        file_hash = hashlib.sha256(file_content).hexdigest()
        
        # Check if we have cached records from /detect-mapping
        cached_records = None
        use_mapped_cache = False
        current_time = time.time()
        
        # Generate config hash to check if mapping changed
        config_hash = hashlib.sha256(mapping_json.encode()).hexdigest()
        
        if file_hash in records_cache:
            cache_entry = records_cache[file_hash]
            timestamp = cache_entry.get('timestamp', 0)
            
            # Check if cache is still valid (within TTL)
            if current_time - timestamp <= CACHE_TTL_SECONDS:
                # Check if we have mapped records with matching config
                if cache_entry.get('config_hash') == config_hash and 'mapped_records' in cache_entry:
                    cached_records = cache_entry['mapped_records']
                    use_mapped_cache = True
                    print(f"✅ CACHE HIT: Using cached MAPPED records for file hash {file_hash[:8]}... ({len(cached_records)} records)")
                elif 'raw_records' in cache_entry:
                    cached_records = cache_entry['raw_records']
                    print(f"✅ CACHE HIT: Using cached RAW records for file hash {file_hash[:8]}... ({len(cached_records)} records)")
            else:
                # Cache expired, remove it
                del records_cache[file_hash]
                print(f"⏰ Cache expired for file hash {file_hash[:8]}...")
        else:
            print(f"❌ CACHE MISS: No cached records for file hash {file_hash[:8]}...")
        
        # Execute unified import with optional cached records
        # Pass pre_mapped=True only if we're using cached MAPPED records
        result = execute_data_import(
            file_content=file_content,
            file_name=file.filename,
            mapping_config=config,
            source_type="local_upload",
            pre_parsed_records=cached_records,
            pre_mapped=use_mapped_cache
        )
        
        # Update cache with mapped records if we didn't use mapped cache
        # This allows subsequent imports with same config to skip mapping
        if not use_mapped_cache and file_hash in records_cache:
            # Note: We would need to get mapped_records from execute_data_import
            # For now, we'll keep the cache entry but update timestamp
            records_cache[file_hash]['timestamp'] = current_time
            records_cache[file_hash]['config_hash'] = config_hash
            print(f"💾 Updated cache entry for file hash {file_hash[:8]}...")

        return MapDataResponse(
            success=True,
            message="Data mapped and inserted successfully",
            records_processed=result["records_processed"],
            duplicates_skipped=result.get("duplicates_skipped", 0),
            table_name=result["table_name"]
        )

    except FileAlreadyImportedException as e:
        raise HTTPException(status_code=409, detail=str(e))
    except DuplicateDataException as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/map-b2-data", response_model=MapDataResponse)
async def map_b2_data_endpoint(
    request: MapB2DataRequest,
    db: Session = Depends(get_db)
):
    from .import_orchestrator import execute_data_import
    from .models import FileAlreadyImportedException, DuplicateDataException
    
    try:
        # Download file from B2
        file_content = download_file_from_b2(request.file_name)
        
        # Execute unified import
        result = execute_data_import(
            file_content=file_content,
            file_name=request.file_name,
            mapping_config=request.mapping,
            source_type="b2_storage",
            source_path=request.file_name
        )

        return MapDataResponse(
            success=True,
            message="B2 data mapped and inserted successfully",
            records_processed=result["records_processed"],
            table_name=result["table_name"]
        )

    except FileAlreadyImportedException as e:
        raise HTTPException(status_code=409, detail=str(e))
    except DuplicateDataException as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/extract-b2-excel-csv", response_model=ExtractExcelCsvResponse)
async def extract_b2_excel_csv_endpoint(request: ExtractB2ExcelRequest):
    try:
        # Download file from B2
        file_content = download_file_from_b2(request.file_name)

        # Extract sheets to CSV
        sheets_csv = extract_excel_sheets_to_csv(file_content, request.rows)

        return ExtractExcelCsvResponse(
            success=True,
            sheets=sheets_csv
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/detect-mapping", response_model=DetectB2MappingResponse)
async def detect_mapping_endpoint(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Detect mapping configuration from an uploaded file.
    
    This endpoint analyzes the structure of an uploaded file and automatically
    detects column names, data types, and suggests a mapping configuration.
    It also caches the parsed records to avoid re-parsing in /map-data.
    
    Parameters:
    - file: The file to analyze (CSV, Excel, JSON, or XML)
    
    Returns:
    - Detected file type
    - Suggested mapping configuration
    - List of columns found
    - Number of rows sampled
    """
    try:
        # Read file content
        file_content = await file.read()
        
        # Calculate file hash for caching
        import hashlib
        file_hash = hashlib.sha256(file_content).hexdigest()
        
        # Detect mapping from file AND get parsed records
        file_type, detected_mapping, columns_found, rows_sampled, records = detect_mapping_from_file(
            file_content, file.filename, return_records=True
        )
        
        # Cache the parsed records for 5 minutes (to be used by /map-data)
        current_time = time.time()
        
        # Store in enhanced cache structure
        records_cache[file_hash] = {
            'raw_records': records,
            'timestamp': current_time,
            'file_name': file.filename
        }
        
        # Clean up old cache entries (older than TTL)
        expired_keys = [k for k, v in records_cache.items() 
                       if current_time - v.get('timestamp', 0) > CACHE_TTL_SECONDS]
        for key in expired_keys:
            del records_cache[key]
        
        print(f"DEBUG: Cached {len(records)} RAW records for file hash {file_hash[:8]}...")

        return DetectB2MappingResponse(
            success=True,
            file_type=file_type,
            detected_mapping=detected_mapping,
            columns_found=columns_found,
            rows_sampled=rows_sampled
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/detect-b2-mapping", response_model=DetectB2MappingResponse)
async def detect_b2_mapping_endpoint(request: DetectB2MappingRequest):
    try:
        # Download file from B2
        file_content = download_file_from_b2(request.file_name)

        # Detect mapping from file
        file_type, detected_mapping, columns_found, rows_sampled = detect_mapping_from_file(
            file_content, request.file_name
        )

        return DetectB2MappingResponse(
            success=True,
            file_type=file_type,
            detected_mapping=detected_mapping,
            columns_found=columns_found,
            rows_sampled=rows_sampled
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables", response_model=TablesListResponse)
async def list_tables(db: Session = Depends(get_db)):
    """List all dynamically created tables."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Query information_schema for user-created tables (exclude system tables)
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews',
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors')
                AND table_name NOT LIKE 'pg_%'
                ORDER BY table_name
            """))

            tables = []
            for row in result:
                table_name = row[0]

                # Get row count for each table
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
                row_count = count_result.scalar()

                tables.append(TableInfo(
                    table_name=table_name,
                    row_count=row_count
                ))

        return TablesListResponse(success=True, tables=tables)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}", response_model=TableDataResponse)
async def query_table(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """Query table data with pagination."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get total row count
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
            total_rows = count_result.scalar()

            # Get column names excluding metadata columns (those starting with _)
            columns_result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name NOT LIKE '\\_%'
                ORDER BY ordinal_position
            """), {"table_name": table_name})
            
            user_columns = [row[0] for row in columns_result]
            columns_sql = ', '.join([f'"{col}"' for col in user_columns])

            # Get paginated data (excluding metadata columns)
            data_result = conn.execute(text(f"""
                SELECT {columns_sql} FROM \"{table_name}\"
                ORDER BY _row_id
                LIMIT :limit OFFSET :offset
            """), {"limit": limit, "offset": offset})

            columns = data_result.keys()
            data = [dict(zip(columns, row)) for row in data_result]

        return TableDataResponse(
            success=True,
            table_name=table_name,
            data=data,
            total_rows=total_rows,
            limit=limit,
            offset=offset
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/schema", response_model=TableSchemaResponse)
async def get_table_schema(table_name: str, db: Session = Depends(get_db)):
    """Get table column information."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get column information
            columns_result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            columns = []
            for row in columns_result:
                columns.append(ColumnInfo(
                    name=row[0],
                    type=row[1],
                    nullable=row[2].upper() == 'YES'
                ))

        return TableSchemaResponse(
            success=True,
            table_name=table_name,
            columns=columns
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables/{table_name}/stats", response_model=TableStatsResponse)
async def get_table_stats(table_name: str, db: Session = Depends(get_db)):
    """Get basic table statistics."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get total rows
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
            total_rows = count_result.scalar()

            # Get column count and data types
            columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'  -- Exclude auto-generated id column
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            data_types = {row[0]: row[1] for row in columns_result}
            columns_count = len(data_types)

        return TableStatsResponse(
            success=True,
            table_name=table_name,
            total_rows=total_rows,
            columns_count=columns_count,
            data_types=data_types
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def process_b2_data_async(task_id: str, file_name: str, mapping: MappingConfig):
    """Background task for processing B2 data asynchronously."""
    from .import_orchestrator import execute_data_import
    
    try:
        # Update task status to processing
        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="processing",
            progress=10,
            message="Downloading file from B2..."
        )

        # Download file from B2
        file_content = download_file_from_b2(file_name)

        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="processing",
            progress=30,
            message="Processing and importing data..."
        )

        # Execute unified import
        result = execute_data_import(
            file_content=file_content,
            file_name=file_name,
            mapping_config=mapping,
            source_type="b2_storage",
            source_path=file_name
        )

        # Update task as completed
        response = MapDataResponse(
            success=True,
            message="B2 data mapped and inserted successfully",
            records_processed=result["records_processed"],
            table_name=result["table_name"]
        )

        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="completed",
            progress=100,
            message="Processing completed successfully",
            result=response
        )

    except FileAlreadyImportedException as e:
        # Update task as failed due to duplicate file
        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="failed",
            message=str(e)
        )
    except DuplicateDataException as e:
        # Update task as failed due to duplicate data
        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="failed",
            message=str(e)
        )
    except Exception as e:
        # Update task as failed
        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="failed",
            message=f"Processing failed: {str(e)}"
        )


@app.post("/map-b2-data-async", response_model=AsyncTaskStatus)
async def map_b2_data_async_endpoint(
    request: MapB2DataAsyncRequest,
    background_tasks: BackgroundTasks
):
    """Start async processing of B2 data."""
    task_id = str(uuid.uuid4())

    # Initialize task status
    task_storage[task_id] = AsyncTaskStatus(
        task_id=task_id,
        status="pending",
        message="Task queued for processing"
    )

    # Add background task
    background_tasks.add_task(
        process_b2_data_async,
        task_id=task_id,
        file_name=request.file_name,
        mapping=request.mapping
    )

    return task_storage[task_id]


@app.get("/tasks/{task_id}", response_model=AsyncTaskStatus)
async def get_task_status(task_id: str):
    """Get the status of an async task."""
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="Task not found")

    return task_storage[task_id]


@app.post("/query-database", response_model=QueryDatabaseResponse)
async def query_database_endpoint(request: QueryDatabaseRequest):
    """
    Execute natural language queries against the database using LangChain agent with conversation memory.
    
    The agent remembers previous queries within the same thread_id, allowing for:
    - Follow-up questions: "Now filter for California only"
    - References to past results: "What was the total from the last query?"
    - Context-aware queries: "Show products" → "Which of those have low stock?"
    
    Parameters:
    - prompt: Natural language query
    - max_rows: Maximum rows to return (1-10000)
    - thread_id: Optional conversation thread ID for memory continuity
    """
    try:
        # Pass thread_id to maintain conversation memory
        result = query_database_with_agent(request.prompt, thread_id=request.thread_id)

        return QueryDatabaseResponse(
            success=result["success"],
            response=result["response"],
            executed_sql=result.get("executed_sql"),
            data_csv=result.get("data_csv"),
            execution_time_seconds=result.get("execution_time_seconds"),
            rows_returned=result.get("rows_returned"),
            error=result.get("error")
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")


@app.post("/analyze-file", response_model=AnalyzeFileResponse)
async def analyze_file_endpoint(
    file: Optional[UploadFile] = File(None),
    file_id: Optional[str] = Form(None),
    sample_size: Optional[int] = Form(None),
    analysis_mode: AnalysisMode = Form(AnalysisMode.MANUAL),
    conflict_resolution: ConflictResolutionMode = Form(ConflictResolutionMode.ASK_USER),
    auto_execute_confidence_threshold: float = Form(0.9),
    max_iterations: int = Form(5),
    db: Session = Depends(get_db)
):
    """
    Analyze uploaded file and recommend import strategy using AI.
    
    This endpoint uses Claude Sonnet to intelligently analyze the file structure,
    compare it with existing database tables, and recommend the best import strategy.
    
    Parameters:
    - file: The file to analyze (CSV, Excel, JSON, or XML) - optional if file_id provided
    - file_id: UUID of previously uploaded file - optional if file provided
    - sample_size: Number of rows to sample (auto-calculated if not provided)
    - analysis_mode: MANUAL (user approval), AUTO_HIGH_CONFIDENCE, or AUTO_ALWAYS
    - conflict_resolution: ASK_USER, LLM_DECIDE, or PREFER_FLEXIBLE
    - auto_execute_confidence_threshold: Minimum confidence for auto-execution (0.0-1.0)
    - max_iterations: Maximum LLM iterations (1-10)
    """
    from .uploaded_files import get_uploaded_file_by_id, update_file_status
    from .b2_utils import download_file_from_b2
    
    try:
        # Validate input: must provide either file or file_id
        if not file and not file_id:
            raise HTTPException(status_code=400, detail="Must provide either 'file' or 'file_id'")
        
        if file and file_id:
            raise HTTPException(status_code=400, detail="Cannot provide both 'file' and 'file_id'")
        
        # Get file content and name
        if file_id:
            # Fetch from uploaded_files and download from B2
            file_record = get_uploaded_file_by_id(file_id)
            if not file_record:
                raise HTTPException(status_code=404, detail=f"File {file_id} not found")
            
            # Update status to 'mapping'
            update_file_status(file_id, "mapping")
            
            file_content = download_file_from_b2(file_record["b2_file_path"])
            file_name = file_record["file_name"]
        else:
            # Read uploaded file
            file_content = await file.read()
            file_name = file.filename
        
        # Detect and process file
        file_type = detect_file_type(file_name)
        
        # For CSV files, extract raw rows for LLM analysis WITHOUT parsing
        raw_csv_rows = None
        records = []
        
        if file_type == 'csv':
            from .processors.csv_processor import extract_raw_csv_rows
            raw_csv_rows = extract_raw_csv_rows(file_content, num_rows=20)
            
            # IMPORTANT: Do NOT parse the CSV file yet!
            # The LLM needs to analyze the raw structure first to determine if it has headers
            # We'll parse it later in auto_import.py with the correct has_header value
            # For now, just use auto-detection to get a sample for the LLM
            records = process_csv(file_content, has_header=None)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Smart sampling
        sample, total_rows = sample_file_data(records, sample_size)
        
        # Prepare metadata
        file_metadata = {
            "name": file_name,
            "total_rows": total_rows,
            "file_type": file_type
        }
        
        # Add raw CSV rows to metadata for LLM analysis
        if raw_csv_rows:
            file_metadata["raw_csv_rows"] = raw_csv_rows
        
        # Run AI analysis
        analysis_result = analyze_file_for_import(
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=analysis_mode,
            conflict_mode=conflict_resolution,
            user_id=None,  # Could be extracted from auth
            max_iterations=max_iterations
        )
        
        if not analysis_result["success"]:
            return AnalyzeFileResponse(
                success=False,
                error=analysis_result.get("error", "Analysis failed"),
                llm_response=analysis_result.get("response"),
                iterations_used=analysis_result.get("iterations_used", 0),
                max_iterations=max_iterations
            )
        
        # Check if LLM made a decision
        llm_decision = analysis_result.get("llm_decision")
        
        # Parse LLM response to extract structured data
        response = AnalyzeFileResponse(
            success=True,
            llm_response=analysis_result["response"],
            iterations_used=analysis_result["iterations_used"],
            max_iterations=max_iterations,
            can_auto_execute=False  # Will be set below based on analysis_mode
        )
        
        # Determine can_auto_execute based on analysis_mode
        if analysis_mode == AnalysisMode.AUTO_ALWAYS:
            response.can_auto_execute = True
        elif analysis_mode == AnalysisMode.AUTO_HIGH_CONFIDENCE:
            # Would need to parse confidence from LLM response
            response.can_auto_execute = False  # Conservative default
        else:  # MANUAL
            response.can_auto_execute = False
        
        # Store analysis result for later execution
        analysis_id = str(uuid.uuid4())
        analysis_storage[analysis_id] = response
        
        # AUTO-EXECUTION LOGIC (separate from determining can_auto_execute)
        if analysis_mode == AnalysisMode.AUTO_ALWAYS and llm_decision:
            # Execute the import automatically
            try:
                execution_result = execute_llm_import_decision(
                    file_content=file_content,
                    file_name=file_name,
                    all_records=records,  # Use all records, not just sample
                    llm_decision=llm_decision
                )
                
                if execution_result["success"]:
                    # Update file status to 'mapped' if file_id was provided
                    if file_id:
                        update_file_status(
                            file_id,
                            "mapped",
                            mapped_table_name=execution_result["table_name"],
                            mapped_rows=execution_result["records_processed"]
                        )
                    
                    # Update response with execution results
                    response.can_auto_execute = True
                    # Add execution info to response (note: this extends the schema)
                    response.llm_response += f"\n\n✅ AUTO-EXECUTION COMPLETED:\n"
                    response.llm_response += f"- Strategy: {execution_result['strategy_executed']}\n"
                    response.llm_response += f"- Table: {execution_result['table_name']}\n"
                    response.llm_response += f"- Records Processed: {execution_result['records_processed']}\n"
                else:
                    # Update file status to 'failed' if file_id was provided
                    error_msg = execution_result.get('error', 'Unknown error')
                    if file_id:
                        update_file_status(file_id, "failed", error_message=error_msg)
                    
                    response.llm_response += f"\n\n❌ AUTO-EXECUTION FAILED:\n"
                    response.llm_response += f"- Error: {error_msg}\n"
                    
            except Exception as e:
                # Update file status to 'failed' if file_id was provided
                error_msg = str(e)
                if file_id:
                    update_file_status(file_id, "failed", error_message=error_msg)
                
                response.llm_response += f"\n\n❌ AUTO-EXECUTION ERROR: {error_msg}\n"
        
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.post("/analyze-b2-file", response_model=AnalyzeFileResponse)
async def analyze_b2_file_endpoint(
    request: AnalyzeB2FileRequest,
    db: Session = Depends(get_db)
):
    """
    Analyze a file from B2 storage and recommend import strategy using AI.
    
    This endpoint downloads a file from Backblaze B2, then uses Claude Sonnet
    to analyze its structure and recommend the best import strategy.
    """
    try:
        # Download file from B2
        file_content = download_file_from_b2(request.file_name)
        
        # Detect and process file
        file_type = detect_file_type(request.file_name)
        
        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Smart sampling
        sample, total_rows = sample_file_data(records, request.sample_size)
        
        # Prepare metadata
        file_metadata = {
            "name": request.file_name,
            "total_rows": total_rows,
            "file_type": file_type
        }
        
        # Run AI analysis
        analysis_result = analyze_file_for_import(
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=request.analysis_mode,
            conflict_mode=request.conflict_resolution,
            user_id=None,
            max_iterations=request.max_iterations
        )
        
        if not analysis_result["success"]:
            return AnalyzeFileResponse(
                success=False,
                error=analysis_result.get("error", "Analysis failed"),
                llm_response=analysis_result.get("response"),
                iterations_used=analysis_result.get("iterations_used", 0),
                max_iterations=request.max_iterations
            )
        
        # Parse LLM response
        response = AnalyzeFileResponse(
            success=True,
            llm_response=analysis_result["response"],
            iterations_used=analysis_result["iterations_used"],
            max_iterations=request.max_iterations,
            can_auto_execute=False  # Will be set below based on analysis_mode
        )
        
        # Determine can_auto_execute based on analysis_mode
        if request.analysis_mode == AnalysisMode.AUTO_ALWAYS:
            response.can_auto_execute = True
        elif request.analysis_mode == AnalysisMode.AUTO_HIGH_CONFIDENCE:
            # Would need to parse confidence from LLM response
            response.can_auto_execute = False  # Conservative default
        else:  # MANUAL
            response.can_auto_execute = False
        
        # Store analysis result
        analysis_id = str(uuid.uuid4())
        analysis_storage[analysis_id] = response
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@app.post("/execute-recommended-import", response_model=MapDataResponse)
async def execute_recommended_import_endpoint(
    request: ExecuteRecommendedImportRequest,
    db: Session = Depends(get_db)
):
    """
    Execute a previously analyzed import recommendation.
    
    This endpoint takes an analysis ID from a previous /analyze-file or /analyze-b2-file
    call and executes the recommended import strategy. Users can optionally modify
    the suggested mapping before execution.
    """
    try:
        # Retrieve stored analysis
        if request.analysis_id not in analysis_storage:
            raise HTTPException(status_code=404, detail="Analysis not found. Please run /analyze-file first.")
        
        analysis = analysis_storage[request.analysis_id]
        
        if not analysis.success:
            raise HTTPException(status_code=400, detail="Cannot execute failed analysis")
        
        # Use user's confirmed mapping or the suggested one
        if request.confirmed_mapping:
            mapping = request.confirmed_mapping
        elif analysis.suggested_mapping:
            mapping = analysis.suggested_mapping
        else:
            raise HTTPException(
                status_code=400, 
                detail="No mapping available. Please provide confirmed_mapping in request."
            )
        
        # Check for conflicts
        if analysis.conflicts and not request.force_execute:
            raise HTTPException(
                status_code=409,
                detail=f"Analysis has {len(analysis.conflicts)} unresolved conflicts. "
                       "Set force_execute=true to proceed anyway."
            )
        
        # Execute import using existing pipeline
        # Note: This is a simplified version - in production, you'd need to:
        # 1. Retrieve the original file content
        # 2. Process it according to the mapping
        # 3. Insert into database
        
        raise HTTPException(
            status_code=501,
            detail="Import execution not yet implemented. This endpoint will execute "
                   "the recommended import strategy in the next phase."
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@app.get("/import-history")
async def list_import_history(
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    List import history with optional filters.
    
    Parameters:
    - table_name: Filter by destination table name
    - user_id: Filter by user ID
    - status: Filter by status ('success', 'failed', 'partial')
    - limit: Maximum number of records to return (default: 100)
    - offset: Number of records to skip for pagination (default: 0)
    """
    from .import_history import get_import_history
    from .schemas import ImportHistoryListResponse, ImportHistoryRecord
    
    try:
        records = get_import_history(
            table_name=table_name,
            user_id=user_id,
            status=status,
            limit=limit,
            offset=offset
        )
        
        # Convert to Pydantic models
        import_records = [ImportHistoryRecord(**record) for record in records]
        
        return ImportHistoryListResponse(
            success=True,
            imports=import_records,
            total_count=len(import_records),
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve import history: {str(e)}")


@app.get("/import-history/{import_id}")
async def get_import_detail(
    import_id: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific import.
    
    Parameters:
    - import_id: UUID of the import to retrieve
    """
    from .import_history import get_import_history
    from .schemas import ImportHistoryDetailResponse, ImportHistoryRecord
    
    try:
        records = get_import_history(import_id=import_id, limit=1)
        
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
        
        import_record = ImportHistoryRecord(**records[0])
        
        return ImportHistoryDetailResponse(
            success=True,
            import_record=import_record
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve import details: {str(e)}")


@app.get("/import-statistics")
async def get_import_statistics(
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    days: int = 30,
    db: Session = Depends(get_db)
):
    """
    Get aggregate statistics about imports.
    
    Parameters:
    - table_name: Filter by destination table name
    - user_id: Filter by user ID
    - days: Number of days to look back (default: 30)
    """
    from .import_history import get_import_statistics
    from .schemas import ImportStatisticsResponse
    
    try:
        stats = get_import_statistics(
            table_name=table_name,
            user_id=user_id,
            days=days
        )
        
        return ImportStatisticsResponse(
            success=True,
            total_imports=stats.get("total_imports", 0),
            successful_imports=stats.get("successful_imports", 0),
            failed_imports=stats.get("failed_imports", 0),
            total_rows_inserted=stats.get("total_rows_inserted", 0),
            total_duplicates_found=stats.get("total_duplicates_found", 0),
            avg_duration_seconds=stats.get("avg_duration_seconds", 0.0),
            tables_affected=stats.get("tables_affected", 0),
            unique_users=stats.get("unique_users", 0),
            period_days=days
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve import statistics: {str(e)}")


@app.get("/tables/{table_name}/lineage")
async def get_table_lineage(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get import lineage for a specific table - all imports that contributed data.
    
    Parameters:
    - table_name: Name of the table to get lineage for
    """
    from .import_history import get_table_import_lineage
    from .schemas import TableLineageResponse, ImportHistoryRecord
    
    try:
        records = get_table_import_lineage(table_name)
        
        # Convert to Pydantic models
        import_records = [ImportHistoryRecord(**record) for record in records]
        
        # Calculate total rows contributed
        total_rows = sum(r.rows_inserted or 0 for r in import_records)
        
        return TableLineageResponse(
            success=True,
            table_name=table_name,
            imports=import_records,
            total_imports=len(import_records),
            total_rows_contributed=total_rows
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve table lineage: {str(e)}")


@app.get("/")
async def root():
    return {"message": "Data Mapper API", "version": "1.0.0"}


# ============================================================================
# FILE UPLOAD ENDPOINTS
# ============================================================================

@app.post("/upload-to-b2")
async def upload_file_to_b2_endpoint(
    file: UploadFile = File(...),
    allow_duplicate: bool = Form(False),
    db: Session = Depends(get_db)
):
    """
    Upload a file to Backblaze B2 storage.
    
    Parameters:
    - file: The file to upload
    - allow_duplicate: If true, allow uploading file with same name (creates new ID)
    
    Returns:
    - File metadata including B2 file ID and upload status
    """
    from .b2_utils import upload_file_to_b2, check_file_exists_in_b2
    from .uploaded_files import insert_uploaded_file, get_uploaded_file_by_name
    from .schemas import UploadFileResponse, UploadedFileInfo, FileExistsResponse
    import traceback
    
    print(f"\n{'='*80}")
    print(f"[UPLOAD] Starting upload process for file: {file.filename}")
    print(f"[UPLOAD] Content type: {file.content_type}")
    print(f"[UPLOAD] Allow duplicate: {allow_duplicate}")
    print(f"{'='*80}\n")
    
    try:
        # Check if file already exists
        print(f"[UPLOAD] Checking if file exists in database...")
        existing_file = get_uploaded_file_by_name(file.filename)
        
        if existing_file:
            print(f"[UPLOAD] File found in database: {existing_file['id']}")
            if not allow_duplicate:
                print(f"[UPLOAD] Duplicate not allowed, returning conflict response")
                return FileExistsResponse(
                    success=False,
                    exists=True,
                    message=f"File '{file.filename}' already exists. Choose to overwrite, create duplicate, or skip.",
                    existing_file=UploadedFileInfo(**existing_file)
                )
            else:
                print(f"[UPLOAD] Duplicate allowed, proceeding with upload")
        else:
            print(f"[UPLOAD] File not found in database, proceeding with new upload")
        
        # Read file content
        print(f"[UPLOAD] Reading file content...")
        file_content = await file.read()
        file_size = len(file_content)
        print(f"[UPLOAD] File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
        
        # Upload to B2
        print(f"[UPLOAD] Calling upload_file_to_b2()...")
        print(f"[UPLOAD] Target folder: uploads")
        print(f"[UPLOAD] Target filename: {file.filename}")
        
        b2_result = upload_file_to_b2(
            file_content=file_content,
            file_name=file.filename,
            folder="uploads"
        )
        
        print(f"[UPLOAD] B2 upload successful!")
        print(f"[UPLOAD] B2 File ID: {b2_result['file_id']}")
        print(f"[UPLOAD] B2 File Path: {b2_result['file_path']}")
        print(f"[UPLOAD] B2 File Size: {b2_result['size']} bytes")
        
        # Store in database
        print(f"[UPLOAD] Storing file metadata in database...")
        uploaded_file = insert_uploaded_file(
            file_name=file.filename,
            b2_file_id=b2_result["file_id"],
            b2_file_path=b2_result["file_path"],
            file_size=file_size,
            content_type=file.content_type,
            user_id=None  # TODO: Get from auth context
        )
        
        print(f"[UPLOAD] Database record created: {uploaded_file['id']}")
        print(f"[UPLOAD] Upload process completed successfully!")
        print(f"{'='*80}\n")
        
        return UploadFileResponse(
            success=True,
            message="File uploaded successfully",
            files=[UploadedFileInfo(**uploaded_file)]
        )
        
    except Exception as e:
        print(f"\n{'!'*80}")
        print(f"[UPLOAD ERROR] Upload failed for file: {file.filename}")
        print(f"[UPLOAD ERROR] Error type: {type(e).__name__}")
        print(f"[UPLOAD ERROR] Error message: {str(e)}")
        print(f"[UPLOAD ERROR] Traceback:")
        print(traceback.format_exc())
        print(f"{'!'*80}\n")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.post("/upload-to-b2/overwrite")
async def overwrite_file_in_b2_endpoint(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    Overwrite an existing file in B2 storage.
    
    Parameters:
    - file: The file to upload (will replace existing file with same name)
    
    Returns:
    - Updated file metadata
    """
    from .b2_utils import upload_file_to_b2, delete_file_from_b2
    from .uploaded_files import get_uploaded_file_by_name, delete_uploaded_file, insert_uploaded_file
    from .schemas import UploadFileResponse, UploadedFileInfo
    
    try:
        # Check if file exists
        existing_file = get_uploaded_file_by_name(file.filename)
        
        if existing_file:
            # Delete old file from B2
            delete_file_from_b2(existing_file["b2_file_path"])
            # Delete old database record
            delete_uploaded_file(existing_file["id"])
        
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)
        
        # Upload new version to B2
        b2_result = upload_file_to_b2(
            file_content=file_content,
            file_name=file.filename,
            folder="uploads"
        )
        
        # Store in database
        uploaded_file = insert_uploaded_file(
            file_name=file.filename,
            b2_file_id=b2_result["file_id"],
            b2_file_path=b2_result["file_path"],
            file_size=file_size,
            content_type=file.content_type,
            user_id=None  # TODO: Get from auth context
        )
        
        return UploadFileResponse(
            success=True,
            message="File overwritten successfully",
            files=[UploadedFileInfo(**uploaded_file)]
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Overwrite failed: {str(e)}")


@app.get("/uploaded-files")
async def list_uploaded_files_endpoint(
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    List uploaded files with optional status filter.
    
    Parameters:
    - status: Filter by status ('uploaded', 'mapping', 'mapped', 'failed')
    - limit: Maximum number of files to return (default: 100)
    - offset: Number of files to skip for pagination (default: 0)
    
    Returns:
    - List of uploaded files with metadata
    """
    from .uploaded_files import get_uploaded_files, get_uploaded_files_count
    from .schemas import UploadedFilesListResponse, UploadedFileInfo
    
    try:
        files = get_uploaded_files(
            status=status,
            user_id=None,  # TODO: Filter by current user
            limit=limit,
            offset=offset
        )
        
        total_count = get_uploaded_files_count(status=status)
        
        return UploadedFilesListResponse(
            success=True,
            files=[UploadedFileInfo(**f) for f in files],
            total_count=total_count,
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")


@app.get("/uploaded-files/{file_id}")
async def get_uploaded_file_endpoint(
    file_id: str,
    db: Session = Depends(get_db)
):
    """
    Get details of a specific uploaded file.
    
    Parameters:
    - file_id: UUID of the uploaded file
    
    Returns:
    - File metadata and status
    """
    from .uploaded_files import get_uploaded_file_by_id
    from .schemas import UploadedFileDetailResponse, UploadedFileInfo
    
    try:
        file = get_uploaded_file_by_id(file_id)
        
        if not file:
            raise HTTPException(status_code=404, detail=f"File {file_id} not found")
        
        return UploadedFileDetailResponse(
            success=True,
            file=UploadedFileInfo(**file)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get file: {str(e)}")


@app.delete("/uploaded-files/{file_id}")
async def delete_uploaded_file_endpoint(
    file_id: str,
    db: Session = Depends(get_db)
):
    """
    Delete an uploaded file from B2 and database.
    
    Parameters:
    - file_id: UUID of the uploaded file to delete
    
    Returns:
    - Success message
    """
    from .b2_utils import delete_file_from_b2
    from .uploaded_files import get_uploaded_file_by_id, delete_uploaded_file
    from .schemas import DeleteFileResponse
    
    try:
        # Get file info
        file = get_uploaded_file_by_id(file_id)
        
        if not file:
            raise HTTPException(status_code=404, detail=f"File {file_id} not found")
        
        # Delete from B2
        b2_deleted = delete_file_from_b2(file["b2_file_path"])
        
        if not b2_deleted:
            raise HTTPException(status_code=500, detail="Failed to delete file from B2")
        
        # Delete from database
        db_deleted = delete_uploaded_file(file_id)
        
        if not db_deleted:
            raise HTTPException(status_code=500, detail="Failed to delete file from database")
        
        return DeleteFileResponse(
            success=True,
            message=f"File '{file['file_name']}' deleted successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@app.patch("/uploaded-files/{file_id}/status")
async def update_file_status_endpoint(
    file_id: str,
    status: str,
    mapped_table_name: Optional[str] = None,
    mapped_rows: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Update the status of an uploaded file.
    
    Parameters:
    - file_id: UUID of the uploaded file
    - status: New status ('uploaded', 'mapping', 'mapped', 'failed')
    - mapped_table_name: Table name if status is 'mapped'
    - mapped_rows: Number of rows if status is 'mapped'
    
    Returns:
    - Updated file metadata
    """
    from .uploaded_files import update_file_status, get_uploaded_file_by_id
    from .schemas import UploadedFileDetailResponse, UploadedFileInfo
    
    try:
        # Update status
        updated = update_file_status(
            file_id=file_id,
            status=status,
            mapped_table_name=mapped_table_name,
            mapped_rows=mapped_rows
        )
        
        if not updated:
            raise HTTPException(status_code=404, detail=f"File {file_id} not found")
        
        # Get updated file
        file = get_uploaded_file_by_id(file_id)
        
        return UploadedFileDetailResponse(
            success=True,
            file=UploadedFileInfo(**file)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status update failed: {str(e)}")


# ============================================================================
# OPTIMIZED UPLOAD ENDPOINTS (Direct Browser-to-B2)
# ============================================================================

@app.post("/check-duplicate", response_model=CheckDuplicateResponse)
async def check_duplicate_endpoint(
    request: CheckDuplicateRequest,
    db: Session = Depends(get_db)
):
    """
    Check if a file is a duplicate before uploading.
    
    This lightweight endpoint checks if a file with the same hash already exists
    in the database. If not, it generates upload authorization for direct
    browser-to-B2 upload.
    
    Parameters:
    - file_name: Name of the file
    - file_hash: SHA-256 hash of the file content
    - file_size: Size of the file in bytes
    
    Returns:
    - is_duplicate: Whether the file already exists
    - can_upload: Whether the file can be uploaded
    - upload_authorization: B2 credentials for direct upload (if can_upload=true)
    """
    from .uploaded_files import get_uploaded_file_by_hash
    from .b2_utils import generate_upload_authorization
    
    try:
        # Check if file with same hash exists
        existing_file = get_uploaded_file_by_hash(request.file_hash)
        
        if existing_file:
            # File is a duplicate
            from .schemas import UploadedFileInfo
            return CheckDuplicateResponse(
                success=True,
                is_duplicate=True,
                message=f"File already exists: {existing_file['file_name']}",
                existing_file=UploadedFileInfo(**existing_file),
                can_upload=False
            )
        
        # File is not a duplicate, generate upload authorization
        upload_auth = generate_upload_authorization(
            file_name=request.file_name,
            folder="uploads"
        )
        
        return CheckDuplicateResponse(
            success=True,
            is_duplicate=False,
            message="File can be uploaded",
            can_upload=True,
            upload_authorization=upload_auth
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Duplicate check failed: {str(e)}")


@app.post("/complete-upload", response_model=CompleteUploadResponse)
async def complete_upload_endpoint(
    request: CompleteUploadRequest,
    db: Session = Depends(get_db)
):
    """
    Complete the upload process after direct browser-to-B2 upload.
    
    This endpoint is called by the frontend after successfully uploading
    a file directly to B2. It saves the file metadata in the database.
    
    Parameters:
    - file_name: Name of the uploaded file
    - file_hash: SHA-256 hash of the file content
    - file_size: Size of the file in bytes
    - content_type: MIME type of the file
    - b2_file_id: B2 file ID returned from upload
    - b2_file_path: Full path in B2 bucket
    
    Returns:
    - File metadata record
    """
    from .uploaded_files import insert_uploaded_file
    from .schemas import UploadedFileInfo
    
    try:
        # Save file metadata to database
        uploaded_file = insert_uploaded_file(
            file_name=request.file_name,
            b2_file_id=request.b2_file_id,
            b2_file_path=request.b2_file_path,
            file_size=request.file_size,
            content_type=request.content_type,
            user_id=None,  # TODO: Get from auth context
            file_hash=request.file_hash
        )
        
        return CompleteUploadResponse(
            success=True,
            message="Upload completed successfully",
            file=UploadedFileInfo(**uploaded_file)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload completion failed: {str(e)}")


# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================

@app.post("/auth/register", response_model=AuthResponse)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    """
    Register a new user.
    
    Parameters:
    - email: User's email address
    - password: User's password (will be hashed)
    - full_name: Optional full name
    """
    try:
        # Create user
        user = create_user(
            db=db,
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name
        )
        
        # Generate JWT token
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=timedelta(minutes=60 * 24)  # 24 hours
        )
        
        return AuthResponse(
            success=True,
            token=Token(access_token=access_token),
            user=UserResponse.from_orm(user)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@app.post("/auth/login", response_model=AuthResponse)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """
    Login with email and password.
    
    Parameters:
    - email: User's email address
    - password: User's password
    
    Returns:
    - JWT access token
    - User information
    """
    try:
        # Authenticate user
        user = authenticate_user(db, credentials.email, credentials.password)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Incorrect email or password"
            )
        
        # Generate JWT token
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=timedelta(minutes=60 * 24)  # 24 hours
        )
        
        return AuthResponse(
            success=True,
            token=Token(access_token=access_token),
            user=UserResponse.from_orm(user)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")


@app.get("/auth/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """
    Get current authenticated user information.
    
    Requires: Bearer token in Authorization header
    """
    return UserResponse.from_orm(current_user)


# ============================================================================
# API KEY MANAGEMENT ENDPOINTS (Admin Only)
# ============================================================================

@app.post("/admin/api-keys")
async def create_api_key_endpoint(
    request: "CreateApiKeyRequest",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Create a new API key (Admin only).
    
    This endpoint generates a new API key for external application access.
    The plain API key is only shown once and cannot be retrieved later.
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - app_name: Name of the application
    - description: Optional description
    - expires_in_days: Optional expiration in days
    - rate_limit_per_minute: Rate limit (default 60)
    - allowed_endpoints: Optional list of allowed endpoint patterns
    
    Returns:
    - The generated API key (only shown once)
    - API key metadata
    """
    from .api_key_auth import create_api_key
    from .api_key_schemas import CreateApiKeyRequest, CreateApiKeyResponse
    
    try:
        # Create API key
        api_key_record, plain_key = create_api_key(
            db=db,
            app_name=request.app_name,
            description=request.description,
            created_by=current_user.id,
            expires_in_days=request.expires_in_days,
            rate_limit_per_minute=request.rate_limit_per_minute,
            allowed_endpoints=request.allowed_endpoints
        )
        
        return CreateApiKeyResponse(
            success=True,
            message="API key created successfully. Save this key securely - it won't be shown again.",
            api_key=plain_key,
            key_id=api_key_record.id,
            app_name=api_key_record.app_name,
            expires_at=api_key_record.expires_at
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create API key: {str(e)}")


@app.get("/admin/api-keys")
async def list_api_keys_endpoint(
    is_active: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    List all API keys (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - is_active: Filter by active status
    - limit: Maximum number of keys to return
    - offset: Number of keys to skip
    
    Returns:
    - List of API keys (without the actual key values)
    """
    from .api_key_auth import list_api_keys
    from .api_key_schemas import ListApiKeysResponse, ApiKeyInfo
    
    try:
        api_keys = list_api_keys(
            db=db,
            created_by=None,  # Show all keys for admin
            is_active=is_active,
            limit=limit,
            offset=offset
        )
        
        # Convert to response format with key preview
        api_key_infos = []
        for key in api_keys:
            # Show last 4 characters of the hash as preview
            key_preview = f"...{key.key_hash[-4:]}"
            
            api_key_infos.append(ApiKeyInfo(
                id=key.id,
                app_name=key.app_name,
                description=key.description,
                created_at=key.created_at,
                last_used_at=key.last_used_at,
                expires_at=key.expires_at,
                is_active=key.is_active,
                rate_limit_per_minute=key.rate_limit_per_minute,
                allowed_endpoints=key.allowed_endpoints,
                key_preview=key_preview
            ))
        
        return ListApiKeysResponse(
            success=True,
            api_keys=api_key_infos,
            total_count=len(api_key_infos),
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list API keys: {str(e)}")


@app.delete("/admin/api-keys/{key_id}")
async def delete_api_key_endpoint(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Permanently delete an API key (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - key_id: UUID of the API key to delete
    
    Returns:
    - Success message
    """
    from .api_key_auth import delete_api_key
    from .api_key_schemas import RevokeApiKeyResponse
    
    try:
        success = delete_api_key(db, key_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
        
        return RevokeApiKeyResponse(
            success=True,
            message="API key deleted successfully",
            key_id=key_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete API key: {str(e)}")


@app.patch("/admin/api-keys/{key_id}")
async def update_api_key_endpoint(
    key_id: str,
    request: "UpdateApiKeyRequest",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Update an API key's settings (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - key_id: UUID of the API key to update
    - description: Optional new description
    - rate_limit_per_minute: Optional new rate limit
    - allowed_endpoints: Optional new allowed endpoints list
    - is_active: Optional active status
    
    Returns:
    - Updated API key metadata
    """
    from .api_key_auth import ApiKey
    from .api_key_schemas import UpdateApiKeyRequest, UpdateApiKeyResponse, ApiKeyInfo
    
    try:
        # Get API key
        api_key = db.query(ApiKey).filter(ApiKey.id == key_id).first()
        
        if not api_key:
            raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
        
        # Update fields
        if request.description is not None:
            api_key.description = request.description
        if request.rate_limit_per_minute is not None:
            api_key.rate_limit_per_minute = request.rate_limit_per_minute
        if request.allowed_endpoints is not None:
            api_key.allowed_endpoints = request.allowed_endpoints
        if request.is_active is not None:
            api_key.is_active = request.is_active
        
        db.commit()
        db.refresh(api_key)
        
        # Convert to response format
        key_preview = f"...{api_key.key_hash[-4:]}"
        
        api_key_info = ApiKeyInfo(
            id=api_key.id,
            app_name=api_key.app_name,
            description=api_key.description,
            created_at=api_key.created_at,
            last_used_at=api_key.last_used_at,
            expires_at=api_key.expires_at,
            is_active=api_key.is_active,
            rate_limit_per_minute=api_key.rate_limit_per_minute,
            allowed_endpoints=api_key.allowed_endpoints,
            key_preview=key_preview
        )
        
        return UpdateApiKeyResponse(
            success=True,
            message="API key updated successfully",
            api_key=api_key_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update API key: {str(e)}")


# ============================================================================
# PUBLIC API ENDPOINTS (API Key Authentication)
# ============================================================================

@app.post("/api/v1/query")
async def public_query_database_endpoint(
    request: QueryDatabaseRequest,
    api_key: "ApiKey" = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    Execute natural language queries against the database (Public API).
    
    This endpoint is designed for external applications to query the database
    using natural language. It requires API key authentication.
    
    Authentication: X-API-Key header
    
    Parameters:
    - prompt: Natural language query
    - thread_id: Optional conversation thread ID for memory continuity
    
    Returns:
    - Query results in CSV format
    - Executed SQL query
    - Execution metadata
    """
    from .api_key_auth import ApiKey, get_api_key_from_header
    
    try:
        # Execute query using the same agent as internal endpoint
        result = query_database_with_agent(request.prompt, thread_id=request.thread_id)

        return QueryDatabaseResponse(
            success=result["success"],
            response=result["response"],
            executed_sql=result.get("executed_sql"),
            data_csv=result.get("data_csv"),
            execution_time_seconds=result.get("execution_time_seconds"),
            rows_returned=result.get("rows_returned"),
            error=result.get("error")
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")


@app.get("/api/v1/tables")
async def public_list_tables_endpoint(
    api_key: "ApiKey" = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    List all available tables (Public API).
    
    Authentication: X-API-Key header
    
    Returns:
    - List of table names with row counts
    """
    from .api_key_auth import ApiKey, get_api_key_from_header
    
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Query information_schema for user-created tables (exclude system tables)
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews',
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors', 'api_keys')
                AND table_name NOT LIKE 'pg_%'
                ORDER BY table_name
            """))

            tables = []
            for row in result:
                table_name = row[0]

                # Get row count for each table
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
                row_count = count_result.scalar()

                tables.append(TableInfo(
                    table_name=table_name,
                    row_count=row_count
                ))

        return TablesListResponse(success=True, tables=tables)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tables/{table_name}/schema")
async def public_get_table_schema_endpoint(
    table_name: str,
    api_key: "ApiKey" = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    Get table schema information (Public API).
    
    Authentication: X-API-Key header
    
    Parameters:
    - table_name: Name of the table
    
    Returns:
    - Table column information
    """
    from .api_key_auth import ApiKey, get_api_key_from_header
    
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get column information
            columns_result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            columns = []
            for row in columns_result:
                columns.append(ColumnInfo(
                    name=row[0],
                    type=row[1],
                    nullable=row[2].upper() == 'YES'
                ))

        return TableSchemaResponse(
            success=True,
            table_name=table_name,
            columns=columns
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INTERACTIVE FILE ANALYSIS ENDPOINTS
# ============================================================================

@app.post("/analyze-file-interactive")
async def analyze_file_interactive_endpoint(
    request: "AnalyzeFileInteractiveRequest",
    db: Session = Depends(get_db)
):
    """
    Interactive file analysis with conversation support.
    
    This endpoint enables a conversational workflow where the LLM can ask
    questions and wait for user responses before making final decisions.
    
    First call (no user_message):
    - Analyzes the file
    - Either asks a question OR makes a decision
    
    Subsequent calls (with user_message):
    - Continues conversation with user's answer
    - May ask more questions or make final decision
    
    Parameters:
    - file_id: UUID of uploaded file
    - user_message: User's response to previous question (optional on first call)
    - thread_id: Conversation thread ID (auto-generated if not provided)
    - max_iterations: Maximum LLM iterations
    """
    from .uploaded_files import get_uploaded_file_by_id
    from .b2_utils import download_file_from_b2
    from .schemas import AnalyzeFileInteractiveResponse
    
    try:
        # Get uploaded file
        file_record = get_uploaded_file_by_id(request.file_id)
        if not file_record:
            raise HTTPException(status_code=404, detail=f"File {request.file_id} not found")
        
        # Download file from B2
        file_content = download_file_from_b2(file_record["b2_file_path"])
        
        # Detect and process file
        file_type = detect_file_type(file_record["file_name"])
        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Smart sampling
        sample, total_rows = sample_file_data(records, None)
        
        # Prepare metadata
        file_metadata = {
            "name": file_record["file_name"],
            "total_rows": total_rows,
            "file_type": file_type
        }
        
        # Generate or use provided thread_id
        thread_id = request.thread_id or str(uuid.uuid4())
        
        # Build prompt based on whether this is first call or continuation
        if request.user_message:
            # Continuation - user is responding to a question
            prompt = request.user_message
        else:
            # First call - start analysis
            prompt = f"""Analyze this file for database import:

File: {file_metadata.get('name', 'unknown')}
Total Rows: {file_metadata.get('total_rows', 'unknown')}
Sample Size: {len(sample)}

You are in INTERACTIVE mode. You can ask the user questions to clarify the best import strategy.

Please analyze the file structure, compare it with existing tables, and either:
1. Ask a clarifying question if you need more information
2. Make a final import recommendation if you have enough information

If you ask a question, use the ask_followup_question tool with clear options for the user to choose from."""
        
        # Run AI analysis with conversation memory
        analysis_result = analyze_file_for_import(
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=AnalysisMode.MANUAL,  # Always manual for interactive
            conflict_mode=ConflictResolutionMode.ASK_USER,
            user_id=None,
            max_iterations=request.max_iterations,
            thread_id=thread_id
        )
        
        if not analysis_result["success"]:
            return AnalyzeFileInteractiveResponse(
                success=False,
                thread_id=thread_id,
                llm_message=analysis_result.get("response", "Analysis failed"),
                needs_user_input=False,
                can_execute=False,
                iterations_used=analysis_result.get("iterations_used", 0),
                max_iterations=request.max_iterations,
                error=analysis_result.get("error")
            )
        
        # Check if LLM made a decision
        llm_decision = analysis_result.get("llm_decision")
        
        # Parse response to determine if LLM is asking a question or making decision
        llm_response = analysis_result["response"]
        
        # Simple heuristic: if response ends with "?" it's likely a question
        # In production, you'd parse the LLM's tool calls to detect ask_followup_question
        is_question = "?" in llm_response[-100:] if len(llm_response) > 0 else False
        
        return AnalyzeFileInteractiveResponse(
            success=True,
            thread_id=thread_id,
            llm_message=llm_response,
            needs_user_input=is_question and not llm_decision,
            question=llm_response if is_question and not llm_decision else None,
            can_execute=llm_decision is not None,
            llm_decision=llm_decision,
            iterations_used=analysis_result["iterations_used"],
            max_iterations=request.max_iterations
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Interactive analysis failed: {str(e)}")


@app.post("/execute-interactive-import")
async def execute_interactive_import_endpoint(
    request: "ExecuteInteractiveImportRequest",
    db: Session = Depends(get_db)
):
    """
    Execute import from an interactive analysis session.
    
    This endpoint executes the import decision made during an interactive
    conversation with the LLM.
    
    Parameters:
    - file_id: UUID of uploaded file
    - thread_id: Conversation thread ID from interactive session
    """
    from .uploaded_files import get_uploaded_file_by_id, update_file_status
    from .b2_utils import download_file_from_b2
    
    try:
        # Get uploaded file
        file_record = get_uploaded_file_by_id(request.file_id)
        if not file_record:
            raise HTTPException(status_code=404, detail=f"File {request.file_id} not found")
        
        # Download file from B2
        file_content = download_file_from_b2(file_record["b2_file_path"])
        
        # Process file to get all records
        file_type = detect_file_type(file_record["file_name"])
        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Get the LLM decision from the conversation
        # Note: In a real implementation, you'd retrieve this from the conversation state
        # For now, we'll need to re-run the analysis to get the decision
        sample, total_rows = sample_file_data(records, None)
        file_metadata = {
            "name": file_record["file_name"],
            "total_rows": total_rows,
            "file_type": file_type
        }
        
        # Re-run analysis with the thread_id to get the final decision
        analysis_result = analyze_file_for_import(
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=AnalysisMode.MANUAL,
            conflict_mode=ConflictResolutionMode.LLM_DECIDE,
            user_id=None,
            max_iterations=5,
            thread_id=request.thread_id
        )
        
        llm_decision = analysis_result.get("llm_decision")
        if not llm_decision:
            raise HTTPException(
                status_code=400,
                detail="No import decision found in conversation. Please complete the interactive analysis first."
            )
        
        # Update file status to 'mapping'
        update_file_status(request.file_id, "mapping")
        
        # Execute the import
        execution_result = execute_llm_import_decision(
            file_content=file_content,
            file_name=file_record["file_name"],
            all_records=records,
            llm_decision=llm_decision
        )
        
        if execution_result["success"]:
            # Update file status to 'mapped'
            update_file_status(
                request.file_id,
                "mapped",
                mapped_table_name=execution_result["table_name"],
                mapped_rows=execution_result["records_processed"]
            )
            
            return MapDataResponse(
                success=True,
                message="Import executed successfully",
                records_processed=execution_result["records_processed"],
                table_name=execution_result["table_name"]
            )
        else:
            # Update file status to 'failed'
            error_msg = execution_result.get('error', 'Unknown error')
            update_file_status(request.file_id, "failed", error_message=error_msg)
            
            raise HTTPException(
                status_code=500,
                detail=f"Import execution failed: {error_msg}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import execution failed: {str(e)}")
