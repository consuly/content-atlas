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
    AnalyzeFileRequest, AnalyzeB2FileRequest, AnalyzeFileResponse,
    ExecuteRecommendedImportRequest, AnalysisMode, ConflictResolutionMode,
    CheckDuplicateRequest, CheckDuplicateResponse, CompleteUploadRequest, CompleteUploadResponse
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
from datetime import timedelta
import time


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown events."""
    # Startup: Initialize database tables
    try:
        from .uploaded_files import create_uploaded_files_table
        create_table_metadata_table()
        create_import_history_table()
        create_uploaded_files_table()
        init_auth_tables()
        print("✓ Database tables initialized successfully")
    except Exception as e:
        print(f"Warning: Could not initialize tables: {e}")
    
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

            # Get paginated data
            data_result = conn.execute(text(f"""
                SELECT * FROM \"{table_name}\"
                ORDER BY id
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
    file: UploadFile = File(...),
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
    - file: The file to analyze (CSV, Excel, JSON, or XML)
    - sample_size: Number of rows to sample (auto-calculated if not provided)
    - analysis_mode: MANUAL (user approval), AUTO_HIGH_CONFIDENCE, or AUTO_ALWAYS
    - conflict_resolution: ASK_USER, LLM_DECIDE, or PREFER_FLEXIBLE
    - auto_execute_confidence_threshold: Minimum confidence for auto-execution (0.0-1.0)
    - max_iterations: Maximum LLM iterations (1-10)
    """
    try:
        # Read file content
        file_content = await file.read()
        
        # Detect and process file
        file_type = detect_file_type(file.filename)
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
        sample, total_rows = sample_file_data(records, sample_size)
        
        # Prepare metadata
        file_metadata = {
            "name": file.filename,
            "total_rows": total_rows,
            "file_type": file_type
        }
        
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
                    file_name=file.filename,
                    all_records=records,  # Use all records, not just sample
                    llm_decision=llm_decision
                )
                
                if execution_result["success"]:
                    # Update response with execution results
                    response.can_auto_execute = True
                    # Add execution info to response (note: this extends the schema)
                    response.llm_response += f"\n\n✅ AUTO-EXECUTION COMPLETED:\n"
                    response.llm_response += f"- Strategy: {execution_result['strategy_executed']}\n"
                    response.llm_response += f"- Table: {execution_result['table_name']}\n"
                    response.llm_response += f"- Records Processed: {execution_result['records_processed']}\n"
                else:
                    response.llm_response += f"\n\n❌ AUTO-EXECUTION FAILED:\n"
                    response.llm_response += f"- Error: {execution_result.get('error', 'Unknown error')}\n"
                    
            except Exception as e:
                response.llm_response += f"\n\n❌ AUTO-EXECUTION ERROR: {str(e)}\n"
        
        
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
