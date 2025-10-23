from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
import json
import uuid
from typing import Dict
from .database import get_db, get_engine
from .schemas import MapDataRequest, MapDataResponse, MappingConfig, MapB2DataRequest, ExtractB2ExcelRequest, ExtractExcelCsvResponse, DetectB2MappingRequest, DetectB2MappingResponse, TablesListResponse, TableInfo, TableSchemaResponse, ColumnInfo, TableDataResponse, TableStatsResponse, MapB2DataAsyncRequest, AsyncTaskStatus
from .processors.csv_processor import process_csv, process_excel, process_large_excel, extract_excel_sheets_to_csv
from .processors.json_processor import process_json
from .processors.xml_processor import process_xml
from .mapper import map_data, detect_mapping_from_file
from .models import create_table_if_not_exists, insert_records
from .config import settings
from .b2_utils import download_file_from_b2

app = FastAPI(title="Data Mapper API", version="1.0.0")

# Global task storage (in production, use Redis or database)
task_storage: Dict[str, AsyncTaskStatus] = {}


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
    try:
        # Parse mapping config
        if not mapping_json:
            raise HTTPException(status_code=400, detail="Mapping configuration required")
        mapping_data = json.loads(mapping_json)
        config = MappingConfig(**mapping_data)

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

        # Map data
        mapped_records = map_data(records, config)

        # Create table if needed
        create_table_if_not_exists(get_engine(), config)

        # Insert records
        records_processed = insert_records(get_engine(), config.table_name, mapped_records)

        return MapDataResponse(
            success=True,
            message="Data mapped and inserted successfully",
            records_processed=records_processed,
            table_name=config.table_name
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/map-b2-data", response_model=MapDataResponse)
async def map_b2_data_endpoint(
    request: MapB2DataRequest,
    db: Session = Depends(get_db)
):
    try:
        # Download file from B2
        file_content = download_file_from_b2(request.file_name)

        # Detect and process file
        file_type = detect_file_type(request.file_name)

        # Use chunked processing for large Excel files (>50MB)
        if file_type == 'excel' and len(file_content) > 50 * 1024 * 1024:  # 50MB
            records = process_large_excel(file_content)
        elif file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        # Map data
        mapped_records = map_data(records, request.mapping)

        # Create table if needed
        create_table_if_not_exists(get_engine(), request.mapping)

        # Insert records
        records_processed = insert_records(get_engine(), request.mapping.table_name, mapped_records)

        return MapDataResponse(
            success=True,
            message="B2 data mapped and inserted successfully",
            records_processed=records_processed,
            table_name=request.mapping.table_name
        )

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
                AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews')
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
            message="Processing file..."
        )

        # Detect and process file
        file_type = detect_file_type(file_name)

        # Use chunked processing for large Excel files (>50MB)
        if file_type == 'excel' and len(file_content) > 50 * 1024 * 1024:  # 50MB
            records = process_large_excel(file_content)
        elif file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise Exception("Unsupported file type")

        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="processing",
            progress=60,
            message="Mapping data..."
        )

        # Map data
        mapped_records = map_data(records, mapping)

        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="processing",
            progress=80,
            message="Creating table and inserting data..."
        )

        # Create table if needed
        create_table_if_not_exists(get_engine(), mapping)

        # Insert records
        records_processed = insert_records(get_engine(), mapping.table_name, mapped_records)

        # Update task as completed
        result = MapDataResponse(
            success=True,
            message="B2 data mapped and inserted successfully",
            records_processed=records_processed,
            table_name=mapping.table_name
        )

        task_storage[task_id] = AsyncTaskStatus(
            task_id=task_id,
            status="completed",
            progress=100,
            message="Processing completed successfully",
            result=result
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


@app.get("/")
async def root():
    return {"message": "Data Mapper API", "version": "1.0.0"}
