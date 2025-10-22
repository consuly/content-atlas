from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
import json
from .database import get_db, get_engine
from .schemas import MapDataRequest, MapDataResponse, MappingConfig, MapB2DataRequest, ExtractB2ExcelRequest, ExtractExcelCsvResponse, DetectB2MappingRequest, DetectB2MappingResponse
from .processors.csv_processor import process_csv, process_excel, extract_excel_sheets_to_csv
from .processors.json_processor import process_json
from .processors.xml_processor import process_xml
from .mapper import map_data, detect_mapping_from_file
from .models import create_table_if_not_exists, insert_records
from .config import settings
from .b2_utils import download_file_from_b2

app = FastAPI(title="Data Mapper API", version="1.0.0")


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


@app.get("/")
async def root():
    return {"message": "Data Mapper API", "version": "1.0.0"}
