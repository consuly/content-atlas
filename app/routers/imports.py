"""
Data import endpoints for mapping and inserting data from various sources.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
import json
import hashlib
import time

from ..database import get_db
from ..schemas import MapDataRequest, MapDataResponse, MappingConfig, MapB2DataRequest
from ..dependencies import records_cache, CACHE_TTL_SECONDS
from ..b2_utils import download_file_from_b2

router = APIRouter(tags=["imports"])


@router.post("/map-data", response_model=MapDataResponse)
async def map_data_endpoint(
    file: UploadFile = File(...),
    mapping_json: str = Form(...),
    db: Session = Depends(get_db)
):
    """
    Map and import data from an uploaded file.
    
    This endpoint accepts a file upload along with a mapping configuration,
    processes the file, and inserts the data into the database according to
    the mapping rules.
    
    Parameters:
    - file: The file to import (CSV, Excel, JSON, or XML)
    - mapping_json: JSON string containing the mapping configuration
    
    Returns:
    - Success status
    - Number of records processed
    - Number of duplicates skipped
    - Target table name
    """
    from ..import_orchestrator import execute_data_import
    from ..models import FileAlreadyImportedException, DuplicateDataException
    
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


@router.post("/map-b2-data", response_model=MapDataResponse)
async def map_b2_data_endpoint(
    request: MapB2DataRequest,
    db: Session = Depends(get_db)
):
    """
    Map and import data from a file stored in Backblaze B2.
    
    This endpoint downloads a file from B2 storage, processes it according
    to the provided mapping configuration, and inserts the data into the database.
    
    Parameters:
    - file_name: Name of the file in B2 storage
    - mapping: Mapping configuration object
    
    Returns:
    - Success status
    - Number of records processed
    - Target table name
    """
    from ..import_orchestrator import execute_data_import
    from ..models import FileAlreadyImportedException, DuplicateDataException
    
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


@router.post("/extract-b2-excel-csv")
async def extract_b2_excel_csv_endpoint(request):
    """
    Extract sheets from an Excel file in B2 storage to CSV format.
    
    This endpoint downloads an Excel file from B2, extracts the specified
    number of rows from each sheet, and returns them in CSV format.
    
    Parameters:
    - file_name: Name of the Excel file in B2 storage
    - rows: Number of rows to extract from each sheet
    
    Returns:
    - Success status
    - Dictionary of sheet names to CSV content
    """
    from ..processors.csv_processor import extract_excel_sheets_to_csv
    from ..schemas import ExtractB2ExcelRequest, ExtractExcelCsvResponse
    
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
