"""
Mapping detection endpoints for analyzing file structure and suggesting configurations.
"""
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from sqlalchemy.orm import Session
import hashlib
import time

from app.db.session import get_db
from app.api.schemas.shared import DetectB2MappingRequest, DetectB2MappingResponse
from app.api.dependencies import records_cache, CACHE_TTL_SECONDS
from app.integrations.b2 import download_file_from_b2
from app.domain.imports.mapper import detect_mapping_from_file

router = APIRouter(tags=["mapping"])


@router.post("/detect-mapping", response_model=DetectB2MappingResponse)
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


@router.post("/detect-b2-mapping", response_model=DetectB2MappingResponse)
async def detect_b2_mapping_endpoint(request: DetectB2MappingRequest):
    """
    Detect mapping configuration from a file in B2 storage.
    
    This endpoint downloads a file from B2, analyzes its structure, and
    automatically detects column names, data types, and suggests a mapping
    configuration.
    
    Parameters:
    - file_name: Name of the file in B2 storage
    
    Returns:
    - Detected file type
    - Suggested mapping configuration
    - List of columns found
    - Number of rows sampled
    """
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
