"""
Import history tracking endpoints for monitoring and auditing data imports.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Optional

from app.db.session import get_db
from app.api.schemas.shared import (
    ImportHistoryListResponse, ImportHistoryRecord,
    ImportHistoryDetailResponse, ImportStatisticsResponse,
    ImportDuplicateRowsResponse, DuplicateDetailResponse,
    DuplicateMergeRequest, DuplicateMergeResponse
)
from app.domain.imports.history import (
    get_import_history,
    get_import_statistics,
    list_duplicate_rows,
    get_duplicate_row_detail,
    resolve_duplicate_row
)

router = APIRouter(prefix="/import-history", tags=["import-history"])


@router.get("", response_model=ImportHistoryListResponse)
async def list_import_history(
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    file_name: Optional[str] = None,
    file_hash: Optional[str] = None,
    source_path: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
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
    - file_name: Filter by original file name
    - file_hash: Filter by SHA-256 hash
    - source_path: Filter by stored source path (e.g. B2 key)
    - file_size_bytes: Filter by file size recorded during import
    - limit: Maximum number of records to return (default: 100)
    - offset: Number of records to skip for pagination (default: 0)
    """
    try:
        records = get_import_history(
            table_name=table_name,
            user_id=user_id,
            status=status,
            file_name=file_name,
            file_hash=file_hash,
            source_path=source_path,
            file_size_bytes=file_size_bytes,
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


@router.get("/{import_id}", response_model=ImportHistoryDetailResponse)
async def get_import_detail(
    import_id: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific import.
    
    Parameters:
    - import_id: UUID of the import to retrieve
    """
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


@router.get("/statistics", response_model=ImportStatisticsResponse)
async def get_import_statistics_endpoint(
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


@router.get("/{import_id}/duplicates", response_model=ImportDuplicateRowsResponse)
async def get_import_duplicate_rows(
    import_id: str,
    limit: int = 20,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Retrieve duplicate rows that were detected during an import.
    """
    if limit <= 0 or limit > 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    try:
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

        import_record = ImportHistoryRecord(**records[0])
        duplicates = list_duplicate_rows(import_id, limit=limit, offset=offset)
        total_count = import_record.duplicates_found or len(duplicates)

        return ImportDuplicateRowsResponse(
            success=True,
            duplicates=duplicates,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve duplicate rows: {str(e)}")


@router.get("/{import_id}/duplicates/{duplicate_id}", response_model=DuplicateDetailResponse)
async def get_duplicate_row_detail_endpoint(
    import_id: str,
    duplicate_id: int,
    db: Session = Depends(get_db)
):
    try:
        detail = get_duplicate_row_detail(import_id, duplicate_id)
        duplicate = detail["duplicate"]
        existing_row = detail.get("existing_row")
        return DuplicateDetailResponse(
            success=True,
            duplicate=duplicate,
            existing_row=existing_row,
            table_name=detail["table_name"],
            uniqueness_columns=detail["uniqueness_columns"]
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve duplicate detail: {str(exc)}")


@router.post(
    "/{import_id}/duplicates/{duplicate_id}/merge",
    response_model=DuplicateMergeResponse
)
async def merge_duplicate_row_endpoint(
    import_id: str,
    duplicate_id: int,
    request: DuplicateMergeRequest,
    db: Session = Depends(get_db)
):
    try:
        result = resolve_duplicate_row(
            import_id=import_id,
            duplicate_id=duplicate_id,
            updates=request.updates or {},
            resolved_by=request.resolved_by,
            note=request.note
        )
        duplicate = result["duplicate"]
        return DuplicateMergeResponse(
            success=True,
            duplicate=duplicate,
            updated_columns=result.get("updated_columns", []),
            existing_row=result.get("existing_row"),
            resolution_details=result.get("resolution_details")
        )
    except ValueError as exc:
        if "not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail=str(exc))
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to merge duplicate row: {str(exc)}")


alias_router = APIRouter(tags=['import-history'])


@alias_router.get("/import-statistics", response_model=ImportStatisticsResponse)
async def get_import_statistics_root(
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    days: int = 30,
    db: Session = Depends(get_db)
):
    return await get_import_statistics_endpoint(
        table_name=table_name,
        user_id=user_id,
        days=days,
        db=db
    )
