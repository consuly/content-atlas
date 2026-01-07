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
    DuplicateMergeRequest, DuplicateMergeResponse,
    ImportMappingErrorsResponse, MappingErrorHistoryRecord,
    ImportValidationFailuresResponse, ValidationFailureDetailResponse,
    ResolveValidationFailureRequest, ResolveValidationFailureResponse,
    MappingConfig,
    RowUpdatesListResponse, RowUpdateDetailResponse,
    RollbackUpdateRequest, RollbackUpdateResponse,
    RollbackAllUpdatesRequest, RollbackAllUpdatesResponse
)
from app.domain.imports.history import (
    get_import_history,
    get_import_statistics,
    list_duplicate_rows,
    get_duplicate_row_detail,
    resolve_duplicate_row,
    get_mapping_errors,
    list_validation_failures,
    get_validation_failure_detail,
    resolve_validation_failure,
    list_all_duplicate_rows,
    list_all_validation_failures,
    list_all_mapping_errors,
    create_import_history_table
)
from app.domain.imports.rollback import (
    list_row_updates,
    list_all_row_updates,
    get_row_update_detail,
    rollback_single_update,
    rollback_import_updates
)
from app.db.models import insert_records
import json

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


@router.get("/duplicates", response_model=ImportDuplicateRowsResponse)
async def list_all_duplicates(
    limit: int = 100,
    offset: int = 0,
    file_name: Optional[str] = None,
    table_name: Optional[str] = None,
    include_resolved: bool = False,
    db: Session = Depends(get_db)
):
    """
    List duplicate rows across all imports.
    """
    try:
        # Ensure schema is up to date to prevent column missing errors
        create_import_history_table()
        
        duplicates, total_count = list_all_duplicate_rows(
            limit=limit,
            offset=offset,
            file_name=file_name,
            table_name=table_name,
            include_resolved=include_resolved
        )
        
        return ImportDuplicateRowsResponse(
            success=True,
            duplicates=duplicates,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        import traceback
        print(f"ERROR listing duplicates: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to list duplicates: {str(e)}")


@router.get("/validation-failures", response_model=ImportValidationFailuresResponse)
async def list_all_validation_failures_endpoint(
    limit: int = 100,
    offset: int = 0,
    file_name: Optional[str] = None,
    table_name: Optional[str] = None,
    include_resolved: bool = False,
    db: Session = Depends(get_db)
):
    """
    List validation failures across all imports.
    """
    try:
        failures, total_count = list_all_validation_failures(
            limit=limit,
            offset=offset,
            file_name=file_name,
            table_name=table_name,
            include_resolved=include_resolved
        )
        
        return ImportValidationFailuresResponse(
            success=True,
            failures=failures,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list validation failures: {str(e)}")


@router.get("/mapping-errors", response_model=ImportMappingErrorsResponse)
async def list_all_mapping_errors_endpoint(
    limit: int = 100,
    offset: int = 0,
    file_name: Optional[str] = None,
    table_name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    List mapping errors across all imports.
    """
    try:
        errors, total_count = list_all_mapping_errors(
            limit=limit,
            offset=offset,
            file_name=file_name,
            table_name=table_name
        )
        
        return ImportMappingErrorsResponse(
            success=True,
            errors=errors,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list mapping errors: {str(e)}")


@router.get("/row-updates", response_model=RowUpdatesListResponse)
async def list_all_row_updates_endpoint(
    limit: int = 100,
    offset: int = 0,
    file_name: Optional[str] = None,
    table_name: Optional[str] = None,
    include_rolled_back: bool = False,
    db: Session = Depends(get_db)
):
    """
    List row updates across all imports.
    """
    try:
        updates, total_count = list_all_row_updates(
            limit=limit,
            offset=offset,
            file_name=file_name,
            table_name=table_name,
            include_rolled_back=include_rolled_back
        )
        
        return RowUpdatesListResponse(
            success=True,
            updates=updates,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list row updates: {str(e)}")


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
        duplicates = list_duplicate_rows(import_id, limit=limit, offset=offset, include_existing_row=True)
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


@router.get("/{import_id}/mapping-errors", response_model=ImportMappingErrorsResponse)
async def get_import_mapping_errors(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Retrieve mapping errors that occurred during an import.
    """
    if limit <= 0 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")

    try:
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")

        import_record = ImportHistoryRecord(**records[0])
        errors_data = get_mapping_errors(import_id=import_id, limit=limit, offset=offset)
        
        # Convert to Pydantic models
        errors = [MappingErrorHistoryRecord(**err) for err in errors_data]
        total_count = import_record.mapping_errors_count or len(errors)

        return ImportMappingErrorsResponse(
            success=True,
            errors=errors,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve mapping errors: {str(e)}")


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
            note=request.note,
            strategy=request.strategy or 'merge'
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


@router.get("/{import_id}/validation-failures", response_model=ImportValidationFailuresResponse)
async def list_validation_failures_endpoint(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    include_resolved: bool = False,
    db: Session = Depends(get_db)
):
    """
    List validation failures for a specific import.
    """
    try:
        failures = list_validation_failures(
            import_id=import_id,
            limit=limit,
            offset=offset,
            include_resolved=include_resolved
        )
        
        # Get total count (simplified for now)
        total_count = len(failures) + offset
        
        return ImportValidationFailuresResponse(
            success=True,
            failures=failures,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list validation failures: {str(e)}")


@router.get("/{import_id}/validation-failures/{failure_id}", response_model=ValidationFailureDetailResponse)
async def get_validation_failure_detail_endpoint(
    import_id: str,
    failure_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific validation failure.
    """
    try:
        failure = get_validation_failure_detail(
            import_id=import_id,
            failure_id=failure_id
        )
        
        return ValidationFailureDetailResponse(
            success=True,
            failure=failure,
            table_name=failure.get("table_name")
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get validation failure detail: {str(e)}")


@router.post("/{import_id}/validation-failures/{failure_id}/resolve", response_model=ResolveValidationFailureResponse)
async def resolve_validation_failure_endpoint(
    import_id: str,
    failure_id: int,
    request: ResolveValidationFailureRequest,
    db: Session = Depends(get_db)
):
    """
    Resolve a validation failure.
    """
    try:
        # If action involves insertion, perform it first
        if request.action in ['inserted_as_is', 'inserted_corrected']:
            # Get failure details to access the record
            failure = get_validation_failure_detail(import_id, failure_id)
            record_to_insert = failure['record']
            
            # If corrected, merge corrections
            if request.action == 'inserted_corrected' and request.corrected_data:
                record_to_insert.update(request.corrected_data)
            
            # Get import history to retrieve mapping config (needed for type coercion)
            import_records = get_import_history(import_id=import_id, limit=1)
            if not import_records:
                raise ValueError("Import history not found")
            
            import_record = import_records[0]
            table_name = import_record['table_name']
            mapping_config_data = import_record.get('mapping_config')
            
            mapping_config = None
            if mapping_config_data:
                if isinstance(mapping_config_data, str):
                    mapping_config_data = json.loads(mapping_config_data)
                mapping_config = MappingConfig.model_validate(mapping_config_data)
            
            # Insert the record
            # We pass pre_mapped=False so type coercion happens based on schema
            # We pass import_id to link the record to the import
            insert_records(
                engine=db.get_bind(),
                table_name=table_name,
                records=[record_to_insert],
                config=mapping_config,
                import_id=import_id,
                pre_mapped=False 
            )

        result = resolve_validation_failure(
            import_id=import_id,
            failure_id=failure_id,
            action=request.action,
            corrected_data=request.corrected_data,
            resolved_by=request.resolved_by,
            note=request.note
        )
        
        return ResolveValidationFailureResponse(
            success=True,
            failure=result,
            message=f"Validation failure resolved with action: {request.action}"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resolve validation failure: {str(e)}")


@router.get("/{import_id}/updates", response_model=RowUpdatesListResponse)
async def list_import_row_updates(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    include_rolled_back: bool = False,
    db: Session = Depends(get_db)
):
    """
    List all row updates from an import with rollback information.
    
    Parameters:
    - import_id: UUID of the import
    - limit: Maximum number of updates to return (default: 100)
    - offset: Number of updates to skip (default: 0)
    - include_rolled_back: Include already rolled back updates (default: False)
    """
    try:
        # Verify import exists
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
        
        updates, total_count = list_row_updates(
            import_id=import_id,
            limit=limit,
            offset=offset,
            include_rolled_back=include_rolled_back
        )
        
        return RowUpdatesListResponse(
            success=True,
            updates=updates,
            total_count=total_count,
            limit=limit,
            offset=offset
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list row updates: {str(e)}")


@router.get("/{import_id}/updates/{update_id}", response_model=RowUpdateDetailResponse)
async def get_import_row_update_detail(
    import_id: str,
    update_id: int,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific row update.
    
    Parameters:
    - import_id: UUID of the import
    - update_id: ID of the row update
    """
    try:
        # Verify import exists
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
        
        detail = get_row_update_detail(import_id=import_id, update_id=update_id)
        
        return RowUpdateDetailResponse(
            success=True,
            update=detail["update"],
            current_row=detail.get("current_row")
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get row update detail: {str(e)}")


@router.post("/{import_id}/updates/{update_id}/rollback", response_model=RollbackUpdateResponse)
async def rollback_import_row_update(
    import_id: str,
    update_id: int,
    request: RollbackUpdateRequest,
    db: Session = Depends(get_db)
):
    """
    Rollback a single row update.
    
    Parameters:
    - import_id: UUID of the import
    - update_id: ID of the row update to rollback
    - rolled_back_by: User performing the rollback (optional)
    - force: Force rollback even if there's a conflict (default: False)
    """
    try:
        # Verify import exists
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
        
        result = rollback_single_update(
            import_id=import_id,
            update_id=update_id,
            rolled_back_by=request.rolled_back_by,
            force=request.force
        )
        
        return RollbackUpdateResponse(
            success=result["success"],
            message=result["message"],
            update=result["update"],
            conflict=result.get("conflict")
        )
    except ValueError as e:
        if "already been rolled back" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rollback update: {str(e)}")


@router.post("/{import_id}/rollback-all", response_model=RollbackAllUpdatesResponse)
async def rollback_all_import_updates(
    import_id: str,
    request: RollbackAllUpdatesRequest,
    db: Session = Depends(get_db)
):
    """
    Rollback all row updates from an import.
    
    Parameters:
    - import_id: UUID of the import
    - rolled_back_by: User performing the rollback (optional)
    - skip_conflicts: Skip updates with conflicts instead of stopping (default: False)
    """
    try:
        # Verify import exists
        records = get_import_history(import_id=import_id, limit=1)
        if not records:
            raise HTTPException(status_code=404, detail=f"Import {import_id} not found")
        
        result = rollback_import_updates(
            import_id=import_id,
            rolled_back_by=request.rolled_back_by,
            skip_conflicts=request.skip_conflicts
        )
        
        return RollbackAllUpdatesResponse(
            success=result["success"],
            message=result["message"],
            updates_rolled_back=result["updates_rolled_back"],
            conflicts=result.get("conflicts")
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to rollback all updates: {str(e)}")


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
