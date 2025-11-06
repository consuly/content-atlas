"""
Async task management endpoints for long-running operations.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
import uuid

from app.api.schemas.shared import MapB2DataAsyncRequest, AsyncTaskStatus, MapDataResponse, MappingConfig
from app.api.dependencies import task_storage
from app.integrations.b2 import download_file_from_b2

router = APIRouter(tags=["tasks"])


def process_b2_data_async(task_id: str, file_name: str, mapping: MappingConfig):
    """Background task for processing B2 data asynchronously."""
    from app.domain.imports.orchestrator import execute_data_import
    from app.db.models import FileAlreadyImportedException, DuplicateDataException
    
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
            duplicates_skipped=result.get("duplicates_skipped", 0),
            duplicate_rows=result.get("duplicate_rows"),
            duplicate_rows_count=result.get("duplicate_rows_count"),
            import_id=result.get("import_id"),
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


@router.post("/map-b2-data-async", response_model=AsyncTaskStatus)
async def map_b2_data_async_endpoint(
    request: MapB2DataAsyncRequest,
    background_tasks: BackgroundTasks
):
    """
    Start async processing of B2 data.
    
    This endpoint queues a background task to process a large file from B2
    storage. Use the returned task_id to check progress via /tasks/{task_id}.
    
    Parameters:
    - file_name: Name of the file in B2 storage
    - mapping: Mapping configuration
    
    Returns:
    - Task ID for tracking progress
    - Initial task status
    """
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


@router.get("/tasks/{task_id}", response_model=AsyncTaskStatus)
async def get_task_status(task_id: str):
    """
    Get the status of an async task.
    
    Poll this endpoint to check the progress of a background task
    started via /map-b2-data-async.
    
    Parameters:
    - task_id: UUID of the task
    
    Returns:
    - Current task status
    - Progress percentage (if available)
    - Result (if completed)
    - Error message (if failed)
    """
    if task_id not in task_storage:
        raise HTTPException(status_code=404, detail="Task not found")

    return task_storage[task_id]
