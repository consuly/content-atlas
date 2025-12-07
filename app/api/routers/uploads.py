"""
File upload endpoints for managing files in B2 storage.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional
import traceback

from app.db.session import get_db
from app.api.schemas.shared import (
    UploadFileResponse, UploadedFileInfo, FileExistsResponse,
    UploadedFilesListResponse, UploadedFileDetailResponse, DeleteFileResponse,
    CheckDuplicateRequest, CheckDuplicateResponse,
    CompleteUploadRequest, CompleteUploadResponse,
    StartMultipartUploadRequest, StartMultipartUploadResponse,
    CompleteMultipartUploadRequest, CompleteMultipartUploadResponse,
    AbortMultipartUploadRequest, AbortMultipartUploadResponse
)
from app.integrations.storage import (
    upload_file as upload_file_to_storage,
    delete_file as delete_file_from_storage,
    generate_presigned_upload_url
)
from app.integrations.storage_multipart import (
    start_multipart_upload,
    generate_presigned_upload_part_url,
    complete_multipart_upload,
    abort_multipart_upload,
    calculate_part_ranges,
    get_optimal_part_size
)
from app.domain.uploads.uploaded_files import (
    insert_uploaded_file, get_uploaded_file_by_name, get_uploaded_file_by_id,
    get_uploaded_files, get_uploaded_files_count, delete_uploaded_file,
    delete_imported_rows_for_file,
    update_file_status, get_uploaded_file_by_hash
)
from app.core.config import settings

router = APIRouter(tags=["uploads"])

MAX_UPLOAD_BYTES = settings.upload_max_file_size_mb * 1024 * 1024


def _ensure_within_size_limit(file_size: int, file_name: str) -> None:
    """Raise an HTTPException if a file exceeds the configured upload limit."""
    if file_size > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"{file_name} is too large. "
                f"Maximum allowed upload size is {settings.upload_max_file_size_mb}MB."
            ),
        )


@router.post("/upload-to-b2", response_model=UploadFileResponse)
async def upload_file_to_storage_endpoint(
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
        _ensure_within_size_limit(file_size, file.filename)
        print(f"[UPLOAD] File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
        
        # Upload to storage
        print(f"[UPLOAD] Calling upload_file_to_storage()...")
        print(f"[UPLOAD] Target folder: uploads")
        print(f"[UPLOAD] Target filename: {file.filename}")
        
        storage_result = upload_file_to_storage(
            file_content=file_content,
            file_name=file.filename,
            folder="uploads"
        )
        
        print(f"[UPLOAD] Storage upload successful!")
        print(f"[UPLOAD] File ID: {storage_result['file_id']}")
        print(f"[UPLOAD] File Path: {storage_result['file_path']}")
        print(f"[UPLOAD] File Size: {storage_result['size']} bytes")
        
        # Store in database
        print(f"[UPLOAD] Storing file metadata in database...")
        uploaded_file = insert_uploaded_file(
            file_name=file.filename,
            b2_file_id=storage_result["file_id"],
            b2_file_path=storage_result["file_path"],
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


@router.post("/upload-to-b2/overwrite", response_model=UploadFileResponse)
async def overwrite_file_in_storage_endpoint(
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
    try:
        # Check if file exists
        existing_file = get_uploaded_file_by_name(file.filename)
        
        if existing_file:
            # Delete old file from storage
            delete_file_from_storage(existing_file["b2_file_path"])
            # Delete old database record
            delete_uploaded_file(existing_file["id"])
        
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)
        _ensure_within_size_limit(file_size, file.filename)
        
        # Upload new version to storage
        storage_result = upload_file_to_storage(
            file_content=file_content,
            file_name=file.filename,
            folder="uploads"
        )
        
        # Store in database
        uploaded_file = insert_uploaded_file(
            file_name=file.filename,
            b2_file_id=storage_result["file_id"],
            b2_file_path=storage_result["file_path"],
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


@router.get("/uploaded-files", response_model=UploadedFilesListResponse)
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


@router.get("/uploaded-files/{file_id}", response_model=UploadedFileDetailResponse)
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


@router.delete("/uploaded-files/{file_id}", response_model=DeleteFileResponse)
async def delete_uploaded_file_endpoint(
    file_id: str,
    delete_table_data: bool = Query(
        False,
        description="Also delete imported rows and history linked to this file's mapped table"
    ),
    db: Session = Depends(get_db)
):
    """
    Delete an uploaded file from B2 and database.
    
    Parameters:
    - file_id: UUID of the uploaded file to delete
    - delete_table_data: If true, remove rows from the mapped table that were created by this upload
    
    Returns:
    - Success message
    """
    try:
        file = get_uploaded_file_by_id(file_id)
        
        if not file:
            raise HTTPException(status_code=404, detail=f"File {file_id} not found")
        
        storage_deleted = delete_file_from_storage(file["b2_file_path"])
        if not storage_deleted:
            raise HTTPException(status_code=500, detail="Failed to delete file from storage")

        cleanup_summary = None
        cleanup_warning: Optional[str] = None
        if delete_table_data:
            try:
                cleanup_summary = delete_imported_rows_for_file(file)
            except Exception as cleanup_error:
                cleanup_warning = f"Failed to remove table data: {cleanup_error}"
        
        db_deleted = delete_uploaded_file(file_id)
        if not db_deleted:
            raise HTTPException(status_code=500, detail="Failed to delete file from database")

        message = f"File '{file['file_name']}' deleted successfully"
        warnings = []

        if cleanup_summary:
            if cleanup_summary.get("rows_removed"):
                table_label = cleanup_summary.get("table_name") or "mapped table"
                message += f" and removed {cleanup_summary['rows_removed']} row(s) from {table_label}"
            elif cleanup_summary.get("reason"):
                warnings.append(f"No table data removed: {cleanup_summary['reason']}")
        if cleanup_warning:
            warnings.append(cleanup_warning)
        
        return DeleteFileResponse(
            success=True,
            message=message,
            data_deleted=cleanup_summary["data_removed"] if cleanup_summary else None,
            rows_removed=cleanup_summary["rows_removed"] if cleanup_summary else None,
            table_name=cleanup_summary["table_name"] if cleanup_summary else None,
            import_ids=cleanup_summary["import_ids"] if cleanup_summary else None,
            warnings=warnings or None
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@router.patch("/uploaded-files/{file_id}/status", response_model=UploadedFileDetailResponse)
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


@router.post("/check-duplicate", response_model=CheckDuplicateResponse)
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
    try:
        print(f"\n[CHECK-DUPLICATE] Checking file: {request.file_name}")
        print(f"[CHECK-DUPLICATE] File hash: {request.file_hash}")
        print(f"[CHECK-DUPLICATE] File size: {request.file_size}")
        
        # Check if file with same hash exists
        existing_file = get_uploaded_file_by_hash(request.file_hash)
        
        if existing_file:
            # File is a duplicate
            print(f"[CHECK-DUPLICATE] Duplicate found: {existing_file['file_name']}")
            return CheckDuplicateResponse(
                success=True,
                is_duplicate=True,
                message=f"File already exists: {existing_file['file_name']}",
                existing_file=UploadedFileInfo(**existing_file),
                can_upload=False
            )
        
        print(f"[CHECK-DUPLICATE] No duplicate found, generating presigned upload URL...")
        
        # File is not a duplicate, generate presigned upload URL
        upload_auth = generate_presigned_upload_url(
            file_name=request.file_name,
            folder="uploads",
            content_type=None  # Will be set by browser
        )
        
        print(f"[CHECK-DUPLICATE] Presigned upload URL generated successfully")
        
        return CheckDuplicateResponse(
            success=True,
            is_duplicate=False,
            message="File can be uploaded",
            can_upload=True,
            upload_authorization=upload_auth
        )
        
    except Exception as e:
        print(f"\n[CHECK-DUPLICATE ERROR] Error: {type(e).__name__}: {str(e)}")
        print(f"[CHECK-DUPLICATE ERROR] Traceback:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Duplicate check failed: {str(e)}")


@router.post("/complete-upload", response_model=CompleteUploadResponse)
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


@router.post("/start-multipart-upload", response_model=StartMultipartUploadResponse)
async def start_multipart_upload_endpoint(
    request: StartMultipartUploadRequest,
    db: Session = Depends(get_db)
):
    """
    Start a multipart upload for large files (>10MB).
    
    This endpoint initiates a multipart upload session and returns presigned URLs
    for each part, allowing the browser to upload chunks in parallel directly to B2.
    
    Parameters:
    - file_name: Name of the file to upload
    - file_size: Total size of the file in bytes
    - file_hash: SHA-256 hash of the file content
    - content_type: Optional MIME type of the file
    
    Returns:
    - upload_id: Unique identifier for this multipart upload session
    - file_path: Full path where the file will be stored
    - part_size: Size of each part in bytes
    - total_parts: Number of parts the file will be split into
    - part_urls: List of presigned URLs for uploading each part
    """
    try:
        print(f"\n[MULTIPART-START] Starting multipart upload for: {request.file_name}")
        print(f"[MULTIPART-START] File size: {request.file_size} bytes ({request.file_size / (1024*1024):.2f} MB)")
        print(f"[MULTIPART-START] File hash: {request.file_hash}")
        
        # Check if file with same hash exists
        existing_file = get_uploaded_file_by_hash(request.file_hash)
        
        if existing_file:
            print(f"[MULTIPART-START] Duplicate found: {existing_file['file_name']}")
            raise HTTPException(
                status_code=409,
                detail=f"File already exists: {existing_file['file_name']}"
            )
        
        # Ensure file size is within limits
        _ensure_within_size_limit(request.file_size, request.file_name)
        
        # Calculate optimal part size
        part_size = get_optimal_part_size(request.file_size)
        print(f"[MULTIPART-START] Optimal part size: {part_size / (1024*1024):.2f} MB")
        
        # Calculate part ranges
        part_ranges = calculate_part_ranges(request.file_size, part_size)
        total_parts = len(part_ranges)
        print(f"[MULTIPART-START] Total parts: {total_parts}")
        
        # Start multipart upload
        multipart_result = start_multipart_upload(
            file_name=request.file_name,
            folder="uploads",
            content_type=request.content_type
        )
        
        upload_id = multipart_result["upload_id"]
        file_path = multipart_result["file_path"]
        
        print(f"[MULTIPART-START] Upload ID: {upload_id}")
        print(f"[MULTIPART-START] File path: {file_path}")
        
        # Generate presigned URLs for each part
        part_urls = []
        for part_range in part_ranges:
            part_number = part_range["part_number"]
            part_url = generate_presigned_upload_part_url(
                file_path=file_path,
                upload_id=upload_id,
                part_number=part_number,
                expires_in=3600  # 1 hour
            )
            part_urls.append(part_url)
        
        print(f"[MULTIPART-START] Generated {len(part_urls)} presigned URLs")
        print(f"[MULTIPART-START] Multipart upload started successfully")
        
        return StartMultipartUploadResponse(
            success=True,
            upload_id=upload_id,
            file_path=file_path,
            part_size=part_size,
            total_parts=total_parts,
            part_urls=part_urls,
            message=f"Multipart upload started with {total_parts} parts"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n[MULTIPART-START ERROR] Error: {type(e).__name__}: {str(e)}")
        print(f"[MULTIPART-START ERROR] Traceback:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to start multipart upload: {str(e)}")


@router.post("/complete-multipart-upload", response_model=CompleteMultipartUploadResponse)
async def complete_multipart_upload_endpoint(
    request: CompleteMultipartUploadRequest,
    db: Session = Depends(get_db)
):
    """
    Complete a multipart upload after all parts have been uploaded.
    
    This endpoint finalizes the multipart upload by combining all parts into
    a single file and saving the metadata to the database.
    
    Parameters:
    - file_name: Name of the uploaded file
    - file_hash: SHA-256 hash of the file content
    - file_size: Total size of the file in bytes
    - content_type: MIME type of the file
    - upload_id: The multipart upload session ID
    - file_path: Full path in B2 bucket
    - parts: List of uploaded parts with PartNumber and ETag
    
    Returns:
    - File metadata record
    """
    try:
        print(f"\n[MULTIPART-COMPLETE] Completing multipart upload for: {request.file_name}")
        print(f"[MULTIPART-COMPLETE] Upload ID: {request.upload_id}")
        print(f"[MULTIPART-COMPLETE] Total parts: {len(request.parts)}")
        
        # Complete the multipart upload in B2
        completion_result = complete_multipart_upload(
            file_path=request.file_path,
            upload_id=request.upload_id,
            parts=request.parts
        )
        
        print(f"[MULTIPART-COMPLETE] B2 file ID: {completion_result['file_id']}")
        
        # Save file metadata to database
        uploaded_file = insert_uploaded_file(
            file_name=request.file_name,
            b2_file_id=completion_result["file_id"],
            b2_file_path=request.file_path,
            file_size=request.file_size,
            content_type=request.content_type,
            user_id=None,  # TODO: Get from auth context
            file_hash=request.file_hash
        )
        
        print(f"[MULTIPART-COMPLETE] Database record created: {uploaded_file['id']}")
        print(f"[MULTIPART-COMPLETE] Multipart upload completed successfully")
        
        return CompleteMultipartUploadResponse(
            success=True,
            message="Multipart upload completed successfully",
            file=UploadedFileInfo(**uploaded_file)
        )
        
    except Exception as e:
        print(f"\n[MULTIPART-COMPLETE ERROR] Error: {type(e).__name__}: {str(e)}")
        print(f"[MULTIPART-COMPLETE ERROR] Traceback:")
        print(traceback.format_exc())
        
        # Try to abort the multipart upload to clean up
        try:
            abort_multipart_upload(request.file_path, request.upload_id)
            print(f"[MULTIPART-COMPLETE ERROR] Aborted multipart upload {request.upload_id}")
        except Exception as abort_error:
            print(f"[MULTIPART-COMPLETE ERROR] Failed to abort upload: {abort_error}")
        
        raise HTTPException(status_code=500, detail=f"Failed to complete multipart upload: {str(e)}")


@router.post("/abort-multipart-upload", response_model=AbortMultipartUploadResponse)
async def abort_multipart_upload_endpoint(
    request: AbortMultipartUploadRequest,
    db: Session = Depends(get_db)
):
    """
    Abort a multipart upload and clean up any uploaded parts.
    
    This should be called if the upload fails or is cancelled to clean up
    storage space and avoid charges for incomplete uploads.
    
    Parameters:
    - upload_id: The multipart upload session ID
    - file_path: Full path in B2 bucket
    
    Returns:
    - Success message
    """
    try:
        print(f"\n[MULTIPART-ABORT] Aborting multipart upload")
        print(f"[MULTIPART-ABORT] Upload ID: {request.upload_id}")
        print(f"[MULTIPART-ABORT] File path: {request.file_path}")
        
        success = abort_multipart_upload(
            file_path=request.file_path,
            upload_id=request.upload_id
        )
        
        if success:
            print(f"[MULTIPART-ABORT] Multipart upload aborted successfully")
            return AbortMultipartUploadResponse(
                success=True,
                message="Multipart upload aborted successfully"
            )
        else:
            print(f"[MULTIPART-ABORT] Failed to abort multipart upload")
            raise HTTPException(status_code=500, detail="Failed to abort multipart upload")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n[MULTIPART-ABORT ERROR] Error: {type(e).__name__}: {str(e)}")
        print(f"[MULTIPART-ABORT ERROR] Traceback:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to abort multipart upload: {str(e)}")
