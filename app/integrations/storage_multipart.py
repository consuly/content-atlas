"""
Multipart/chunked upload support for S3-compatible storage (B2, AWS S3, etc.)
Enables parallel chunked uploads for large files (>10MB) for significant speed improvements.
"""
import logging
from typing import Dict, Any, List, Optional
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.core.config import settings
from app.integrations.storage import get_storage_client, StorageError, StorageUploadError

logger = logging.getLogger(__name__)


def start_multipart_upload(
    file_name: str,
    folder: str = "uploads",
    content_type: Optional[str] = None
) -> Dict[str, Any]:
    """
    Start a multipart upload session for large files.
    
    This initiates a multipart upload on S3-compatible storage, which allows
    uploading a file in multiple parts that can be uploaded in parallel.
    
    Args:
        file_name: Name of the file to upload
        folder: Folder/prefix to store the file in (default: "uploads")
        content_type: Optional MIME type for the file
    
    Returns:
        Dictionary containing:
        - upload_id: Unique identifier for this multipart upload session
        - file_path: Full path where the file will be stored
        - bucket_name: Name of the storage bucket
    
    Raises:
        StorageUploadError: If multipart upload initialization fails
    """
    try:
        client = get_storage_client()
        
        # Construct the full file path
        file_path = f"{folder}/{file_name}"
        
        # Prepare parameters
        params = {
            'Bucket': settings.storage_bucket_name,
            'Key': file_path,
        }
        
        # Add content type if specified
        if content_type:
            params['ContentType'] = content_type
        
        # Start multipart upload
        response = client.create_multipart_upload(**params)
        
        upload_id = response['UploadId']
        
        logger.info(
            f"Started multipart upload for {file_path} with upload_id: {upload_id}"
        )
        
        return {
            "upload_id": upload_id,
            "file_path": file_path,
            "bucket_name": settings.storage_bucket_name
        }
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"Failed to start multipart upload: {error_code} - {str(e)}")
        raise StorageUploadError(f"Failed to start multipart upload: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error starting multipart upload: {str(e)}")
        raise StorageUploadError(f"Failed to start multipart upload: {str(e)}")


def generate_presigned_upload_part_url(
    file_path: str,
    upload_id: str,
    part_number: int,
    expires_in: int = 3600
) -> str:
    """
    Generate a presigned URL for uploading a specific part of a multipart upload.
    
    This allows the browser to upload individual parts directly to storage
    without going through the backend server.
    
    Args:
        file_path: Full path of the file in storage
        upload_id: The multipart upload session ID
        part_number: Part number (1-indexed, must be between 1 and 10,000)
        expires_in: URL expiration time in seconds (default: 3600 = 1 hour)
    
    Returns:
        Presigned URL for uploading this specific part
    
    Raises:
        StorageError: If URL generation fails
    """
    try:
        client = get_storage_client()
        
        # Generate presigned URL for upload_part operation
        presigned_url = client.generate_presigned_url(
            'upload_part',
            Params={
                'Bucket': settings.storage_bucket_name,
                'Key': file_path,
                'UploadId': upload_id,
                'PartNumber': part_number
            },
            ExpiresIn=expires_in
        )
        
        return presigned_url
        
    except Exception as e:
        logger.error(f"Failed to generate presigned URL for part {part_number}: {str(e)}")
        raise StorageError(f"Failed to generate presigned URL: {str(e)}")


def complete_multipart_upload(
    file_path: str,
    upload_id: str,
    parts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Complete a multipart upload by combining all uploaded parts.
    
    After all parts have been uploaded, this finalizes the multipart upload
    and combines the parts into a single file.
    
    Args:
        file_path: Full path of the file in storage
        upload_id: The multipart upload session ID
        parts: List of uploaded parts, each containing:
            - PartNumber: Part number (1-indexed)
            - ETag: ETag returned from the part upload
    
    Returns:
        Dictionary containing:
        - file_id: The file's ETag (version identifier)
        - file_path: Full path in storage
        - location: Full URL to the file
    
    Raises:
        StorageUploadError: If completion fails
    """
    try:
        client = get_storage_client()
        
        # Sort parts by part number to ensure correct order
        sorted_parts = sorted(parts, key=lambda x: x['PartNumber'])
        
        # Complete the multipart upload
        response = client.complete_multipart_upload(
            Bucket=settings.storage_bucket_name,
            Key=file_path,
            UploadId=upload_id,
            MultipartUpload={'Parts': sorted_parts}
        )
        
        logger.info(
            f"Completed multipart upload for {file_path} with {len(parts)} parts"
        )
        
        return {
            "file_id": response['ETag'].strip('"'),
            "file_path": file_path,
            "location": response.get('Location', '')
        }
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"Failed to complete multipart upload: {error_code} - {str(e)}")
        raise StorageUploadError(f"Failed to complete multipart upload: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error completing multipart upload: {str(e)}")
        raise StorageUploadError(f"Failed to complete multipart upload: {str(e)}")


def abort_multipart_upload(
    file_path: str,
    upload_id: str
) -> bool:
    """
    Abort a multipart upload and clean up any uploaded parts.
    
    This should be called if the upload fails or is cancelled to clean up
    storage space and avoid charges for incomplete uploads.
    
    Args:
        file_path: Full path of the file in storage
        upload_id: The multipart upload session ID
    
    Returns:
        True if abort was successful, False otherwise
    """
    try:
        client = get_storage_client()
        
        client.abort_multipart_upload(
            Bucket=settings.storage_bucket_name,
            Key=file_path,
            UploadId=upload_id
        )
        
        logger.info(f"Aborted multipart upload {upload_id} for {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error aborting multipart upload: {str(e)}")
        return False


def list_multipart_upload_parts(
    file_path: str,
    upload_id: str
) -> List[Dict[str, Any]]:
    """
    List all parts that have been uploaded for a multipart upload.
    
    Useful for resuming failed uploads or verifying upload progress.
    
    Args:
        file_path: Full path of the file in storage
        upload_id: The multipart upload session ID
    
    Returns:
        List of uploaded parts with metadata
    
    Raises:
        StorageError: If listing fails
    """
    try:
        client = get_storage_client()
        
        response = client.list_parts(
            Bucket=settings.storage_bucket_name,
            Key=file_path,
            UploadId=upload_id
        )
        
        parts = []
        for part in response.get('Parts', []):
            parts.append({
                'PartNumber': part['PartNumber'],
                'ETag': part['ETag'],
                'Size': part['Size'],
                'LastModified': part['LastModified']
            })
        
        return parts
        
    except Exception as e:
        logger.error(f"Error listing multipart upload parts: {str(e)}")
        raise StorageError(f"Failed to list upload parts: {str(e)}")


def calculate_part_ranges(
    file_size: int,
    part_size: int = 5 * 1024 * 1024  # 5MB default
) -> List[Dict[str, int]]:
    """
    Calculate byte ranges for splitting a file into parts.
    
    Args:
        file_size: Total size of the file in bytes
        part_size: Size of each part in bytes (default: 5MB)
            Note: S3 requires parts to be at least 5MB (except the last part)
    
    Returns:
        List of dictionaries containing:
        - part_number: Part number (1-indexed)
        - start: Starting byte position (inclusive)
        - end: Ending byte position (inclusive)
        - size: Size of this part in bytes
    """
    # Ensure part size is at least 5MB (S3 requirement)
    min_part_size = 5 * 1024 * 1024
    if part_size < min_part_size:
        part_size = min_part_size
    
    parts = []
    part_number = 1
    start = 0
    
    while start < file_size:
        end = min(start + part_size - 1, file_size - 1)
        size = end - start + 1
        
        parts.append({
            'part_number': part_number,
            'start': start,
            'end': end,
            'size': size
        })
        
        part_number += 1
        start = end + 1
    
    return parts


def get_optimal_part_size(file_size: int) -> int:
    """
    Calculate optimal part size based on file size.
    
    S3 has a limit of 10,000 parts per upload, so we need to ensure
    the part size is large enough to stay under this limit.
    
    Args:
        file_size: Total size of the file in bytes
    
    Returns:
        Optimal part size in bytes (minimum 5MB)
    """
    min_part_size = 5 * 1024 * 1024  # 5MB
    max_parts = 10000
    
    # Calculate minimum part size needed to stay under 10,000 parts
    required_part_size = file_size // max_parts
    
    # Use the larger of minimum part size or required part size
    part_size = max(min_part_size, required_part_size)
    
    # Round up to nearest MB for cleaner sizes
    part_size = ((part_size + 1024 * 1024 - 1) // (1024 * 1024)) * (1024 * 1024)
    
    return part_size


def download_file(file_path: str) -> bytes:
    """
    Download a file from S3-compatible storage.
    
    This function delegates to the main storage module for consistency.
    Provided here for convenience so users can import from storage_multipart.
    
    Args:
        file_path: The full path of the file in storage (e.g., "uploads/file.csv")
    
    Returns:
        File content as bytes
    
    Raises:
        StorageDownloadError: If download fails
    """
    from app.integrations.storage import download_file as storage_download
    return storage_download(file_path)
