"""
S3-compatible storage integration for Backblaze B2, AWS S3, MinIO, etc.
Uses boto3 for universal S3-compatible storage operations.
"""
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

from app.core.config import settings

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Base exception for storage operations."""
    pass


class StorageConnectionError(StorageError):
    """Raised when storage connection fails."""
    pass


class StorageUploadError(StorageError):
    """Raised when file upload fails."""
    pass


class StorageDownloadError(StorageError):
    """Raised when file download fails."""
    pass


def get_storage_client():
    """
    Get S3-compatible storage client.
    
    Works with:
    - Backblaze B2 (S3-compatible API)
    - AWS S3
    - MinIO
    - Wasabi
    - DigitalOcean Spaces
    - Any S3-compatible storage
    
    Returns:
        boto3 S3 client configured for the storage provider
    
    Raises:
        ValueError: If storage configuration is incomplete
    """
    if not all([settings.storage_access_key_id, settings.storage_secret_access_key, settings.storage_bucket_name]):
        raise ValueError(
            "Storage configuration is incomplete. Please set STORAGE_ACCESS_KEY_ID, "
            "STORAGE_SECRET_ACCESS_KEY, and STORAGE_BUCKET_NAME in your environment."
        )
    
    # Configure boto3 client
    config = Config(
        signature_version='s3v4',
        retries={'max_attempts': 3, 'mode': 'standard'}
    )
    
    client_kwargs = {
        'service_name': 's3',
        'aws_access_key_id': settings.storage_access_key_id,
        'aws_secret_access_key': settings.storage_secret_access_key,
        'config': config,
    }
    
    # Add endpoint URL for non-AWS providers (B2, MinIO, etc.)
    if settings.storage_endpoint_url:
        client_kwargs['endpoint_url'] = settings.storage_endpoint_url
    
    # Add region if specified
    if settings.storage_region:
        client_kwargs['region_name'] = settings.storage_region
    
    try:
        return boto3.client(**client_kwargs)
    except Exception as e:
        logger.error(f"Failed to create storage client: {e}")
        raise StorageConnectionError(f"Failed to connect to storage: {str(e)}")


def upload_file(file_content: bytes, file_name: str, folder: str = "uploads") -> Dict[str, Any]:
    """
    Upload a file to S3-compatible storage.
    
    Args:
        file_content: The file content as bytes
        file_name: The name for the file
        folder: The folder/prefix to store the file in (default: "uploads")
    
    Returns:
        Dictionary with upload details:
        - file_id: The file's ETag (version identifier)
        - file_name: Original file name
        - file_path: Full path in storage
        - size: File size in bytes
    
    Raises:
        StorageUploadError: If upload fails
    """
    try:
        client = get_storage_client()
        
        # Construct the full file path with folder
        file_path = f"{folder}/{file_name}"
        
        # Upload file
        response = client.put_object(
            Bucket=settings.storage_bucket_name,
            Key=file_path,
            Body=file_content
        )
        
        return {
            "file_id": response['ETag'].strip('"'),  # Remove quotes from ETag
            "file_name": file_name,
            "file_path": file_path,
            "size": len(file_content)
        }
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"Storage upload failed: {error_code} - {str(e)}")
        raise StorageUploadError(f"Upload failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during upload: {str(e)}")
        raise StorageUploadError(f"Upload failed: {str(e)}")


def download_file(file_path: str) -> bytes:
    """
    Download a file from S3-compatible storage.
    
    Args:
        file_path: The full path of the file in storage (e.g., "uploads/file.csv")
    
    Returns:
        File content as bytes
    
    Raises:
        StorageDownloadError: If download fails
    """
    try:
        client = get_storage_client()
        
        response = client.get_object(
            Bucket=settings.storage_bucket_name,
            Key=file_path
        )
        
        return response['Body'].read()
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            raise StorageDownloadError(f"File not found: {file_path}")
        logger.error(f"Storage download failed: {error_code} - {str(e)}")
        raise StorageDownloadError(f"Download failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during download: {str(e)}")
        raise StorageDownloadError(f"Download failed: {str(e)}")


def delete_file(file_path: str) -> bool:
    """
    Delete a file from S3-compatible storage.
    
    Args:
        file_path: The full path of the file in storage (e.g., "uploads/file.csv")
    
    Returns:
        True if deletion was successful, False otherwise
    """
    try:
        client = get_storage_client()
        
        client.delete_object(
            Bucket=settings.storage_bucket_name,
            Key=file_path
        )
        
        return True
        
    except Exception as e:
        logger.error(f"Error deleting file from storage: {str(e)}")
        return False


def file_exists(file_path: str) -> bool:
    """
    Check if a file exists in S3-compatible storage.
    
    Args:
        file_path: The full path of the file in storage (e.g., "uploads/file.csv")
    
    Returns:
        True if file exists, False otherwise
    """
    try:
        client = get_storage_client()
        
        client.head_object(
            Bucket=settings.storage_bucket_name,
            Key=file_path
        )
        
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            return False
        logger.error(f"Error checking file existence: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking file existence: {str(e)}")
        return False


def generate_presigned_upload_url(
    file_name: str,
    folder: str = "uploads",
    expires_in: int = 3600,
    content_type: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate a pre-signed URL for direct browser-to-storage upload.
    
    This is the recommended way to handle large file uploads. The browser
    uploads directly to storage without going through the backend server.
    
    Args:
        file_name: The name of the file to upload
        folder: The folder/prefix to store the file in (default: "uploads")
        expires_in: URL expiration time in seconds (default: 3600 = 1 hour)
        content_type: Optional MIME type for the file
    
    Returns:
        Dictionary containing:
        - upload_url: The pre-signed URL for PUT request
        - file_path: The full path where the file will be stored
        - method: HTTP method to use (PUT)
        - expires_in: Expiration time in seconds
        - fields: Additional fields to include (if using POST)
    
    Raises:
        StorageError: If URL generation fails
    """
    try:
        client = get_storage_client()
        
        # Construct the full file path
        file_path = f"{folder}/{file_name}"
        
        # Prepare parameters for pre-signed URL
        params = {
            'Bucket': settings.storage_bucket_name,
            'Key': file_path,
        }
        
        # Add content type if specified
        if content_type:
            params['ContentType'] = content_type
        
        # Generate pre-signed URL for PUT operation
        presigned_url = client.generate_presigned_url(
            'put_object',
            Params=params,
            ExpiresIn=expires_in
        )
        
        return {
            "upload_url": presigned_url,
            "file_path": file_path,
            "method": "PUT",
            "expires_in": expires_in,
            "content_type": content_type
        }
        
    except Exception as e:
        logger.error(f"Failed to generate presigned upload URL: {str(e)}")
        raise StorageError(f"Failed to generate upload URL: {str(e)}")


def generate_presigned_download_url(
    file_path: str,
    expires_in: int = 3600,
    filename: Optional[str] = None
) -> str:
    """
    Generate a pre-signed URL for secure file download.
    
    Args:
        file_path: The full path of the file in storage
        expires_in: URL expiration time in seconds (default: 3600 = 1 hour)
        filename: Optional filename for Content-Disposition header
    
    Returns:
        Pre-signed download URL
    
    Raises:
        StorageError: If URL generation fails
    """
    try:
        client = get_storage_client()
        
        params = {
            'Bucket': settings.storage_bucket_name,
            'Key': file_path,
        }
        
        # Add Content-Disposition header if filename specified
        if filename:
            params['ResponseContentDisposition'] = f'attachment; filename="{filename}"'
        
        presigned_url = client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=expires_in
        )
        
        return presigned_url
        
    except Exception as e:
        logger.error(f"Failed to generate presigned download URL: {str(e)}")
        raise StorageError(f"Failed to generate download URL: {str(e)}")


def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """
    Get metadata for a file in storage.
    
    Args:
        file_path: The full path of the file in storage
    
    Returns:
        Dictionary with file metadata:
        - size: File size in bytes
        - last_modified: Last modification timestamp
        - content_type: MIME type
        - etag: File version identifier
    
    Raises:
        StorageError: If metadata retrieval fails
    """
    try:
        client = get_storage_client()
        
        response = client.head_object(
            Bucket=settings.storage_bucket_name,
            Key=file_path
        )
        
        return {
            "size": response.get('ContentLength', 0),
            "last_modified": response.get('LastModified'),
            "content_type": response.get('ContentType'),
            "etag": response.get('ETag', '').strip('"')
        }
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            raise StorageError(f"File not found: {file_path}")
        logger.error(f"Failed to get file metadata: {str(e)}")
        raise StorageError(f"Failed to get file metadata: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error getting file metadata: {str(e)}")
        raise StorageError(f"Failed to get file metadata: {str(e)}")


def list_files(folder: str = "uploads", max_keys: int = 1000) -> list[Dict[str, Any]]:
    """
    List files in a storage folder.
    
    Args:
        folder: The folder/prefix to list files from (default: "uploads")
        max_keys: Maximum number of files to return (default: 1000)
    
    Returns:
        List of file information dictionaries:
        - file_name: Full path of the file
        - file_id: ETag (version identifier)
        - size: File size in bytes
        - last_modified: Last modification timestamp
    
    Raises:
        StorageError: If listing fails
    """
    try:
        client = get_storage_client()
        
        # Ensure folder ends with /
        prefix = folder if folder.endswith('/') else f"{folder}/"
        
        response = client.list_objects_v2(
            Bucket=settings.storage_bucket_name,
            Prefix=prefix,
            MaxKeys=max_keys
        )
        
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'file_name': obj['Key'],
                'file_id': obj['ETag'].strip('"'),
                'size': obj['Size'],
                'last_modified': obj['LastModified']
            })
        
        return files
        
    except Exception as e:
        logger.error(f"Error listing files in folder '{folder}': {str(e)}")
        raise StorageError(f"Failed to list files: {str(e)}")


def delete_all_files(folder: str = "uploads") -> Dict[str, Any]:
    """
    Delete all files from a storage folder.
    
    Args:
        folder: The folder/prefix to delete files from (default: "uploads")
    
    Returns:
        Dictionary with deletion results:
        - success: Whether all deletions succeeded
        - deleted_count: Number of files deleted
        - failed_count: Number of files that failed to delete
    """
    try:
        files = list_files(folder)
        
        deleted_count = 0
        failed_count = 0
        
        for file_info in files:
            try:
                if delete_file(file_info['file_name']):
                    deleted_count += 1
                    logger.info(f"Deleted file: {file_info['file_name']}")
                else:
                    failed_count += 1
                    logger.error(f"Failed to delete file: {file_info['file_name']}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Error deleting file {file_info['file_name']}: {str(e)}")
        
        return {
            'success': failed_count == 0,
            'deleted_count': deleted_count,
            'failed_count': failed_count
        }
        
    except Exception as e:
        logger.error(f"Error deleting files from folder '{folder}': {str(e)}")
        return {
            'success': False,
            'deleted_count': 0,
            'failed_count': 0,
            'error': str(e)
        }
