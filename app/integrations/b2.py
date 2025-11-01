import io
from b2sdk.v2 import B2Api, InMemoryAccountInfo
from app.core.config import settings
from typing import Dict, Any


def get_b2_api():
    """Initialize and return B2 API client."""
    if not all([settings.b2_application_key_id, settings.b2_application_key, settings.b2_bucket_name]):
        raise ValueError("B2 configuration is incomplete. Please set B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, and B2_BUCKET_NAME in your environment.")

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", settings.b2_application_key_id, settings.b2_application_key)
    return b2_api


def download_file_from_b2(file_name: str) -> bytes:
    """
    Download a file from Backblaze B2 bucket.

    Args:
        file_name: The name/key of the file in the B2 bucket

    Returns:
        File content as bytes
    """
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)

    # Download file - bucket.download_file_by_name returns a DownloadedFile object
    downloaded_file = bucket.download_file_by_name(file_name)

    # Get the content from the response
    return downloaded_file.response.content


def upload_file_to_b2(file_content: bytes, file_name: str, folder: str = "uploads") -> dict:
    """
    Upload a file to Backblaze B2 bucket.

    Args:
        file_content: The file content as bytes
        file_name: The name for the file
        folder: The folder/prefix to store the file in (default: "uploads")

    Returns:
        Dictionary with upload details (file_id, file_name, file_path)
    """
    import traceback
    
    try:
        b2_api = get_b2_api()
        
        bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)

        # Construct the full file path with folder
        file_path = f"{folder}/{file_name}"

        # Upload file
        file_info = bucket.upload_bytes(
            data_bytes=file_content,
            file_name=file_path
        )

        result = {
            "file_id": file_info.id_,
            "file_name": file_name,
            "file_path": file_path,
            "size": len(file_content)
        }
        return result
        
    except Exception as e:
        print(f"\n[B2_UTILS ERROR] Upload failed!")
        print(f"[B2_UTILS ERROR] Error type: {type(e).__name__}")
        print(f"[B2_UTILS ERROR] Error message: {str(e)}")
        print(f"[B2_UTILS ERROR] Traceback:")
        print(traceback.format_exc())
        raise


def check_file_exists_in_b2(file_name: str, folder: str = "uploads") -> bool:
    """
    Check if a file exists in Backblaze B2 bucket.

    Args:
        file_name: The name of the file to check
        folder: The folder/prefix where the file should be (default: "uploads")

    Returns:
        True if file exists, False otherwise
    """
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)

    file_path = f"{folder}/{file_name}"

    try:
        # Try to get file info
        bucket.get_file_info_by_name(file_path)
        return True
    except Exception:
        return False


def delete_file_from_b2(file_path: str) -> bool:
    """
    Delete a file from Backblaze B2 bucket.

    Args:
        file_path: The full path of the file in B2 (e.g., "uploads/file.csv")

    Returns:
        True if deletion was successful, False otherwise
    """
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)

    try:
        # Get file version info
        file_version = bucket.get_file_info_by_name(file_path)
        # Delete the file version
        b2_api.delete_file_version(file_version.id_, file_version.file_name)
        return True
    except Exception as e:
        print(f"Error deleting file from B2: {e}")
        return False


def generate_upload_authorization(file_name: str, folder: str = "uploads") -> Dict[str, Any]:
    """
    Generate upload authorization for direct browser-to-B2 uploads.
    
    This function provides the necessary credentials and URLs for the frontend
    to upload files directly to B2, bypassing the backend server for better performance.
    
    Args:
        file_name: The name of the file to upload
        folder: The folder/prefix to store the file in (default: "uploads")
    
    Returns:
        Dictionary containing:
        - upload_url: The URL to upload to
        - authorization_token: The auth token for the upload
        - file_path: The full path where the file will be stored
        - bucket_id: The B2 bucket ID
    """
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)
    
    # Get upload URL and authorization token
    upload_url_response = bucket.get_upload_url()
    
    # Construct the full file path
    file_path = f"{folder}/{file_name}"
    
    return {
        "upload_url": upload_url_response.upload_url,
        "authorization_token": upload_url_response.auth_token,
        "file_path": file_path,
        "bucket_id": bucket.id_,
        "api_url": b2_api.account_info.get_api_url(),
        "download_url": b2_api.account_info.get_download_url()
    }


def get_large_file_upload_part_url(file_id: str) -> Dict[str, Any]:
    """
    Get upload URL for a large file part.
    
    For files >100MB, B2 recommends using the Large File API which allows
    uploading in parts (minimum 5MB per part, except the last part).
    
    Args:
        file_id: The B2 large file ID
    
    Returns:
        Dictionary containing upload URL and authorization token for the part
    """
    b2_api = get_b2_api()
    
    # Get upload part URL
    response = b2_api.get_upload_part_url(file_id)
    
    return {
        "upload_url": response.upload_url,
        "authorization_token": response.auth_token
    }


def start_large_file_upload(file_name: str, folder: str = "uploads") -> Dict[str, Any]:
    """
    Start a large file upload session.
    
    This initializes a large file upload in B2, which is required for files >100MB
    or when you want to use parallel part uploads for better performance.
    
    Args:
        file_name: The name of the file to upload
        folder: The folder/prefix to store the file in (default: "uploads")
    
    Returns:
        Dictionary containing:
        - file_id: The B2 large file ID
        - file_path: The full path where the file will be stored
    """
    b2_api = get_b2_api()
    bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)
    
    # Construct the full file path
    file_path = f"{folder}/{file_name}"
    
    # Start large file upload
    large_file = bucket.start_large_file(
        file_name=file_path,
        content_type=None,  # Will be auto-detected
        file_info={}
    )
    
    return {
        "file_id": large_file.file_id,
        "file_path": file_path
    }


def finish_large_file_upload(file_id: str, part_sha1_array: list) -> Dict[str, Any]:
    """
    Finish a large file upload after all parts have been uploaded.
    
    Args:
        file_id: The B2 large file ID
        part_sha1_array: Array of SHA1 hashes for each part in order
    
    Returns:
        Dictionary with file information
    """
    b2_api = get_b2_api()
    
    # Finish the large file upload
    file_info = b2_api.finish_large_file(file_id, part_sha1_array)
    
    return {
        "file_id": file_info.id_,
        "file_name": file_info.file_name,
        "size": file_info.size,
        "content_type": file_info.content_type
    }
