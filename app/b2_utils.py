import io
from b2sdk.v2 import B2Api, InMemoryAccountInfo
from .config import settings


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

    # Download to memory
    download_dest = io.BytesIO()
    bucket.download_file_by_name(file_name, download_dest)

    return download_dest.getvalue()
