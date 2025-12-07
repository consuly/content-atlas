import uuid
from pathlib import Path

import pytest

from app.core.config import settings
from app.integrations.storage import (
    delete_file,
    download_file,
    upload_file,
)


@pytest.mark.integration
def test_storage_upload_and_download_roundtrip():
    """
    Perform a simple upload/download cycle against S3-compatible storage.

    This test skips automatically if storage credentials are not configured in the environment.
    """
    if not all(
        [
            settings.storage_access_key_id,
            settings.storage_secret_access_key,
            settings.storage_bucket_name,
        ]
    ):
        pytest.skip("Storage credentials not configured; skipping live storage test")

    sample_file = Path("tests/test_data_small.csv")
    data = sample_file.read_bytes()

    unique_name = f"test-{uuid.uuid4().hex}.csv"
    folder = "tests"

    upload_result = upload_file(data, unique_name, folder=folder)
    file_path = upload_result["file_path"]

    try:
        downloaded = download_file(file_path)
        assert downloaded == data, "Downloaded content did not match uploaded content"
    finally:
        delete_file(file_path)
