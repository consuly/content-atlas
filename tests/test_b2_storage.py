import uuid
from pathlib import Path

import pytest

from app.core.config import settings
from app.integrations.b2 import (
    delete_file_from_b2,
    download_file_from_b2,
    upload_file_to_b2,
)


@pytest.mark.integration
def test_b2_upload_and_download_roundtrip():
    """
    Perform a simple upload/download cycle against Backblaze B2.

    This test skips automatically if B2 credentials are not configured in the environment.
    """
    if not all(
        [
            settings.b2_application_key_id,
            settings.b2_application_key,
            settings.b2_bucket_name,
        ]
    ):
        pytest.skip("B2 credentials not configured; skipping live storage test")

    sample_file = Path("tests/test_data_small.csv")
    data = sample_file.read_bytes()

    unique_name = f"test-{uuid.uuid4().hex}.csv"
    folder = "tests"

    upload_result = upload_file_to_b2(data, unique_name, folder=folder)
    file_path = upload_result["file_path"]

    try:
        downloaded = download_file_from_b2(file_path)
        assert downloaded == data, "Downloaded content did not match uploaded content"
    finally:
        delete_file_from_b2(file_path)
