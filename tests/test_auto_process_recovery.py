import json
import os
import uuid
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready


client = TestClient(app)


def _upload_fixture(csv_path: str) -> str:
    """Upload a CSV via the public endpoint and return the uploaded file ID."""
    with open(csv_path, "rb") as handle:
        response = client.post(
            "/upload-to-b2",
            data={"allow_duplicate": "true"},
            files={
                "file": (
                    os.path.basename(csv_path),
                    handle,
                    "text/csv",
                )
            },
        )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    uploaded = payload["files"][0]
    return uploaded["id"]


@pytest.fixture(autouse=True)
def fake_storage_storage(monkeypatch):
    """
    Patch B2 helpers so uploads store bytes in-memory and downloads read from that store,
    mirroring the real controller flow used by the frontend.
    """
    storage: Dict[str, bytes] = {}

    def fake_upload(file_content: bytes, file_name: str, folder: str = "uploads"):
        path = f"{folder}/{file_name}"
        storage[path] = bytes(file_content)
        return {
            "file_id": str(uuid.uuid4()),
            "file_name": file_name,
            "file_path": path,
            "size": len(file_content),
        }

    def fake_download(file_path: str) -> bytes:
        if file_path not in storage:
            from app.integrations.storage import StorageDownloadError
            raise StorageDownloadError(f"File not found: {file_path}")
        return storage[file_path]

    def fake_delete(file_path: str) -> bool:
        storage.pop(file_path, None)
        return True

    # Patch the routers that call these helpers.
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_storage", fake_delete)
    monkeypatch.setattr("app.integrations.storage.upload_file", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr("app.integrations.storage_multipart.download_file", fake_download)
    monkeypatch.setattr("app.api.routers.analysis.routes._download_file_from_storage", fake_download)

    return storage
