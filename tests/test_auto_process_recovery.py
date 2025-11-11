import json
import os
import uuid
from typing import Dict

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from app.db.session import get_engine


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
def fake_b2_storage(monkeypatch):
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
            raise FileNotFoundError(f"File {file_path} not found in fake B2 bucket")
        return storage[file_path]

    def fake_delete(file_path: str) -> bool:
        storage.pop(file_path, None)
        return True

    # Patch the routers that call these helpers.
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_b2", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_b2", fake_delete)
    monkeypatch.setattr("app.integrations.b2.download_file_from_b2", fake_download)
    monkeypatch.setattr("app.main.download_file_from_b2", fake_download, raising=False)

    return storage


@pytest.mark.not_b2
def test_marketing_agency_auto_process_recovers_via_llm_plan(monkeypatch, fake_b2_storage):
    """
    Full-stack regression test that mirrors the frontend workflow:
    - Upload the US CSV, auto-process successfully.
    - Upload the Texas CSV, allow the backend to adapt the schema and merge automatically.
    - Ensure duplicate reporting survives the entire flow.
    """
    # Disable shortcut fixtures so the real analysis/LLM path is used.
    monkeypatch.setattr(
        "app.core.config.settings.enable_marketing_fixture_shortcuts",
        False,
        raising=False,
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        True,
        raising=False,
    )

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "marketing_agency_contacts" CASCADE'))
        for table in (
            "file_imports",
            "table_metadata",
            "import_history",
            "mapping_errors",
            "import_duplicates",
            "import_jobs",
            "uploaded_files",
        ):
            conn.execute(text(f'DELETE FROM "{table}"'))

    us_file_id = _upload_fixture("tests/csv/Marketing Agency - US.csv")
    tx_file_id = _upload_fixture("tests/csv/Marketing Agency - Texas.csv")

    auto_payload = {
        "analysis_mode": "auto_always",
        "conflict_resolution": "llm_decide",
        "max_iterations": "5",
    }

    response_us = client.post(
        "/analyze-file",
        data={"file_id": us_file_id, **auto_payload},
    )
    assert response_us.status_code == 200, response_us.text
    data_us = response_us.json()
    assert data_us["success"] is True
    assert "AUTO-EXECUTION COMPLETED" in data_us["llm_response"]

    response_tx = client.post(
        "/analyze-file",
        data={"file_id": tx_file_id, **auto_payload},
    )
    assert response_tx.status_code == 200, response_tx.text
    data_tx = response_tx.json()
    assert data_tx["success"] is True
    assert "AUTO-EXECUTION COMPLETED" in data_tx["llm_response"]

    # Verify duplicates were recorded for the Texas import
    with engine.connect() as conn:
        latest_import = conn.execute(
            text(
                """
                SELECT import_id, duplicates_found
                FROM import_history
                WHERE table_name = 'marketing_agency_contacts'
                ORDER BY import_timestamp DESC
                LIMIT 1
                """
            )
        ).mappings().one()

        assert latest_import["duplicates_found"] == 2

        duplicate_rows_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM import_duplicates
                WHERE import_id = :import_id
                """
            ),
            {"import_id": latest_import["import_id"]},
        ).scalar()

        assert duplicate_rows_count == 2

    # Confirm duplicate API mirrors the stored rows
    duplicates_response = client.get(
        f"/import-history/{latest_import['import_id']}/duplicates",
        params={"limit": 10},
    )
    assert duplicates_response.status_code == 200, duplicates_response.text
    duplicates_payload = duplicates_response.json()
    assert duplicates_payload["success"] is True
    assert duplicates_payload["total_count"] == 2
    assert len(duplicates_payload["duplicates"]) == 2

    print(
        "Duplicate rows via API:",
        json.dumps(duplicates_payload["duplicates"], indent=2),
    )
