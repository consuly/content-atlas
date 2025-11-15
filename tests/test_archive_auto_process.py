import importlib
import io
import os
import zipfile
from typing import Dict

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready


client = TestClient(app)


def _build_zip(file_map: Dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for archive_name, source_path in file_map.items():
            with open(source_path, "rb") as handle:
                zf.writestr(archive_name, handle.read())
    return buffer.getvalue()


def _upload_zip(zip_bytes: bytes, filename: str = "batch.zip") -> str:
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": (filename, io.BytesIO(zip_bytes), "application/zip")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    print("ARCHIVE HAPPY PAYLOAD", payload)
    assert payload["success"] is True
    return payload["files"][0]["id"]


@pytest.fixture(autouse=True)
def reset_tables():
    engine = ensure_system_tables_ready()
    with engine.begin() as conn:
        for table in (
            "marketing_agency_contacts",
            "marketing_agency_leads_us",
        ):
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        for table in (
            "import_history",
            "mapping_errors",
            "import_duplicates",
            "import_jobs",
            "uploaded_files",
            "file_imports",
        ):
            conn.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))
    yield


@pytest.fixture
def fake_b2_storage(monkeypatch):
    storage: Dict[str, bytes] = {}

    def fake_upload(file_content: bytes, file_name: str, folder: str = "uploads"):
        path = f"{folder}/{file_name}"
        storage[path] = bytes(file_content)
        return {
            "file_id": file_name,
            "file_name": file_name,
            "file_path": path,
            "size": len(file_content),
        }

    def fake_download(file_path: str) -> bytes:
        if file_path not in storage:
            raise FileNotFoundError(file_path)
        return storage[file_path]

    def fake_delete(file_path: str) -> bool:
        storage.pop(file_path, None)
        return True

    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_b2", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_b2", fake_delete)
    monkeypatch.setattr("app.integrations.b2.upload_file_to_b2", fake_upload)
    monkeypatch.setattr("app.integrations.b2.download_file_from_b2", fake_download)
    monkeypatch.setattr("app.main.download_file_from_b2", fake_download, raising=False)
    return storage


@pytest.mark.not_b2
def test_auto_process_archive_happy_path(fake_b2_storage):
    entries = {
        "Marketing Agency - US.csv": os.path.join(
            "tests", "csv", "Marketing Agency - US.csv"
        ),
        "Marketing Agency - Texas.csv": os.path.join(
            "tests", "csv", "Marketing Agency - Texas.csv"
        ),
    }
    zip_bytes = _build_zip(entries)
    archive_id = _upload_zip(zip_bytes, filename="marketing.zip")

    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    assert payload["processed_files"] == 2
    assert payload["failed_files"] == 0
    assert payload["skipped_files"] == 0
    assert len(payload["results"]) == 2
    assert all(result["status"] == "processed" for result in payload["results"])
    assert all(result["uploaded_file_id"] for result in payload["results"])


@pytest.mark.not_b2
def test_auto_process_archive_continues_after_failures(monkeypatch, fake_b2_storage):
    analysis_module = importlib.import_module("app.api.routers.analysis")

    def fake_analyze(**_kwargs):
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "archive_auto_failures",
                "column_mapping": {"name": "name"},
            },
        }

    monkeypatch.setattr(
        analysis_module, "_get_analyze_file_for_import", lambda: fake_analyze
    )

    def fake_execute(**_kwargs):
        return {
            "success": False,
            "error": "boom",
            "strategy_attempted": "NEW_TABLE",
            "target_table": "archive_auto_failures",
        }

    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", fake_execute
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    csv_bytes = "name\nalpha\nbeta\n".encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("simple.csv", csv_bytes)
        zf.writestr("notes.txt", b"skip me")

    archive_id = _upload_zip(buffer.getvalue(), filename="mixed.zip")
    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is False
    assert payload["processed_files"] == 0
    assert payload["failed_files"] == 1
    assert payload["skipped_files"] == 1
    assert len(payload["results"]) == 2
    failed_entry = next(result for result in payload["results"] if result["status"] == "failed")
    assert failed_entry["message"]
    skipped_entry = next(result for result in payload["results"] if result["status"] == "skipped")
    assert skipped_entry["message"] == "Unsupported file type"
