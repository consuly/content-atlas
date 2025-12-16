import importlib
import io
import os
import time
import zipfile
import threading
from typing import Dict

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from app.db.session import get_session_local
from app.api.schemas.shared import (
    AnalyzeFileResponse,
    AnalysisMode,
    AutoExecutionResult,
    ConflictResolutionMode,
)
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


def _wait_for_job(job_id: str, timeout: float = 5.0) -> dict:
    """Poll import job until completion or timeout."""
    deadline = time.monotonic() + timeout
    last_payload = None
    while time.monotonic() < deadline:
        resp = client.get(f"/import-jobs/{job_id}")
        assert resp.status_code == 200, resp.text
        last_payload = resp.json()
        job = last_payload.get("job") or {}
        if job.get("status") in ("succeeded", "failed"):
            return job
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} did not complete in time; last payload={last_payload}")


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
            "mapping_chunk_status",
            "import_duplicates",
            "import_jobs",
            "uploaded_files",
            "file_imports",
        ):
            conn.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))
    yield


@pytest.fixture
def fake_storage_storage(monkeypatch):
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
            from app.integrations.storage import StorageDownloadError
            raise StorageDownloadError(f"File not found: {file_path}")
        return storage[file_path]

    def fake_delete(file_path: str) -> bool:
        storage.pop(file_path, None)
        return True

    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_storage", fake_delete)
    monkeypatch.setattr("app.integrations.storage.upload_file", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr("app.integrations.storage_multipart.download_file", fake_download)
    monkeypatch.setattr("app.api.routers.analysis.routes._download_file_from_storage", fake_download)
    return storage


@pytest.mark.not_b2
def test_auto_process_archive_happy_path(fake_storage_storage):
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
    job_id = payload["job_id"]
    assert job_id

    job = _wait_for_job(job_id)
    assert job["status"] == "succeeded"
    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 2
    assert metadata["failed_files"] == 0
    assert metadata["skipped_files"] == 0
    assert len(metadata["results"]) == 2
    assert all(result["status"] == "processed" for result in metadata["results"])
    assert all(result["uploaded_file_id"] for result in metadata["results"])


@pytest.mark.not_b2
def test_auto_process_archive_continues_after_failures(monkeypatch, fake_storage_storage):
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

    # Patch the analyzer function that _get_analyze_file_for_import looks for
    monkeypatch.setattr("app.main.analyze_file_for_import", fake_analyze)

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
    assert payload["success"] is True
    job_id = payload["job_id"]
    assert job_id

    job = _wait_for_job(job_id)
    assert job["status"] == "failed"
    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 0
    assert metadata["failed_files"] == 1
    assert metadata["skipped_files"] == 1


@pytest.mark.not_b2
def test_auto_process_archive_forces_target_table(monkeypatch, fake_storage_storage, tmp_path):
    forced_table = "forced_archive_table"

    def fake_analyze(**_kwargs):
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "llm_pick",
                "column_mapping": {"name": "name"},
                "unique_columns": ["name"],
                "has_header": True,
                "expected_column_types": {"name": "TEXT"},
            },
        }

    # Patch the analyzer function that _get_analyze_file_for_import looks for
    monkeypatch.setattr("app.main.analyze_file_for_import", fake_analyze)

    captured = {}

    def fake_execute(file_content, file_name, all_records, llm_decision):
        captured["llm_decision"] = llm_decision
        assert llm_decision["target_table"] == forced_table
        assert llm_decision["strategy"] == "ADAPT_DATA"
        return {
            "success": True,
            "strategy_executed": llm_decision["strategy"],
            "table_name": llm_decision["target_table"],
            "records_processed": len(all_records),
            "duplicates_skipped": 0,
        }
    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", fake_execute
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    csv_path = tmp_path / "simple.csv"
    csv_path.write_text("name\nalpha\nbeta\n", encoding="utf-8")
    zip_bytes = _build_zip({"simple.csv": str(csv_path)})
    archive_id = _upload_zip(zip_bytes, filename="forced.zip")

    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "target_table_name": forced_table,
            "target_table_mode": "existing",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    job_id = payload["job_id"]

    job = _wait_for_job(job_id)
    assert job["status"] == "succeeded"
    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 1
    assert metadata["failed_files"] == 0
    assert metadata["results"][0]["table_name"] == forced_table
    assert captured["llm_decision"]["target_table"] == forced_table
    assert job["metadata"]["forced_table_name"] == forced_table
    assert len(metadata["results"]) == 1
    assert metadata["skipped_files"] == 0


@pytest.mark.not_b2
def test_auto_process_archive_reuses_cached_decision(monkeypatch, fake_storage_storage):
    analysis_calls = {"count": 0}
    execution_calls = {"count": 0}

    def fake_analyze(**_kwargs):
        analysis_calls["count"] += 1
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "cached_reuse",
                "column_mapping": {"name": "name"},
            },
        }

    # Patch the analyzer function that _get_analyze_file_for_import looks for
    monkeypatch.setattr("app.main.analyze_file_for_import", fake_analyze)

    def fake_execute(**_kwargs):
        execution_calls["count"] += 1
        return {
            "success": True,
            "strategy_attempted": "NEW_TABLE",
            "strategy_executed": "NEW_TABLE",
            "target_table": "cached_reuse",
            "table_name": "cached_reuse",
            "records_processed": 2,
            "duplicates_skipped": 0,
        }

    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", fake_execute
    )
    monkeypatch.setattr(
        "app.main.execute_llm_import_decision", fake_execute, raising=False
    )

    csv_bytes = "name\nalpha\nbeta\n".encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("first.csv", csv_bytes)
        zf.writestr("second.csv", csv_bytes)

    archive_id = _upload_zip(buffer.getvalue(), filename="cached.zip")
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
    job_id = payload["job_id"]
    job = _wait_for_job(job_id)
    assert job["status"] == "succeeded"

    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 2
    assert metadata["failed_files"] == 0
    assert analysis_calls["count"] == 1  # second entry reused cached decision
    assert execution_calls["count"] == 2  # still executes both files


@pytest.mark.not_b2
def test_auto_process_archive_recovers_when_cached_plan_missing(monkeypatch, fake_storage_storage):
    """If a cached fingerprint lacks a decision, the worker should still analyze and return a result."""
    analysis_module = importlib.import_module("app.api.routers.analysis")
    cached_event = threading.Event()
    cached_event.set()
    fingerprint_cache = {"shared-fp": {"event": cached_event}}
    fingerprint_lock = threading.Lock()
    analyze_calls = {"count": 0}

    async def fake_analyze_file_endpoint(**_kwargs):
        analyze_calls["count"] += 1
        return AnalyzeFileResponse(
            success=True,
            llm_response="ok",
            llm_decision={
                "strategy": "NEW_TABLE",
                "target_table": "clients_list",
                "column_mapping": {"name": "name"},
            },
            auto_execution_result=AutoExecutionResult(
                success=True,
                strategy_executed="NEW_TABLE",
                table_name="clients_list",
                records_processed=1,
                duplicates_skipped=0,
                import_id="import-1",
            ),
            can_auto_execute=True,
        )

    monkeypatch.setattr("app.api.routers.analysis.routes.analyze_file_endpoint", fake_analyze_file_endpoint)
    monkeypatch.setattr(
        "app.api.routers.analysis.routes._build_structure_fingerprint", lambda *_args, **_kwargs: "shared-fp"
    )

    entry_bytes = "name\nalpha\n".encode()
    session_local = get_session_local()
    db_session = session_local()
    try:
        result = analysis_module.routes._process_entry_bytes(
            entry_bytes=entry_bytes,
            archive_path="shared.csv",
            entry_name="shared.csv",
            stored_file_name="shared.csv",
            archive_folder="uploads/test",
            fingerprint_cache=fingerprint_cache,
            fingerprint_lock=fingerprint_lock,
            analysis_mode=AnalysisMode.AUTO_ALWAYS,
            conflict_resolution=ConflictResolutionMode.LLM_DECIDE,
            auto_execute_confidence_threshold=0.9,
            max_iterations=3,
            forced_table_name="clients_list",
            forced_table_mode="existing",
            llm_instruction=None,
            db_session=db_session,
        )
    finally:
        db_session.close()

    assert analyze_calls["count"] == 1  # fallback analysis ran instead of returning an empty summary
    assert result.status == "processed"
    assert result.message
    assert fingerprint_cache["shared-fp"].get("llm_decision")


@pytest.mark.not_b2
def test_auto_process_archive_resume_failed_entries(monkeypatch, fake_storage_storage):
    execution_calls = {"count": 0}

    def fake_analyze(**_kwargs):
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "archive_resume",
                "column_mapping": {"name": "name"},
            },
        }

    # Patch the analyzer function that _get_analyze_file_for_import looks for
    monkeypatch.setattr("app.main.analyze_file_for_import", fake_analyze)

    def fake_execute(**_kwargs):
        execution_calls["count"] += 1
        if execution_calls["count"] == 1:
            return {
                "success": False,
                "error": "transient",
                "strategy_attempted": "NEW_TABLE",
                "target_table": "archive_resume",
            }
        return {
            "success": True,
            "strategy_attempted": "NEW_TABLE",
            "strategy_executed": "NEW_TABLE",
            "target_table": "archive_resume",
            "table_name": "archive_resume",
            "records_processed": 2,
            "duplicates_skipped": 0,
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
        zf.writestr("first.csv", csv_bytes)
        zf.writestr("second.csv", csv_bytes)

    archive_id = _upload_zip(buffer.getvalue(), filename="resume.zip")
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
    first_job_id = response.json()["job_id"]

    first_job = _wait_for_job(first_job_id)
    assert first_job["status"] == "failed"
    first_metadata = first_job["result_metadata"]
    assert first_metadata["processed_files"] == 1
    assert first_metadata["failed_files"] == 1

    resume_response = client.post(
        "/auto-process-archive/resume",
        data={
            "file_id": archive_id,
            "from_job_id": first_job_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
        },
    )
    assert resume_response.status_code == 200, resume_response.text
    second_job_id = resume_response.json()["job_id"]
    assert second_job_id

    resumed_job = _wait_for_job(second_job_id)
    assert resumed_job["status"] == "succeeded"
    resumed_metadata = resumed_job["result_metadata"]
    assert resumed_metadata["processed_files"] == 2
    assert resumed_metadata["failed_files"] == 0
    assert len(resumed_metadata["results"]) == 2
    assert all(result["status"] == "processed" for result in resumed_metadata["results"])


@pytest.mark.not_b2
def test_auto_process_archive_resume_all(monkeypatch, fake_storage_storage):
    """
    When resume_failed_entries_only=False, the endpoint should reprocess the entire archive
    regardless of prior success/failure state.
    """
    execution_calls = {"count": 0}

    def fake_analyze(**_kwargs):
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "archive_resume_all",
                "column_mapping": {"name": "name"},
            },
        }

    # Patch the analyzer function that _get_analyze_file_for_import looks for
    monkeypatch.setattr("app.main.analyze_file_for_import", fake_analyze)

    def fake_execute(**_kwargs):
        execution_calls["count"] += 1
        if execution_calls["count"] == 1:
            return {
                "success": False,
                "error": "transient",
                "strategy_attempted": "NEW_TABLE",
                "target_table": "archive_resume_all",
            }
        return {
            "success": True,
            "strategy_attempted": "NEW_TABLE",
            "strategy_executed": "NEW_TABLE",
            "target_table": "archive_resume_all",
            "table_name": "archive_resume_all",
            "records_processed": 2,
            "duplicates_skipped": 0,
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
        zf.writestr("first.csv", csv_bytes)
        zf.writestr("second.csv", csv_bytes)

    archive_id = _upload_zip(buffer.getvalue(), filename="resume-all.zip")
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
    first_job_id = response.json()["job_id"]

    first_job = _wait_for_job(first_job_id)
    assert first_job["status"] == "failed"
    first_metadata = first_job["result_metadata"]
    assert first_metadata["processed_files"] == 1
    assert first_metadata["failed_files"] == 1

    resume_response = client.post(
        "/auto-process-archive/resume",
        data={
            "file_id": archive_id,
            "from_job_id": first_job_id,
            "resume_failed_entries_only": "false",
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
        },
    )
    assert resume_response.status_code == 200, resume_response.text
    second_job_id = resume_response.json()["job_id"]
    assert second_job_id

    resumed_job = _wait_for_job(second_job_id)
    assert resumed_job["status"] == "succeeded"
    resumed_metadata = resumed_job["result_metadata"]
    assert resumed_metadata["processed_files"] == 2
    assert resumed_metadata["failed_files"] == 0
    assert len(resumed_metadata["results"]) == 2
    assert all(result["status"] == "processed" for result in resumed_metadata["results"])
    assert execution_calls["count"] == 4  # two executions on first run, two on full reprocess
