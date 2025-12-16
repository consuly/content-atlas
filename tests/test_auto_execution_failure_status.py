import os

import pytest
from fastapi.testclient import TestClient

from app.api.routers import analysis as analysis_module
from app.main import app
from app.core import config


client = TestClient(app)


@pytest.fixture
def fake_storage_storage(monkeypatch):
    storage = {}

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

    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_storage", fake_delete)
    monkeypatch.setattr("app.integrations.storage.upload_file", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr("app.main.download_file_from_storage", fake_download, raising=False)
    return storage


@pytest.fixture
def in_memory_state(monkeypatch):
    uploads = {}
    jobs = {}

    def insert_uploaded_file(
        file_name: str,
        b2_file_id: str,
        b2_file_path: str,
        file_size: int,
        content_type: str = None,
        user_id: str = None,
        file_hash: str = None,
    ):
        file_id = str(len(uploads) + 1)
        record = {
            "id": file_id,
            "file_name": file_name,
            "b2_file_id": b2_file_id,
            "b2_file_path": b2_file_path,
            "file_size": file_size,
            "content_type": content_type,
            "upload_date": None,
            "status": "uploaded",
            "mapped_table_name": None,
            "mapped_rows": None,
            "error_message": None,
            "active_job_id": None,
            "active_job_status": None,
            "active_job_stage": None,
            "active_job_progress": None,
            "active_job_started_at": None,
        }
        uploads[file_id] = record
        return record

    def get_uploaded_file_by_id(file_id: str):
        return uploads.get(file_id)

    def get_uploaded_file_by_name(file_name: str):
        for record in uploads.values():
            if record["file_name"] == file_name:
                return record
        return None

    def update_file_status(file_id: str, status: str, **kwargs):
        if file_id in uploads:
            uploads[file_id]["status"] = status
            uploads[file_id]["error_message"] = kwargs.get("error_message")
            uploads[file_id]["mapped_table_name"] = kwargs.get("mapped_table_name")
            uploads[file_id]["mapped_rows"] = kwargs.get("mapped_rows")
            uploads[file_id]["active_job_id"] = kwargs.get("expected_active_job_id") or uploads[file_id].get("active_job_id")
            uploads[file_id]["active_job_status"] = status
            uploads[file_id]["active_job_stage"] = kwargs.get("stage") or uploads[file_id].get("active_job_stage")
            uploads[file_id]["active_job_progress"] = kwargs.get("progress") or uploads[file_id].get("active_job_progress")

    def create_import_job(
        file_id: str,
        trigger_source: str,
        analysis_mode: str,
        conflict_mode: str,
        metadata: dict | None = None,
    ):
        job_id = str(len(jobs) + 1)
        job = {
            "id": job_id,
            "file_id": file_id,
            "status": "running",
            "stage": "queued",
            "progress": 0,
            "trigger_source": trigger_source,
            "analysis_mode": analysis_mode,
            "conflict_mode": conflict_mode,
            "metadata": metadata or {},
            "result_metadata": None,
        }
        jobs[job_id] = job
        uploads[file_id]["active_job_id"] = job_id
        return job

    def update_import_job(job_id: str, **kwargs):
        job = jobs.get(job_id)
        if not job:
            return
        for key in ("status", "stage", "progress", "error_message"):
            if kwargs.get(key) is not None:
                job[key] = kwargs[key]
        if "metadata" in kwargs and kwargs["metadata"]:
            merged = dict(job.get("metadata") or {})
            merged.update(kwargs["metadata"])
            job["metadata"] = merged

    def complete_import_job(
        job_id: str,
        success: bool,
        result_metadata: dict | None = None,
        error_message: str | None = None,
        **_kwargs,
    ):
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "succeeded" if success else "failed"
        job["stage"] = "completed"
        job["error_message"] = error_message
        job["result_metadata"] = result_metadata

    for target in (
        "app.domain.uploads.uploaded_files.insert_uploaded_file",
        "app.api.routers.uploads.insert_uploaded_file",
    ):
        monkeypatch.setattr(target, insert_uploaded_file)
    for target in (
        "app.domain.uploads.uploaded_files.get_uploaded_file_by_id",
        "app.api.routers.uploads.get_uploaded_file_by_id",
        "app.api.routers.analysis.routes.get_uploaded_file_by_id",
    ):
        monkeypatch.setattr(target, get_uploaded_file_by_id)
    monkeypatch.setattr("app.api.routers.uploads.get_uploaded_file_by_name", get_uploaded_file_by_name)
    for target in (
        "app.domain.uploads.uploaded_files.update_file_status",
        "app.api.routers.analysis.routes.update_file_status",
    ):
        monkeypatch.setattr(target, update_file_status)

    for target in (
        "app.domain.imports.jobs.create_import_job",
        "app.api.routers.analysis.routes.create_import_job",
    ):
        monkeypatch.setattr(target, create_import_job)
    for target in (
        "app.domain.imports.jobs.update_import_job",
        "app.api.routers.analysis.routes.update_import_job",
    ):
        monkeypatch.setattr(target, update_import_job)
    for target in (
        "app.domain.imports.jobs.complete_import_job",
        "app.api.routers.analysis.routes.complete_import_job",
    ):
        monkeypatch.setattr(target, complete_import_job)

    class DummySession:
        def close(self):
            pass

    monkeypatch.setattr("app.api.routers.analysis.routes.get_session_local", lambda: lambda: DummySession())

    return {"uploads": uploads, "jobs": jobs}


def test_auto_execute_failure_marks_file_failed(
    monkeypatch,
    fake_storage_storage,
    in_memory_state,
):
    # Disable auto-retry to keep control of the failure path
    monkeypatch.setattr(config.settings, "enable_auto_retry_failed_imports", False)

    # Seed a workbook in fake B2 storage and uploaded_files table
    workbook_bytes = open(os.path.join("tests", "xlsx", "test-2-tabs.xlsx"), "rb").read()
    b2_path = "uploads/auto-fail.xlsx"
    fake_storage_storage[b2_path] = workbook_bytes

    # Create the file record using the in-memory state
    file_record = in_memory_state["uploads"]["550e8400-e29b-41d4-a716-446655440000"] = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "file_name": "auto-fail.xlsx",
        "b2_file_id": "fake-id",
        "b2_file_path": b2_path,
        "file_size": len(workbook_bytes),
        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "upload_date": None,
        "status": "uploaded",
        "mapped_table_name": None,
        "mapped_rows": None,
        "error_message": None,
        "active_job_id": None,
        "active_job_status": None,
        "active_job_stage": None,
        "active_job_progress": None,
        "active_job_started_at": None,
    }
    file_id = file_record["id"]

    # Force deterministic analysis + failing execution
    monkeypatch.setattr(
        analysis_module,
        "_get_analyze_file_for_import",
        lambda: lambda **_kwargs: {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "clients",
                "column_mapping": {"first_name": "first_name"},
                "unique_columns": ["first_name"],
                "has_header": True,
            },
        },
    )

    def _fail_execute(**_kwargs):
        return {
            "success": False,
            "error": "boom",
            "strategy_attempted": "NEW_TABLE",
            "target_table": "clients",
        }

    # Patch both the module-level function and where it's imported in routes
    monkeypatch.setattr(
        analysis_module,
        "_get_execute_llm_import_decision",
        lambda: _fail_execute,
    )
    monkeypatch.setattr(
        "app.api.routers.analysis.routes._get_execute_llm_import_decision",
        lambda: _fail_execute,
    )

    response = client.post(
        "/analyze-file",
        data={
            "file_id": file_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "1",
        },
    )

    assert response.status_code == 200, response.text
    assert in_memory_state["uploads"][file_id]["status"] == "failed"
    assert in_memory_state["uploads"][file_id]["error_message"] == "boom"
