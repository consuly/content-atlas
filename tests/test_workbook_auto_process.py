import io
import os
import time
import uuid
from typing import Dict, Optional

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def _upload_workbook(bytes_data: bytes, filename: str) -> str:
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": (filename, io.BytesIO(bytes_data), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
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
    uploads: Dict[str, Dict] = {}
    jobs: Dict[str, Dict] = {}

    def insert_uploaded_file(
        file_name: str,
        b2_file_id: str,
        b2_file_path: str,
        file_size: int,
        content_type: str = None,
        user_id: str = None,
        file_hash: str = None,
        parent_file_id: str = None,
    ) -> Dict:
        file_id = str(uuid.uuid4())
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

    def get_uploaded_file_by_id(file_id: str) -> Optional[Dict]:
        return uploads.get(file_id)

    def get_uploaded_file_by_name(file_name: str) -> Optional[Dict]:
        for record in uploads.values():
            if record["file_name"] == file_name:
                return record
        return None

    def update_file_status(file_id: str, status: str, **kwargs) -> None:
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
        metadata: Optional[Dict] = None,
    ) -> Dict:
        job_id = str(uuid.uuid4())
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

    def update_import_job(job_id: str, **kwargs) -> None:
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

    def complete_import_job(job_id: str, success: bool, result_metadata: Optional[Dict] = None, error_message: Optional[str] = None, **_kwargs):
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "succeeded" if success else "failed"
        job["stage"] = "completed"
        job["error_message"] = error_message
        job["result_metadata"] = result_metadata

    def get_import_job(job_id: str) -> Optional[Dict]:
        return jobs.get(job_id)

    def list_import_jobs(file_id: Optional[str] = None, limit: int = 50, offset: int = 0):
        filtered = [job for job in jobs.values() if not file_id or job["file_id"] == file_id]
        return filtered[offset : offset + limit], len(filtered)

    for target in (
        "app.domain.uploads.uploaded_files.insert_uploaded_file",
        "app.api.routers.analysis.routes.insert_uploaded_file",
        "app.api.routers.uploads.insert_uploaded_file",
    ):
        monkeypatch.setattr(target, insert_uploaded_file)
    for target in (
        "app.domain.uploads.uploaded_files.get_uploaded_file_by_id",
        "app.api.routers.analysis.routes.get_uploaded_file_by_id",
        "app.api.routers.uploads.get_uploaded_file_by_id",
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
    for target in (
        "app.domain.imports.jobs.get_import_job",
        "app.api.routers.analysis.routes.get_import_job",
        "app.api.routers.jobs.get_import_job",
    ):
        monkeypatch.setattr(target, get_import_job)
    for target in (
        "app.domain.imports.jobs.list_import_jobs",
        "app.api.routers.jobs.list_import_jobs",
    ):
        monkeypatch.setattr(target, list_import_jobs)

    class DummySession:
        def close(self):
            pass

    monkeypatch.setattr("app.api.routers.analysis.routes.get_session_local", lambda: lambda: DummySession())

    return {"uploads": uploads, "jobs": jobs}


def _fake_analyze_response(target_table: str) -> dict:
    """Return a deterministic LLM decision for workbook sheets."""
    return {
        "success": True,
        "response": "ok",
        "iterations_used": 1,
        "llm_decision": {
            "strategy": "NEW_TABLE",
            "target_table": target_table,
            "column_mapping": {
                "first_name": "first_name",
                "last_name": "last_name",
                "email": "email",
                "sales oct": "sales_oct",
                "sales nov": "sales_nov",
            },
            "unique_columns": ["email"],
            "has_header": True,
        },
    }


def _upsert_clients(store: Dict[str, Dict], records):
    """Insert or update records by email, filling whichever sales column is present."""
    for record in records:
        email = record["email"]
        existing = store.get(email)
        if existing:
            existing["sales_oct"] = existing.get("sales_oct") or record.get("sales_oct")
            existing["sales_nov"] = existing.get("sales_nov") or record.get("sales_nov")
            existing["first_name"] = record.get("first_name")
            existing["last_name"] = record.get("last_name")
        else:
            store[email] = {
                "first_name": record.get("first_name"),
                "last_name": record.get("last_name"),
                "email": email,
                "sales_oct": record.get("sales_oct"),
                "sales_nov": record.get("sales_nov"),
            }


@pytest.mark.not_b2
def test_auto_process_workbook_merges_sheets(monkeypatch, fake_storage_storage, in_memory_state):
    analysis_module = __import__("app.api.routers.analysis", fromlist=[""])

    target_table = "clients_workbook"
    records_store: Dict[str, Dict] = {}

    monkeypatch.setattr(
        analysis_module, "_get_analyze_file_for_import", lambda: lambda **_kwargs: _fake_analyze_response(target_table)
    )

    def fake_execute(file_content: bytes, file_name: str, all_records, llm_decision: dict):
        df = pd.read_csv(io.BytesIO(file_content))
        columns = {col.lower(): col for col in df.columns}
        df.rename(
            columns={
                columns.get("sales oct", "sales oct"): "sales_oct",
                columns.get("sales nov", "sales nov"): "sales_nov",
            },
            inplace=True,
        )
        records = df.to_dict("records")
        _upsert_clients(records_store, records)
        return {
            "success": True,
            "strategy_executed": llm_decision.get("strategy"),
            "table_name": target_table,
            "records_processed": len(records),
            "duplicates_skipped": 0,
        }

    monkeypatch.setattr("app.integrations.auto_import.execute_llm_import_decision", fake_execute)

    workbook_path = os.path.join("tests", "xlsx", "test-2-tabs.xlsx")
    with open(workbook_path, "rb") as handle:
        file_bytes = handle.read()
    file_id = _upload_workbook(file_bytes, filename="test-2-tabs.xlsx")

    response = client.get(f"/workbooks/{file_id}/sheets")
    assert response.status_code == 200, response.text
    sheets = response.json()["sheets"]
    assert set(sheets) == {"Clients Oct", "Clients Nov"}

    response = client.post(
        "/auto-process-workbook",
        data={
            "file_id": file_id,
            "sheet_names": '["Clients Oct","Clients Nov"]',
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "target_table_name": target_table,
            "target_table_mode": "new",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    job_id: Optional[str] = payload.get("job_id")
    assert job_id

    job = _wait_for_job(job_id)
    assert job["status"] == "succeeded"
    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 2
    assert metadata["failed_files"] == 0

    assert len(records_store) == 38
    for email in ("dnorcutta@csmonitor.com", "abanasiakb@google.ru"):
        row = records_store[email]
        assert row["sales_oct"] is not None
        assert row["sales_nov"] is not None
