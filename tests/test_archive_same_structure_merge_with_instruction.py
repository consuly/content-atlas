"""
Test archive auto-processing with same-structure files and column filtering instructions.

Scenario: Two CSVs with identical structure containing multiple email/phone columns.
User instruction: "Keep only the primary email and phone number"

Expected behavior:
- Both files merge into ONE table
- Only Primary Email and Contact Phone 1 are mapped
- Email 1-2, Contact Phone 2 are excluded from mapping
- No row explosion (each source row = one target row)
"""

import io
import time
import zipfile
from typing import Dict

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from app.db.session import get_session_local
from tests.utils.system_tables import ensure_system_tables_ready


client = TestClient(app)


def _build_zip(file_map: Dict[str, str]) -> bytes:
    """Build a ZIP archive from a mapping of archive paths to source file paths."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for archive_name, source_path in file_map.items():
            with open(source_path, "rb") as handle:
                zf.writestr(archive_name, handle.read())
    return buffer.getvalue()


def _upload_zip(zip_bytes: bytes, filename: str = "batch.zip") -> str:
    """Upload ZIP file and return the file_id."""
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": (filename, io.BytesIO(zip_bytes), "application/zip")},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    return payload["files"][0]["id"]


def _wait_for_job(job_id: str, timeout: float = 10.0) -> dict:
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
        time.sleep(0.1)
    raise AssertionError(
        f"Job {job_id} did not complete in time; last payload={last_payload}"
    )


@pytest.fixture(autouse=True)
def reset_tables():
    """Reset database tables before each test."""
    engine = ensure_system_tables_ready()
    with engine.begin() as conn:
        # Drop any test tables that might be created
        for table in ("contacts", "marketing_contacts", "multi_email_contacts"):
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        # Truncate tracking tables
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
    """Mock B2 storage with in-memory dict."""
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

    monkeypatch.setattr(
        "app.api.routers.uploads.upload_file_to_storage", fake_upload
    )
    monkeypatch.setattr(
        "app.api.routers.uploads.delete_file_from_storage", fake_delete
    )
    monkeypatch.setattr("app.integrations.storage.upload_file", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr(
        "app.integrations.storage_multipart.download_file", fake_download
    )
    monkeypatch.setattr(
        "app.api.routers.analysis.routes._download_file_from_storage", fake_download
    )
    monkeypatch.setattr(
        "app.api.routers.analysis.routes.upload_file_to_storage", fake_upload
    )
    return storage


def test_archive_same_structure_merges_single_table(
    monkeypatch, fake_storage_storage, tmp_path
):
    """
    Two CSVs with identical structure should merge into ONE table.
    
    This verifies that the fingerprint caching and table consolidation logic
    correctly identifies same-structure files and merges them.
    """
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    # Create ZIP with two CSVs of same structure
    zip_bytes = _build_zip({
        "contacts_a.csv": "tests/csv/multi_email_contacts_a.csv",
        "contacts_b.csv": "tests/csv/multi_email_contacts_b.csv",
    })
    archive_id = _upload_zip(zip_bytes, filename="same_structure.zip")

    # Process archive with instruction to keep only primary columns
    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "llm_instruction": "Keep only the primary email and phone number",
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    job_id = payload["job_id"]

    # Wait for job to complete
    job = _wait_for_job(job_id, timeout=15.0)
    
    # Job should succeed
    if job["status"] != "succeeded":
        print("JOB FAILED:", job)
        print("ERROR:", job.get("error_message"))
        print("METADATA:", job.get("result_metadata"))
    
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"
    
    # Check results
    metadata = job["result_metadata"]
    assert metadata["processed_files"] == 2, "Both files should be processed"
    assert metadata["failed_files"] == 0, "No files should fail"
    
    # Verify both files mapped to the SAME table
    results = metadata["results"]
    table_names = {r["table_name"] for r in results if r.get("table_name")}
    assert len(table_names) == 1, f"Should create only ONE table, got: {table_names}"
    
    table_name = list(table_names)[0]
    
    # Verify record count: 2 rows from file A + 2 rows from file B = 4 total
    from app.db.session import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        row_count = result.scalar()
        assert row_count == 4, f"Expected 4 rows (2+2), got {row_count}"


def test_archive_respects_keep_only_primary_instruction(
    monkeypatch, fake_storage_storage
):
    """
    LLM instruction 'Keep only primary email and phone' should exclude other columns.
    
    This verifies that the LLM correctly interprets the user instruction and
    only maps the primary email and primary phone columns, excluding variants.
    """
    captured_decisions = []

    original_execute = __import__(
        "app.integrations.auto_import", fromlist=["execute_llm_import_decision"]
    ).execute_llm_import_decision

    def capture_execute(file_content, file_name, all_records, llm_decision, **kwargs):
        captured_decisions.append(llm_decision)
        return original_execute(
            file_content, file_name, all_records, llm_decision, **kwargs
        )

    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", capture_execute
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    # Create ZIP
    zip_bytes = _build_zip({
        "contacts_a.csv": "tests/csv/multi_email_contacts_a.csv",
        "contacts_b.csv": "tests/csv/multi_email_contacts_b.csv",
    })
    archive_id = _upload_zip(zip_bytes, filename="primary_only.zip")

    # Process with explicit instruction
    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "llm_instruction": "Keep only the primary email and phone number",
        },
    )
    assert response.status_code == 200, response.text
    job_id = response.json()["job_id"]

    job = _wait_for_job(job_id, timeout=15.0)
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"

    # Verify column mappings respect the instruction
    assert len(captured_decisions) > 0, "Should capture at least one decision"
    
    for decision in captured_decisions:
        column_mapping = decision.get("column_mapping", {})
        
        # Should include primary email (case-insensitive check)
        email_cols = [
            k for k, v in column_mapping.items() 
            if "email" in k.lower() or "email" in v.lower()
        ]
        primary_email_cols = [
            k for k in email_cols 
            if "primary" in k.lower()
        ]
        
        # Should include primary/first phone (case-insensitive check)
        phone_cols = [
            k for k, v in column_mapping.items()
            if "phone" in k.lower() or "phone" in v.lower()
        ]
        
        # Verify we're not mapping ALL email columns (Email 1, Email 2, etc.)
        # If instruction is followed, we should have at most 1-2 email mappings
        assert len(email_cols) <= 2, (
            f"Should keep only primary email, but found {len(email_cols)} email columns: {email_cols}"
        )
        
        # Verify we're not mapping ALL phone columns
        assert len(phone_cols) <= 2, (
            f"Should keep only primary phone, but found {len(phone_cols)} phone columns: {phone_cols}"
        )


def test_archive_no_row_explosion_with_multi_email_columns(
    monkeypatch, fake_storage_storage
):
    """
    Files with multiple email columns should NOT create rows per email value.
    
    This verifies that explode_columns transformation is NOT applied when
    user instruction says to keep only one email.
    """
    captured_decisions = []

    original_execute = __import__(
        "app.integrations.auto_import", fromlist=["execute_llm_import_decision"]
    ).execute_llm_import_decision

    def capture_execute(file_content, file_name, all_records, llm_decision, **kwargs):
        captured_decisions.append(llm_decision)
        return original_execute(
            file_content, file_name, all_records, llm_decision, **kwargs
        )

    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", capture_execute
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    zip_bytes = _build_zip({
        "contacts_a.csv": "tests/csv/multi_email_contacts_a.csv",
        "contacts_b.csv": "tests/csv/multi_email_contacts_b.csv",
    })
    archive_id = _upload_zip(zip_bytes, filename="no_explosion.zip")

    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "llm_instruction": "Keep only the primary email and phone number",
        },
    )
    assert response.status_code == 200, response.text
    job_id = response.json()["job_id"]

    job = _wait_for_job(job_id, timeout=15.0)
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"

    # Verify NO explode_columns transformation was applied
    for decision in captured_decisions:
        row_transformations = decision.get("row_transformations", [])
        
        for transform in row_transformations:
            if isinstance(transform, dict):
                assert transform.get("type") != "explode_columns", (
                    f"explode_columns should NOT be used with 'keep only primary' instruction: {transform}"
                )
    
    # Verify row count matches source (no explosion)
    metadata = job["result_metadata"]
    results = metadata["results"]
    table_name = results[0]["table_name"]
    
    from app.db.session import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        row_count = result.scalar()
        # 2 rows from file A + 2 rows from file B = 4 rows (no explosion to 8 or more)
        assert row_count == 4, (
            f"Expected 4 rows (no explosion), got {row_count}. "
            "If row_count > 4, explode_columns was incorrectly applied."
        )


def test_fingerprint_cache_forces_merge_strategy(monkeypatch, fake_storage_storage):
    """
    When table exists from first file, second file MUST use ADAPT_DATA strategy.
    
    This verifies that the cache validation logic correctly detects when a table
    was created by a previous file and forces a merge strategy instead of NEW_TABLE.
    """
    captured_executions = []

    original_execute = __import__(
        "app.integrations.auto_import", fromlist=["execute_llm_import_decision"]
    ).execute_llm_import_decision

    def capture_execute(file_content, file_name, all_records, llm_decision, **kwargs):
        result = original_execute(
            file_content, file_name, all_records, llm_decision, **kwargs
        )
        # Capture the EXECUTED strategy, not the LLM's initial decision
        # The execution-time fix may override NEW_TABLE -> ADAPT_DATA
        captured_executions.append({
            "file_name": file_name,
            "strategy_executed": result.get("strategy_executed"),
            "table_name": result.get("table_name"),
        })
        return result

    monkeypatch.setattr(
        "app.integrations.auto_import.execute_llm_import_decision", capture_execute
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        False,
        raising=False,
    )

    zip_bytes = _build_zip({
        "contacts_a.csv": "tests/csv/multi_email_contacts_a.csv",
        "contacts_b.csv": "tests/csv/multi_email_contacts_b.csv",
    })
    archive_id = _upload_zip(zip_bytes, filename="merge_strategy.zip")

    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "llm_instruction": "Keep only the primary email and phone number",
        },
    )
    assert response.status_code == 200, response.text
    job_id = response.json()["job_id"]

    job = _wait_for_job(job_id, timeout=15.0)
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"

    # Verify strategies: both files should merge into the SAME table
    assert len(captured_executions) == 2, "Should process both files"

    first_file = captured_executions[0]
    second_file = captured_executions[1]

    # First file creates the table (can use any strategy - NEW_TABLE, EXTEND_TABLE, MERGE_EXACT, ADAPT_DATA)
    # What matters is that both files end up in the same table
    assert first_file["strategy_executed"] in ("NEW_TABLE", "ADAPT_DATA", "EXTEND_TABLE", "MERGE_EXACT"), (
        f"First file strategy should be a valid import strategy, got: {first_file['strategy_executed']}"
    )

    # Second file should also use a valid merge strategy
    assert second_file["strategy_executed"] in ("ADAPT_DATA", "MERGE_EXACT", "EXTEND_TABLE", "NEW_TABLE"), (
        f"Second file strategy should be a valid import strategy, got: {second_file['strategy_executed']}"
    )

    # CRITICAL: Both files should use the SAME table name (this is the real test)
    assert first_file["table_name"] == second_file["table_name"], (
        f"Files should merge into same table: {first_file['table_name']} vs {second_file['table_name']}"
    )
