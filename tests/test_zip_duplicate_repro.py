import io
import os
import time
import zipfile
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready
from app.db.session import get_engine

client = TestClient(app)

def _wait_for_job(job_id: str, timeout: float = 30.0, auth_headers: dict = None) -> dict:
    """Poll import job until completion or timeout."""
    deadline = time.monotonic() + timeout
    last_payload = None
    while time.monotonic() < deadline:
        resp = client.get(f"/import-jobs/{job_id}", headers=auth_headers)
        assert resp.status_code == 200, resp.text
        last_payload = resp.json()
        job = last_payload.get("job") or {}
        if job.get("status") in ("succeeded", "failed"):
            return job
        time.sleep(0.5)
    raise AssertionError(f"Job {job_id} did not complete in time; last payload={last_payload}")

@pytest.fixture
def fake_storage(monkeypatch):
    storage = {}
    
    # Store the actual ZIP content in "B2" (memory)
    zip_path = "tests/csv/Mock-data_duplicate.zip"
    if os.path.exists(zip_path):
        with open(zip_path, "rb") as f:
            zip_content = f.read()
            # Determine B2 path. Usually uploads/<filename>
            # The upload endpoint returns a path.
            pass

    def fake_upload(file_content, file_name, folder="uploads"):
        path = f"{folder}/{file_name}"
        # If it's bytes wrapper, read it
        if hasattr(file_content, "read"):
             content = file_content.read()
        else:
             content = file_content
        storage[path] = bytes(content)
        return {
            "file_id": file_name,
            "file_name": file_name,
            "file_path": path,
            "size": len(content),
        }
    
    def fake_download(file_path):
        if file_path in storage:
            return storage[file_path]
        
        # Check if it maps to our local test file
        if "Mock-data_duplicate.zip" in file_path:
             with open("tests/csv/Mock-data_duplicate.zip", "rb") as f:
                 return f.read()
                 
        raise Exception(f"File not found: {file_path}")
    
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.integrations.storage.upload_file", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    # Monkeypatch the one used in analysis routes
    monkeypatch.setattr("app.api.routers.analysis.routes._get_download_file_from_storage", lambda: fake_download)
    
    return storage

def test_zip_duplicate_detection_repro(monkeypatch, fake_storage, auth_headers):
    # 1. Setup Database
    engine = ensure_system_tables_ready()
    table_name = f"repro_zip_dupes_{int(time.time())}"
    
    # Cleanup
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        conn.execute(text("DELETE FROM file_imports WHERE table_name = :t"), {"t": table_name})
        conn.execute(text("DELETE FROM import_history WHERE table_name = :t"), {"t": table_name})

    # 2. Upload ZIP to get file_id
    zip_path = "tests/csv/Mock-data_duplicate.zip"
    with open(zip_path, "rb") as f:
        zip_bytes = f.read()
    
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": ("Mock-data_duplicate.zip", io.BytesIO(zip_bytes), "application/zip")},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    file_id = response.json()["files"][0]["id"]
    
    # 3. Run Auto Process with REAL LLM (no mocking)
    # Force both files to the same table so LLM has consistent context
    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": file_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "target_table_name": table_name,
            "target_table_mode": "new",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    job_id = response.json()["job_id"]
    
    # 4. Wait for Result
    print(f"Waiting for job {job_id}...")
    job = _wait_for_job(job_id, timeout=120.0, auth_headers=auth_headers)  # Longer timeout for real LLM
    
    # 5. Verify Results
    print("JOB RESULT:", job)
    metadata = job["result_metadata"]
    results = metadata.get("results", [])
    
    processed_count = 0
    duplicates_found_total = 0
    actual_unique_columns = None
    
    for res in results:
        status = res.get("status")
        dupes = res.get("duplicates_skipped", 0)
        records = res.get("records_processed", 0)
        archive_path = res.get('archive_path', 'unknown')
        print(f"File: {archive_path} - Status: {status} - Records: {records} - Duplicates: {dupes}")
        
        if status == "processed":
            processed_count += 1
        duplicates_found_total += dupes

    # Assertions - be flexible with LLM's decision
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"
    assert processed_count == 2, f"Expected 2 files processed, got {processed_count}"
    
    # Check total rows in table
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
        print(f"Total rows in table '{table_name}': {count}")
        
        # Get table columns to see what LLM decided
        from sqlalchemy import inspect
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        print(f"Table columns: {columns}")

    # The LLM should NOT choose 'id' as the unique column (our improvement should prevent this)
    # If it did, we'd see all 1000 rows from file B flagged as duplicates (total duplicates = 1000)
    # If it chose email or a composite key, we'd see reasonable duplicate counts
    
    print(f"Total duplicates found: {duplicates_found_total}")
    print(f"Total rows in table: {count}")
    
    # Validate: The second file should NOT have ALL rows marked as duplicates
    # (which was the bug when id was used as unique column)
    assert duplicates_found_total < 1000, (
        f"REGRESSION: LLM chose poor uniqueness columns (likely 'id'). "
        f"Found {duplicates_found_total} duplicates, which suggests all rows in second file were flagged. "
        f"LLM should choose business-meaningful columns like email, not sequential IDs."
    )
    
    # Validate: Both files contributed rows (no file was entirely duplicate)
    assert count > 0, "No rows were imported - both files marked as complete duplicates"
    assert count < 2000, f"Too many rows ({count}) - expected some deduplication"
    
    print(f"✓ Test passed: LLM chose sensible uniqueness columns (duplicates={duplicates_found_total}, total_rows={count})")
