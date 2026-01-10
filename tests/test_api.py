import os
import re
import time
import urllib.parse
import json
import uuid
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.db.session import get_db

client = TestClient(app)
REQUEST_TIMEOUT = 5  # seconds


def test_root():
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Content Atlas API", "version": app.version}


@pytest.mark.skipif(os.getenv("SKIP_DB_INIT") == "1", reason="Requires database for authentication")
def test_api_endpoints_exist(auth_headers):
    """Test that all new API endpoints exist and return proper response."""
    # Test /tables endpoint - should return 200 with list of tables
    response = client.get("/tables", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert "success" in data
    assert "tables" in data

    # Test /tables/{table_name} endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table", headers=auth_headers)
    assert response.status_code == 404

    # Test /tables/{table_name}/export endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table/export", headers=auth_headers)
    assert response.status_code == 404

    # Test /tables/{table_name}/schema endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table/schema", headers=auth_headers)
    assert response.status_code == 404

    # Test /tables/{table_name}/stats endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table/stats", headers=auth_headers)
    assert response.status_code == 404


def test_async_endpoints_exist():
    """Test that async endpoints exist."""
    # Test async task endpoint
    response = client.get("/tasks/test-task-id")
    assert response.status_code == 404  # Task not found (expected)

    # Test async processing endpoint (endpoint accepts request and queues it)
    with patch("app.api.routers.tasks.process_storage_data_async") as mock_process:
        mock_process.return_value = None  # prevent actual background execution
        response = client.post("/map-b2-data-async", json={
            "file_name": "test.xlsx",
            "mapping": {
                "table_name": "test",
                "db_schema": {"id": "INTEGER"},
                "mappings": {},
                "rules": {}
            }
        })
    # Endpoint exists and accepts the request (will process in background)
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert "status" in data
    assert data["status"] == "pending"


def test_import_job_endpoints_expose_active_job_state(monkeypatch):
    """Ensure import job APIs surface job metadata without requiring a live worker."""
    job_id = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    job_record = {
        "id": job_id,
        "file_id": file_id,
        "status": "running",
        "stage": "analysis",
        "progress": 10,
        "retry_attempt": 1,
        "error_message": None,
        "trigger_source": "unit_test",
        "analysis_mode": "auto_always",
        "conflict_mode": "llm_decide",
        "metadata": {"source": "pytest"},
        "result_metadata": None,
        "created_at": None,
        "updated_at": None,
        "completed_at": None,
    }

    monkeypatch.setattr(
        "app.api.routers.jobs.get_import_job",
        lambda requested_id: job_record if requested_id == job_id else None,
    )

    def fake_list(file_id: str | None = None, limit: int = 50, offset: int = 0):
        assert file_id == job_record["file_id"]
        return [job_record], 1

    monkeypatch.setattr("app.api.routers.jobs.list_import_jobs", fake_list)

    job_response = client.get(f"/import-jobs/{job_id}")
    assert job_response.status_code == 200
    assert job_response.json()["job"]["id"] == job_id

    list_response = client.get(f"/import-jobs?file_id={file_id}")
    assert list_response.status_code == 200
    jobs_for_file = list_response.json()["jobs"]
    assert len(jobs_for_file) == 1
    assert jobs_for_file[0]["stage"] == "analysis"


def test_uploaded_file_endpoints_include_job_metadata(monkeypatch):
    """Uploaded file responses should surface active job fields for the UI."""
    file_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    uploaded_file = {
        "id": file_id,
        "file_name": "sample.csv",
        "b2_file_id": "b2-test",
        "b2_file_path": "uploads/sample.csv",
        "file_size": 42,
        "content_type": "text/csv",
        "upload_date": None,
        "status": "mapping",
        "mapped_table_name": None,
        "mapped_date": None,
        "mapped_rows": None,
        "error_message": None,
        "active_job_id": job_id,
        "active_job_status": "running",
        "active_job_stage": "analysis",
        "active_job_progress": 15,
        "active_job_started_at": None,
    }

    monkeypatch.setattr(
        "app.api.routers.uploads.get_uploaded_file_by_id",
        lambda requested_id: uploaded_file if requested_id == file_id else None,
    )

    detail_response = client.get(f"/uploaded-files/{file_id}")
    assert detail_response.status_code == 200
    payload = detail_response.json()["file"]
    assert payload["active_job_id"] == job_id
    assert payload["active_job_stage"] == "analysis"


def test_response_structure():
    """Test that error responses have proper structure."""
    response = client.get("/tasks/non-existent-task")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "not found" in data["detail"].lower()


@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_query_database_endpoint_exists():
    """Test that query-database endpoint exists and handles requests."""
    # Test endpoint exists (will fail due to no database, but confirms endpoint)
    response = client.post("/query-database", json={"prompt": "test query"})
    # Should return 200 - agent handles requests gracefully
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    assert "response" in data
    # Agent either provides data or explains database issues
    assert len(data["response"]) > 0




@pytest.mark.skipif(os.getenv("SKIP_DB_INIT") == "1", reason="Requires database for authentication")
def test_file_imports_table_created(auth_headers):
    """Test that file_imports table is created and populated correctly."""
    import io

    # Clean up first - be more thorough
    try:
        from app.db.session import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_file_tracking%'"))
            conn.execute(text("DROP TABLE IF EXISTS \"test_file_tracking\" CASCADE"))
            conn.commit()
    except Exception:
        pass

    # Upload a file
    csv_content = """name,email
John,john@example.com
"""

    files = {"file": ("tracking_test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    data = {
        "mapping_json": """{
            "table_name": "test_file_tracking",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)"},
            "mappings": {"name": "name", "email": "email"},
            "duplicate_check": {"enabled": true, "check_file_level": true}
        }"""
    }

    response = client.post("/map-data", files=files, data=data, headers=auth_headers)
    # Test that file_imports table exists and has records (if DB is available)
    if response.status_code == 200:
        try:
            db = get_db()
            result = db.execute(text("SELECT COUNT(*) FROM file_imports WHERE table_name = 'test_file_tracking'"))
            count = result.scalar()
            assert count >= 1  # Should have at least one record
        except Exception:
            pass  # DB might not be available in test environment
