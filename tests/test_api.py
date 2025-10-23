import time
import urllib.parse
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_root():
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Data Mapper API", "version": "1.0.0"}


def test_api_endpoints_exist():
    """Test that all new API endpoints exist and return proper error when DB unavailable."""
    # These will return 500 due to no database, but confirm endpoints exist
    response = client.get("/tables")
    assert response.status_code == 500  # Database connection error

    response = client.get("/tables/test_table")
    assert response.status_code == 500

    response = client.get("/tables/test_table/schema")
    assert response.status_code == 500

    response = client.get("/tables/test_table/stats")
    assert response.status_code == 500


def test_async_endpoints_exist():
    """Test that async endpoints exist."""
    # Test async task endpoint
    response = client.get("/tasks/test-task-id")
    assert response.status_code == 404  # Task not found (expected)

    # Test async processing endpoint (endpoint accepts request and queues it)
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


def test_response_structure():
    """Test that error responses have proper structure."""
    response = client.get("/tasks/non-existent-task")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "not found" in data["detail"].lower()


def test_map_b2_data_real_file():
    """Test mapping a real B2 Excel file end-to-end."""
    # Real B2 file URL
    real_url = "https://s3.us-east-005.backblazeb2.com/content-atlas/uploads/760ed001-5a4a-4bf3-85c8-98516cabd2b6/0f439c29-c563-4d5a-ade1-3381612aa5bf/Think%20Data%20Group%20-%20August%202025.xlsx?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=0058da29ca683780000000001%2F20251022%2Fus-east-005%2Fs3%2Faws4_request&X-Amz-Date=20251022T210726Z&X-Amz-Expires=86400&X-Amz-Signature=dcf845d70b56155cc21f5efd942b64740dd88d51d88d72a96cc05cfe9da4feee&X-Amz-SignedHeaders=host&x-id=GetObject"

    # Extract file name from URL (decode URL encoding)
    path_part = real_url.split('/content-atlas/', 1)[1].split('?', 1)[0]
    file_name = urllib.parse.unquote(path_part)

    # Step 1: Detect mapping from the real file
    response = client.post("/detect-b2-mapping", json={"file_name": file_name})
    assert response.status_code == 200
    detect_data = response.json()
    assert detect_data["success"] == True
    mapping = detect_data["detected_mapping"]

    # Step 2: Start async processing with detected mapping
    response = client.post("/map-b2-data-async", json={
        "file_name": file_name,
        "mapping": mapping
    })
    assert response.status_code == 200
    async_data = response.json()
    assert "task_id" in async_data
    task_id = async_data["task_id"]

    # Step 3: Poll task status until completion (with timeout)
    start_time = time.time()
    timeout = 300  # 5 minutes timeout
    while time.time() - start_time < timeout:
        response = client.get(f"/tasks/{task_id}")
        assert response.status_code == 200
        status_data = response.json()

        if status_data["status"] == "completed":
            # Verify successful completion
            assert status_data["result"]["success"] == True
            assert status_data["result"]["records_processed"] > 0
            break
        elif status_data["status"] == "failed":
            # Fail the test if processing failed
            assert False, f"Async processing failed: {status_data['message']}"

        # Wait before polling again
        time.sleep(2)
    else:
        # Timeout reached
        assert False, f"Task did not complete within {timeout} seconds"
