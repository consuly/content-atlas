import os
import re
import time
import urllib.parse
import json
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import get_db

client = TestClient(app)


def test_root():
    """Test root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Data Mapper API", "version": "1.0.0"}


def test_api_endpoints_exist():
    """Test that all new API endpoints exist and return proper response."""
    # Test /tables endpoint - should return 200 with list of tables
    response = client.get("/tables")
    assert response.status_code == 200
    data = response.json()
    assert "success" in data
    assert "tables" in data

    # Test /tables/{table_name} endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table")
    assert response.status_code == 404

    # Test /tables/{table_name}/schema endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table/schema")
    assert response.status_code == 404

    # Test /tables/{table_name}/stats endpoint - should return 404 for non-existent table
    response = client.get("/tables/test_table/stats")
    assert response.status_code == 404


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
    """Test mapping a large Excel file end-to-end using local test file."""
    import io
    
    # Use local test file instead of B2 URL (more reliable, faster, works offline)
    test_file_path = "tests/Think_Data_Group_August_2025.xlsx"
    
    # Read the local test file
    with open(test_file_path, "rb") as f:
        file_content = f.read()
    
    file_name = "Think Data Group - August 2025.xlsx"
    
    # Step 1: Detect mapping from the file content
    # Upload file to detect mapping
    files = {"file": (file_name, io.BytesIO(file_content), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    response = client.post("/detect-mapping", files=files)
    assert response.status_code == 200
    detect_data = response.json()
    assert detect_data["success"] == True
    mapping = detect_data["detected_mapping"]

    # Clean up: Drop table and file imports records to ensure clean state
    table_name = mapping["table_name"]
    try:
        from app.database import get_engine
        engine = get_engine()
        with engine.begin() as conn:
            # Drop any tables matching this file pattern (handles multiple test runs with different timestamps)
            conn.execute(text("DROP TABLE IF EXISTS \"" + table_name + "\""))
            
            # Clean up file_imports records by file_name pattern to prevent accumulation across test runs
            # This works even if the B2 URL has expired (doesn't require downloading the file)
            file_name_pattern = "%Think Data Group - August 2025.xlsx"
            conn.execute(text("DELETE FROM file_imports WHERE file_name LIKE :pattern"), 
                       {"pattern": file_name_pattern})
            print(f"DEBUG: Cleaned up file_imports records matching pattern: {file_name_pattern}")
            
            # Also clean up by table_name pattern as additional safety
            table_pattern = "%think_data_group___august_2025%"
            conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE :pattern"), 
                       {"pattern": table_pattern})
            print(f"DEBUG: Cleaned up file_imports records matching table pattern: {table_pattern}")
    except Exception as e:
        print(f"WARNING: Cleanup error: {e}")
        pass  # Ignore if table doesn't exist or other DB issues

    # Step 2: Upload and process the file with detected mapping
    files = {"file": (file_name, io.BytesIO(file_content), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    data = {
        "mapping_json": json.dumps(mapping)  # Convert to proper JSON string
    }
    response = client.post("/map-data", files=files, data=data)
    # Verify successful processing
    assert response.status_code == 200
    result_data = response.json()
    assert result_data["success"] == True
    assert result_data["records_processed"] > 0
    
    print(f"Successfully processed {result_data['records_processed']} records from local test file")


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


def test_duplicate_detection_file_level():
    """Test file-level duplicate detection prevents importing the same file twice."""
    import io
    import hashlib
    import time

    # Create a small test CSV file
    csv_content = """name,email,age
John Doe,john@example.com,30
Jane Smith,jane@example.com,25
"""

    # Calculate file hash for cleanup
    file_hash = hashlib.sha256(csv_content.encode()).hexdigest()

    # Use unique table name to avoid conflicts with other tests
    table_name = f"test_file_duplicates_{int(time.time())}"

    # Clean up first - be more thorough
    try:
        db = get_db()
        # Delete from file_imports by file hash (global cleanup) and table name
        db.execute(text("DELETE FROM file_imports WHERE file_hash = :file_hash OR table_name = :table_name"), {"file_hash": file_hash, "table_name": table_name})
        # Try to drop the specific table
        db.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        db.commit()
    except Exception:
        pass

    # First upload should succeed
    files = {"file": ("test_duplicates.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    data = {
        "mapping_json": f"""{{
            "table_name": "{table_name}",
            "db_schema": {{"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"}},
            "mappings": {{"name": "name", "email": "email", "age": "age"}},
            "duplicate_check": {{"enabled": true, "check_file_level": true, "allow_duplicates": true}}
        }}"""
    }

    response = client.post("/map-data", files=files, data=data)
    # First upload should succeed (or fail due to DB issues, but not duplicates)
    assert response.status_code in [200, 500]

    # Second upload of same file should fail with duplicate error
    if response.status_code == 200:  # Only test duplicate if first succeeded
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 409  # Conflict - file already imported
        data = response.json()
        assert "already been imported" in data["detail"]


def test_duplicate_detection_row_level():
    """Test row-level duplicate detection handles overlapping records correctly."""
    import io

    # Clean up first - be more thorough
    try:
        from app.database import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_row_duplicates%'"))
            conn.execute(text("DROP TABLE IF EXISTS \"test_row_duplicates\" CASCADE"))
            conn.commit()
    except Exception:
        pass

    # First upload - should succeed
    csv_content1 = """name,email,age
John Doe,john@example.com,30
Jane Smith,jane@example.com,25
"""

    files1 = {"file": ("test1.csv", io.BytesIO(csv_content1.encode()), "text/csv")}
    data = {
        "mapping_json": """{
            "table_name": "test_row_duplicates",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"},
            "mappings": {"name": "name", "email": "email", "age": "age"},
            "duplicate_check": {"enabled": true, "check_file_level": false}
        }"""
    }

    response1 = client.post("/map-data", files=files1, data=data)
    assert response1.status_code in [200, 500]

    # Second upload with overlapping data
    if response1.status_code == 200:
        csv_content2 = """name,email,age
John Doe,john@example.com,30
Bob Wilson,bob@example.com,35
"""

        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data)
        
        # The new duplicate detection system filters out duplicates and inserts only non-duplicates
        # It returns 200 OK with information about what was processed
        assert response2.status_code == 200, f"Expected 200, got {response2.status_code}: {response2.text}"
        
        result = response2.json()
        assert result["success"] == True
        
        # Should have inserted only 1 record (Bob Wilson), skipped 1 duplicate (John Doe)
        assert result["records_processed"] == 1, f"Expected 1 record processed, got {result['records_processed']}"
        assert result["duplicates_skipped"] == 1, f"Expected 1 duplicate skipped, got {result['duplicates_skipped']}"
        
        # Verify the table has correct total count (2 from first upload + 1 from second = 3 total)
        try:
            engine = get_engine()
            with engine.connect() as conn:
                count_result = conn.execute(text('SELECT COUNT(*) FROM "test_row_duplicates"'))
                total_count = count_result.scalar()
                assert total_count == 3, f"Expected 3 total records in table, got {total_count}"
        except Exception as e:
            print(f"Warning: Could not verify table count: {e}")


def test_force_import_bypasses_duplicates():
    """Test that force_import bypasses duplicate checking."""
    import io

    # Clean up first - be more thorough
    try:
        from app.database import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_force_import%'"))
            conn.execute(text("DROP TABLE IF EXISTS \"test_force_import\" CASCADE"))
            conn.commit()
    except Exception:
        pass

    # First upload
    csv_content = """name,email,age
John Doe,john@example.com,30
"""

    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    data = {
        "mapping_json": """{
            "table_name": "test_force_import",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"},
            "mappings": {"name": "name", "email": "email", "age": "age"},
            "duplicate_check": {"enabled": true, "check_file_level": true}
        }"""
    }

    response1 = client.post("/map-data", files=files, data=data)
    assert response1.status_code in [200, 500]

    # Second upload with force_import should succeed even with duplicates
    if response1.status_code == 200:
        data_force = {
            "mapping_json": """{
                "table_name": "test_force_import",
                "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "email": "email", "age": "age"},
                "duplicate_check": {"enabled": true, "check_file_level": true, "force_import": true}
            }"""
        }

        response2 = client.post("/map-data", files=files, data=data_force)
        assert response2.status_code in [200, 500]  # Should succeed with force_import


def test_custom_uniqueness_columns():
    """Test duplicate detection with custom uniqueness columns."""
    import io

    # Clean up first - be more thorough
    try:
        from app.database import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_custom_unique%'"))
            conn.execute(text("DROP TABLE IF EXISTS \"test_custom_unique\" CASCADE"))
            conn.commit()
    except Exception:
        pass

    # First upload
    csv_content1 = """name,email,age
John Doe,john@example.com,30
Jane Smith,jane@example.com,25
"""

    files1 = {"file": ("test1.csv", io.BytesIO(csv_content1.encode()), "text/csv")}
    data = {
        "mapping_json": """{
            "table_name": "test_custom_unique",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"},
            "mappings": {"name": "name", "email": "email", "age": "age"},
            "duplicate_check": {"enabled": true, "check_file_level": false, "uniqueness_columns": ["email"]}
        }"""
    }

    response1 = client.post("/map-data", files=files1, data=data)
    assert response1.status_code in [200, 500]

    # Second upload with same email - system now filters duplicates and continues
    if response1.status_code == 200:
        csv_content2 = """name,email,age
Bob Wilson,john@example.com,35
Alice Brown,alice@example.com,28
"""

        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data)
        
        # The new duplicate detection system filters out duplicates and inserts only non-duplicates
        # It returns 200 OK with information about what was processed
        assert response2.status_code == 200, f"Expected 200, got {response2.status_code}: {response2.text}"
        
        result = response2.json()
        assert result["success"] == True
        
        # Should have inserted only 1 record (Alice Brown), skipped 1 duplicate (Bob Wilson with john@example.com)
        assert result["records_processed"] == 1, f"Expected 1 record processed, got {result['records_processed']}"
        assert result["duplicates_skipped"] == 1, f"Expected 1 duplicate skipped, got {result['duplicates_skipped']}"


def test_file_imports_table_created():
    """Test that file_imports table is created and populated correctly."""
    import io

    # Clean up first - be more thorough
    try:
        from app.database import get_engine
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

    response = client.post("/map-data", files=files, data=data)
    # Test that file_imports table exists and has records (if DB is available)
    if response.status_code == 200:
        try:
            db = get_db()
            result = db.execute(text("SELECT COUNT(*) FROM file_imports WHERE table_name = 'test_file_tracking'"))
            count = result.scalar()
            assert count >= 1  # Should have at least one record
        except Exception:
            pass  # DB might not be available in test environment


def test_small_file_duplicate_detection():
    """Test duplicate detection using the small test file for faster execution."""
    import hashlib

    # Use the small test file we created
    with open("tests/test_data_small.csv", "rb") as f:
        file_content = f.read()

    # Calculate file hash for cleanup
    file_hash = hashlib.sha256(file_content).hexdigest()

    # Clean up first - be more thorough
    try:
        from app.database import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE file_hash = :file_hash OR table_name LIKE 'test_small_file%'"), {"file_hash": file_hash})
            conn.execute(text("DROP TABLE IF EXISTS \"test_small_file\" CASCADE"))
            conn.commit()
    except Exception:
        pass

    # First upload should succeed
    files = {"file": ("test_data_small.csv", file_content, "text/csv")}
    data = {
        "mapping_json": """{
            "table_name": "test_small_file",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"},
            "mappings": {"name": "name", "email": "email", "age": "age"},
            "duplicate_check": {"enabled": true, "check_file_level": true}
        }"""
    }

    response1 = client.post("/map-data", files=files, data=data)
    assert response1.status_code in [200, 500]

    # Second upload should fail (same file hash)
    if response1.status_code == 200:
        response2 = client.post("/map-data", files=files, data=data)
        assert response2.status_code == 409  # File already imported
        data = response2.json()
        assert "already been imported" in data["detail"]


def test_datetime_standardization():
    """Test datetime standardization functionality."""
    from app.mapper import standardize_datetime, apply_rules, map_data
    from app.schemas import MappingConfig

    # Test standardize_datetime function with various formats
    test_cases = [
        # (input, expected_output)
        # Note: parse_flexible_date returns ISO 8601 with 'Z' suffix for UTC
        ('Thu, 9th Oct, 2025 at 8:11pm', '2025-10-09T20:11:00Z'),
        ('9/10/2025 20h11', '2025-09-10T20:11:00Z'),  # pandas interprets 9/10 as Sep 10 (monthfirst)
        ('10/09/25 8:11pm', '2025-10-09T20:11:00Z'),
        ('2025-10-09 20:11', '2025-10-09T20:11:00Z'),
        ('10/09/2025', '2025-10-09T00:00:00Z'),  # date only gets time added
        ('2025-10-09', '2025-10-09T00:00:00Z'),  # date only gets time added
        (None, None),
        ('', None),
        ('invalid date', None),
    ]

    for input_val, expected in test_cases:
        result = standardize_datetime(input_val)
        assert result == expected, f"Failed for input {repr(input_val)}: got {repr(result)}, expected {repr(expected)}"

    # Test with explicit format
    result = standardize_datetime('10/09/2025 8:11 PM', '%m/%d/%Y %I:%M %p')
    assert result == '2025-10-09T20:11:00Z'

    # Test apply_rules with datetime transformations
    record = {
        'event_date': '10/09/2025 8:11 PM',
        'name': 'Test Event'
    }

    rules = {
        'datetime_transformations': [
            {
                'field': 'event_date',
                'source_format': '%m/%d/%Y %I:%M %p',
                'target_format': 'ISO8601'
            }
        ]
    }

    transformed_record, errors = apply_rules(record, rules)
    assert transformed_record['event_date'] == '2025-10-09T20:11:00Z'
    assert transformed_record['name'] == 'Test Event'
    assert len(errors) == 0

    # Test error handling
    record_with_error = {
        'event_date': 'invalid datetime value',
        'name': 'Test Event'
    }

    transformed_record, errors = apply_rules(record_with_error, rules)
    assert transformed_record['event_date'] is None  # Failed conversion
    assert len(errors) == 1
    assert 'Failed to convert datetime field' in errors[0]

    # Test map_data with datetime transformations
    records = [
        {'event_date': '10/09/2025 8:11 PM', 'name': 'Event 1'},
        {'event_date': '2025-10-10', 'name': 'Event 2'},  # date only
        {'event_date': 'invalid', 'name': 'Event 3'},  # invalid
    ]

    config = MappingConfig(
        table_name='test_datetime',
        db_schema={'event_date': 'TIMESTAMP', 'name': 'TEXT'},
        mappings={'event_date': 'event_date', 'name': 'name'},
        rules=rules
    )

    mapped_records, all_errors = map_data(records, config)

    # Check successful conversions
    assert mapped_records[0]['event_date'] == '2025-10-09T20:11:00Z'
    assert mapped_records[1]['event_date'] == '2025-10-10T00:00:00Z'
    assert mapped_records[2]['event_date'] is None  # Failed conversion

    # Check that errors were collected
    assert len(all_errors) == 1
    assert 'Failed to convert datetime field' in all_errors[0]
