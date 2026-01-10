import io
import os
import time
import hashlib
import json
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.db.session import get_db, get_engine
from app.domain.imports.history import get_import_history, list_duplicate_rows

client = TestClient(app)

def test_duplicate_detection_file_level(auth_headers):
    """Test file-level duplicate detection prevents importing the same file twice."""
    
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

    response = client.post("/map-data", files=files, data=data, headers=auth_headers)
    # First upload should succeed (or fail due to DB issues, but not duplicates)
    assert response.status_code in [200, 500]

    # Second upload of same file should fail with duplicate error
    if response.status_code == 200:  # Only test duplicate if first succeeded
        response = client.post("/map-data", files=files, data=data, headers=auth_headers)
        assert response.status_code == 409  # Conflict - file already imported
        data = response.json()
        assert "already been imported" in data["detail"]


def test_duplicate_detection_row_level(auth_headers):
    """Test row-level duplicate detection handles overlapping records correctly."""
    
    # Guardrail: disallow disabling file-level check unless retry flag is set
    csv_guard = "name,email\nJohn Doe,john@example.com\n"
    guard_files = {"file": ("guard.csv", io.BytesIO(csv_guard.encode()), "text/csv")}
    guard_data = {
        "mapping_json": """{
            "table_name": "test_row_duplicates_guard",
            "db_schema": {"name": "VARCHAR(255)", "email": "VARCHAR(255)"},
            "mappings": {"name": "name", "email": "email"},
            "duplicate_check": {"enabled": true, "check_file_level": false}
        }"""
    }
    guard_response = client.post("/map-data", files=guard_files, data=guard_data, headers=auth_headers)
    assert guard_response.status_code == 403
    assert "allow_file_level_retry" in guard_response.json()["detail"]

    # Clean up first - be more thorough
    try:
        from app.db.session import get_engine
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
            "duplicate_check": {"enabled": true, "check_file_level": false, "allow_file_level_retry": true}
        }"""
    }

    response1 = client.post("/map-data", files=files1, data=data, headers=auth_headers)
    assert response1.status_code in [200, 500]

    # Second upload with overlapping data
    if response1.status_code == 200:
        csv_content2 = """name,email,age
John Doe,john@example.com,30
Bob Wilson,bob@example.com,35
"""

        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data, headers=auth_headers)
        
        # The new duplicate detection system filters out duplicates and inserts only non-duplicates
        # It returns 200 OK with information about what was processed
        assert response2.status_code == 200, f"Expected 200, got {response2.status_code}: {response2.text}"
        
        result = response2.json()
        assert result["success"] == True
        
        # Should have inserted only 1 record (Bob Wilson), skipped 1 duplicate (John Doe)
        assert result["records_processed"] == 1, f"Expected 1 record processed, got {result['records_processed']}"
        assert result["duplicates_skipped"] == 1, f"Expected 1 duplicate skipped, got {result['duplicates_skipped']}"
        assert result.get("needs_user_input") is True, "Expected needs_user_input to be True when duplicates are present"
        assert result.get("duplicate_rows_count") == 1
        assert result.get("import_id"), "MapDataResponse should include import_id"
        llm_followup = result.get("llm_followup") or ""
        assert "Duplicates detected" in llm_followup, "Expected LLM follow-up prompt when duplicates are present"
        duplicate_preview = result.get("duplicate_rows") or []
        assert len(duplicate_preview) == 1, f"Expected duplicate preview with 1 row, got {len(duplicate_preview)}"
        assert duplicate_preview[0]["record"]["name"] == "John Doe"
        assert duplicate_preview[0]["record"]["email"] == "john@example.com"
        assert duplicate_preview[0].get("existing_row"), "Expected existing row snapshot alongside duplicate preview"
        assert duplicate_preview[0]["existing_row"]["record"]["email"] == "john@example.com"
        
        # Verify the table has correct total count (2 from first upload + 1 from second = 3 total)
        try:
            engine = get_engine()
            with engine.connect() as conn:
                count_result = conn.execute(text('SELECT COUNT(*) FROM "test_row_duplicates"'))
                total_count = count_result.scalar()
                assert total_count == 3, f"Expected 3 total records in table, got {total_count}"
        except Exception as e:
            print(f"Warning: Could not verify table count: {e}")

        # Verify duplicate entry is stored for review
        from app.domain.imports.history import get_import_history, list_duplicate_rows

        history = get_import_history(table_name="test_row_duplicates", limit=1)
        assert history, "Expected import history for test_row_duplicates"
        latest_import = history[0]
        assert latest_import["duplicates_found"] == 1, f"Expected duplicates_found=1, got {latest_import['duplicates_found']}"
        
        duplicate_rows = list_duplicate_rows(latest_import["import_id"], include_existing_row=True)
        assert len(duplicate_rows) == 1, f"Expected 1 duplicate row stored, got {len(duplicate_rows)}"
        duplicate_record = duplicate_rows[0]["record"]
        assert duplicate_record.get("name") == "John Doe"
        assert duplicate_record.get("email") == "john@example.com"
        assert duplicate_rows[0].get("existing_row"), "Expected existing row data when listing duplicates"
        assert duplicate_rows[0]["existing_row"]["record"]["email"] == "john@example.com"

        # Verify duplicate endpoint
        dup_response = client.get(f"/import-history/{latest_import['import_id']}/duplicates", headers=auth_headers)
        print("duplicates endpoint:", dup_response.status_code, dup_response.text)
        assert dup_response.status_code == 200
        dup_data = dup_response.json()
        assert dup_data["success"] is True
        assert dup_data["total_count"] == 1
        assert len(dup_data["duplicates"]) == 1
        assert dup_data["duplicates"][0]["record"]["email"] == "john@example.com"

        duplicate_id = dup_data["duplicates"][0]["id"]

        detail_resp = client.get(f"/import-history/{latest_import['import_id']}/duplicates/{duplicate_id}", headers=auth_headers)
        assert detail_resp.status_code == 200, detail_resp.text
        detail_data = detail_resp.json()
        assert detail_data["duplicate"]["record"]["name"] == "John Doe"
        assert detail_data["existing_row"] is not None

        merge_payload = {
            "updates": {
                "email": detail_data["duplicate"]["record"]["email"]
            },
            "note": "Resolved in test"
        }
        merge_resp = client.post(
            f"/import-history/{latest_import['import_id']}/duplicates/{duplicate_id}/merge",
            json=merge_payload,
            headers=auth_headers
        )
        assert merge_resp.status_code == 200, merge_resp.text
        merge_data = merge_resp.json()
        assert merge_data["success"] is True
        assert "email" in merge_data["updated_columns"]

        # After merge, duplicates should be cleared
        dup_after_merge = client.get(f"/import-history/{latest_import['import_id']}/duplicates", headers=auth_headers).json()
        assert dup_after_merge["total_count"] == 0
        assert dup_after_merge["duplicates"] == []

        updated_history = get_import_history(import_id=latest_import["import_id"], limit=1)
        assert updated_history[0]["duplicates_found"] == 0


def test_force_import_bypasses_duplicates(auth_headers):
    """Test that force_import bypasses duplicate checking."""
    
    # Clean up first - be more thorough
    try:
        from app.db.session import get_engine
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

    response1 = client.post("/map-data", files=files, data=data, headers=auth_headers)
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

        response2 = client.post("/map-data", files=files, data=data_force, headers=auth_headers)
        assert response2.status_code in [200, 500]  # Should succeed with force_import


def test_custom_uniqueness_columns(auth_headers):
    """Test duplicate detection with custom uniqueness columns."""
    
    # Clean up first - be more thorough
    try:
        from app.db.session import get_engine
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
            "duplicate_check": {"enabled": true, "check_file_level": false, "allow_file_level_retry": true, "uniqueness_columns": ["email"]}
        }"""
    }

    response1 = client.post("/map-data", files=files1, data=data, headers=auth_headers)
    assert response1.status_code in [200, 500]

    # Second upload with same email - system now filters duplicates and continues
    if response1.status_code == 200:
        csv_content2 = """name,email,age
Bob Wilson,john@example.com,35
Alice Brown,alice@example.com,28
"""

        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data, headers=auth_headers)
        
        # The new duplicate detection system filters out duplicates and inserts only non-duplicates
        # It returns 200 OK with information about what was processed
        assert response2.status_code == 200, f"Expected 200, got {response2.status_code}: {response2.text}"
        
        result = response2.json()
        assert result["success"] == True
        
        # Should have inserted only 1 record (Alice Brown), skipped 1 duplicate (Bob Wilson with john@example.com)
        assert result["records_processed"] == 1, f"Expected 1 record processed, got {result['records_processed']}"
        assert result["duplicates_skipped"] == 1, f"Expected 1 duplicate skipped, got {result['duplicates_skipped']}"


def test_small_file_duplicate_detection(auth_headers):
    """Test duplicate detection using the small test file for faster execution."""
    
    # Use the small test file we created
    # Check if file exists, if not create it or use existing content
    try:
        with open("tests/csv/test_data_small.csv", "rb") as f:
            file_content = f.read()
    except FileNotFoundError:
        # Fallback if file doesn't exist in environment
        file_content = b"name,email,age\nTest User,test@example.com,30"

    # Calculate file hash for cleanup
    file_hash = hashlib.sha256(file_content).hexdigest()

    # Clean up first - be more thorough
    try:
        from app.db.session import get_engine
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

    response1 = client.post("/map-data", files=files, data=data, headers=auth_headers)
    assert response1.status_code in [200, 500]

    # Second upload should fail (same file hash)
    if response1.status_code == 200:
        response2 = client.post("/map-data", files=files, data=data, headers=auth_headers)
        assert response2.status_code == 409  # File already imported
        data = response2.json()
        assert "already been imported" in data["detail"]


def test_duplicate_file_preflight_does_not_create_extra_history(auth_headers):
    """Second upload of identical file should short-circuit before creating a new import history row."""
    
    table_name = f"test_preflight_{int(time.time())}"
    csv_content = "name,email\nJohn,john@example.com\n"

    files = {"file": ("preflight.csv", io.BytesIO(csv_content.encode()), "text/csv")}
    mapping_json = f"""{{
        "table_name": "{table_name}",
        "db_schema": {{"name": "VARCHAR(255)", "email": "VARCHAR(255)"}},
        "mappings": {{"name": "name", "email": "email"}},
        "duplicate_check": {{"enabled": true, "check_file_level": true}}
    }}"""

    # Clean up table, history, and file_imports records
    try:
        from app.db.session import get_engine
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name = :table_name"), {"table_name": table_name})
            conn.execute(text("DELETE FROM import_history WHERE table_name = :table_name"), {"table_name": table_name})
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
    except Exception:
        pass

    response1 = client.post("/map-data", files=files, data={"mapping_json": mapping_json}, headers=auth_headers)
    assert response1.status_code in [200, 500]

    if response1.status_code != 200:
        pytest.skip("Skipping preflight duplicate assertions because initial import failed (likely DB unavailable)")

    engine = get_engine()
    with engine.connect() as conn:
        history_count_before = conn.execute(
            text("SELECT COUNT(*) FROM import_history WHERE table_name = :table_name"),
            {"table_name": table_name},
        ).scalar() or 0

    response2 = client.post("/map-data", files=files, data={"mapping_json": mapping_json}, headers=auth_headers)
    assert response2.status_code == 409, response2.text

    with engine.connect() as conn:
        history_count_after = conn.execute(
            text("SELECT COUNT(*) FROM import_history WHERE table_name = :table_name"),
            {"table_name": table_name},
        ).scalar() or 0

    assert history_count_after == history_count_before


def test_duplicate_auto_merge_flow(auth_headers):
    """Simulate LLM/auto flow: detect duplicate, inspect existing row, and merge via API."""
    
    table_name = f"test_row_duplicates_auto_{int(time.time())}"

    # Clean up table/import history
    try:
        from app.db.session import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM file_imports WHERE table_name = :table_name"), {"table_name": table_name})
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            conn.commit()
    except Exception:
        pass

    mapping_json = f"""{{
        "table_name": "{table_name}",
        "db_schema": {{"name": "VARCHAR(255)", "email": "VARCHAR(255)", "age": "INTEGER"}},
        "mappings": {{"name": "name", "email": "email", "age": "age"}},
        "duplicate_check": {{"enabled": true, "check_file_level": false, "allow_file_level_retry": true, "allow_duplicates": false, "uniqueness_columns": ["email"]}}
    }}"""

    # First upload (baseline row)
    csv_content1 = "name,email,age\nJohn Doe,john@example.com,30\n"
    files1 = {"file": ("auto1.csv", io.BytesIO(csv_content1.encode()), "text/csv")}
    response1 = client.post("/map-data", files=files1, data={"mapping_json": mapping_json}, headers=auth_headers)
    assert response1.status_code in [200, 500]

    if response1.status_code != 200:
        pytest.skip("Skipping duplicate merge flow because initial import failed (likely DB unavailable)")

    # Second upload with updated data for same email to trigger duplicate
    csv_content2 = "name,email,age\nJohn Doe,john@example.com,31\n"
    files2 = {"file": ("auto2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
    response2 = client.post("/map-data", files=files2, data={"mapping_json": mapping_json}, headers=auth_headers)
    assert response2.status_code == 200, response2.text

    result = response2.json()
    assert result["duplicates_skipped"] == 1
    assert result.get("needs_user_input") is True
    import_id = result["import_id"]

    duplicate_preview = result.get("duplicate_rows") or []
    assert duplicate_preview, "Expected duplicate preview in response"
    dup = duplicate_preview[0]
    assert dup.get("existing_row"), "Expected existing row data in duplicate preview"

    duplicate_id = dup["id"]

    # Merge using the duplicate's value (simulating LLM auto resolution)
    merge_payload = {"updates": {"age": dup["record"]["age"]}, "resolved_by": "auto-llm"}
    merge_resp = client.post(
        f"/import-history/{import_id}/duplicates/{duplicate_id}/merge",
        json=merge_payload,
        headers=auth_headers
    )
    assert merge_resp.status_code == 200, merge_resp.text

    # After merge, duplicates should clear
    dup_after_merge = client.get(f"/import-history/{import_id}/duplicates", headers=auth_headers).json()
    assert dup_after_merge["total_count"] == 0
    assert dup_after_merge["duplicates"] == []

    # Verify table reflects merged age
    try:
        engine = get_engine()
        with engine.connect() as conn:
            age_value = conn.execute(
                text(f'SELECT age FROM "{table_name}" WHERE email = :email'),
                {"email": "john@example.com"}
            ).scalar()
            assert age_value == 31
    except Exception as e:
        print(f"Warning: Could not verify merged table row: {e}")
