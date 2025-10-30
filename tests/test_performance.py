"""
Performance test for data import - measures actual import speed without double processing.
"""
import io
import json
import time
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.database import get_engine

client = TestClient(app)


def test_import_performance_120k_rows():
    """
    Test import performance for 120K row file.
    
    This test skips the detect-mapping step to measure pure import performance.
    The original test_map_b2_data_real_file parses the file twice (once for
    detect-mapping, once for import), which doubles the time.
    """
    # Read the test file
    test_file_path = "tests/Think_Data_Group_August_2025.xlsx"
    with open(test_file_path, "rb") as f:
        file_content = f.read()
    
    file_name = "Think Data Group - August 2025.xlsx"
    
    # Create mapping manually (skip detect-mapping to avoid double parsing)
    # This is a simplified schema - adjust based on your actual file structure
    table_name = f"perf_test_{int(time.time())}"
    
    mapping = {
        "table_name": table_name,
        "db_schema": {
            # Add your actual column definitions here
            # For now, using TEXT for all columns as a safe default
            "column_1": "TEXT",
            "column_2": "TEXT",
            "column_3": "TEXT",
            # ... add more columns as needed
        },
        "mappings": {
            # Map source columns to target columns
            # For now, using identity mapping
        },
        "rules": {},
        "duplicate_check": {
            "enabled": False  # Disable for pure performance test
        }
    }
    
    # Clean up any existing test data
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.execute(text("DELETE FROM file_imports WHERE table_name = :table_name"), 
                       {"table_name": table_name})
    except Exception as e:
        print(f"Cleanup warning: {e}")
    
    # Measure import time
    start_time = time.time()
    
    files = {"file": (file_name, io.BytesIO(file_content), 
             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    data = {"mapping_json": json.dumps(mapping)}
    
    response = client.post("/map-data", files=files, data=data)
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Verify success
    assert response.status_code == 200
    result_data = response.json()
    assert result_data["success"] == True
    
    records_processed = result_data["records_processed"]
    
    print(f"\n{'='*60}")
    print(f"PERFORMANCE TEST RESULTS")
    print(f"{'='*60}")
    print(f"File: {file_name}")
    print(f"Records processed: {records_processed:,}")
    print(f"Import time: {duration:.2f} seconds")
    print(f"Records/second: {records_processed/duration:.0f}")
    print(f"{'='*60}\n")
    
    # Cleanup
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.execute(text("DELETE FROM file_imports WHERE table_name = :table_name"), 
                       {"table_name": table_name})
    except Exception:
        pass


def test_import_with_detect_mapping():
    """
    Original test that includes detect-mapping step.
    This will be slower because it parses the file twice.
    """
    test_file_path = "tests/Think_Data_Group_August_2025.xlsx"
    with open(test_file_path, "rb") as f:
        file_content = f.read()
    
    file_name = "Think Data Group - August 2025.xlsx"
    
    # Step 1: Detect mapping (first parse)
    detect_start = time.time()
    files = {"file": (file_name, io.BytesIO(file_content), 
             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    response = client.post("/detect-mapping", files=files)
    detect_time = time.time() - detect_start
    
    assert response.status_code == 200
    detect_data = response.json()
    mapping = detect_data["detected_mapping"]
    table_name = mapping["table_name"]
    
    # Clean up
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            file_name_pattern = "%Think Data Group - August 2025.xlsx"
            conn.execute(text("DELETE FROM file_imports WHERE file_name LIKE :pattern"), 
                       {"pattern": file_name_pattern})
    except Exception as e:
        print(f"Cleanup warning: {e}")
    
    # Step 2: Import data (second parse)
    import_start = time.time()
    files = {"file": (file_name, io.BytesIO(file_content), 
             "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
    data = {"mapping_json": json.dumps(mapping)}
    response = client.post("/map-data", files=files, data=data)
    import_time = time.time() - import_start
    
    total_time = detect_time + import_time
    
    assert response.status_code == 200
    result_data = response.json()
    records_processed = result_data["records_processed"]
    
    print(f"\n{'='*60}")
    print(f"FULL WORKFLOW TEST RESULTS (with detect-mapping)")
    print(f"{'='*60}")
    print(f"Detect mapping time: {detect_time:.2f}s")
    print(f"Import time: {import_time:.2f}s")
    print(f"Total time: {total_time:.2f}s")
    print(f"Records processed: {records_processed:,}")
    print(f"{'='*60}\n")
    
    # Cleanup
    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.execute(text("DELETE FROM file_imports WHERE table_name = :table_name"), 
                       {"table_name": table_name})
    except Exception:
        pass
