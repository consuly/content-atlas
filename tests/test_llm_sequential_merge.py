"""
Test for LLM-powered sequential file merging.

This test validates that the system can intelligently merge two similar CSV files
with different schemas into a single table without user intervention.

Real-world scenario: User uploads multiple related files (e.g., client lists from
different sources) and the system automatically organizes them into one table.
"""

import os
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.db.session import get_engine

client = TestClient(app)


@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_llm_sequential_file_merge():
    """
    Test that LLM can intelligently merge two similar files into one table.
    
    Process:
    1. Upload client-list-a.csv - LLM creates new table
    2. Upload client-list-b.csv - LLM recognizes similarity and merges into same table
    
    Expected: Only ONE table created with data from both files
    """
    
    # ============================================================================
    # SETUP & CLEANUP
    # ============================================================================
    print("\n" + "="*80)
    print("SETUP: Cleaning up any existing test data")
    print("="*80)
    
    try:
        engine = get_engine()
        with engine.begin() as conn:
            # Get ALL tables from the database
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            
            all_tables = [row[0] for row in result]
            all_tables_set = set(all_tables)
            
            # System tables that should NOT be dropped
            system_tables = {
                'file_imports', 
                'table_metadata', 
                'import_history', 
                'mapping_errors',
                'mapping_chunk_status',
                'import_duplicates'
            }
            
            # Filter to only user data tables (exclude system and test tables)
            user_tables = [
                t for t in all_tables
                if t not in system_tables
                and not t.startswith('test_')
                and not t.startswith('uploads')
            ]
            
            # Drop all user data tables to ensure clean slate
            if user_tables:
                print(f"  Found {len(user_tables)} user data table(s) to clean up:")
                for table_name in user_tables:
                    print(f"    Dropping: {table_name}")
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            else:
                print("  No user data tables to clean up")
            
            # Clean up ALL import tracking records (not just client-list files)
            # This ensures no orphaned records from previous test runs
            for system_table in ("file_imports", "table_metadata", "import_history"):
                if system_table in all_tables_set:
                    conn.execute(text(f'DELETE FROM "{system_table}"'))
                    print(f"  Cleaned up all {system_table} records")
                else:
                    print(f"  Skipping cleanup for {system_table}; table does not exist")
            
    except Exception as e:
        print(f"  Warning during cleanup: {e}")
    
    # ============================================================================
    # STEP 1: PROCESS FIRST FILE (client-list-a.csv)
    # ============================================================================
    print("\n" + "="*80)
    print("STEP 1: Processing first file (client-list-a.csv)")
    print("="*80)
    
    # Read first file
    with open("tests/csv/client-list-a.csv", "rb") as f:
        file_content_a = f.read()
    
    print(f"  File size: {len(file_content_a)} bytes")
    
    # Analyze and process first file with full automation
    files_a = {"file": ("client-list-a.csv", file_content_a, "text/csv")}
    response_a = client.post(
        "/analyze-file",
        files=files_a,
        data={
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": 5
        }
    )
    
    print(f"  Response status: {response_a.status_code}")
    assert response_a.status_code == 200, f"First file analysis failed: {response_a.text}"
    
    data_a = response_a.json()
    print(f"  Analysis success: {data_a['success']}")
    print(f"  LLM iterations used: {data_a['iterations_used']}/{data_a['max_iterations']}")
    
    if not data_a['success']:
        print(f"  ERROR: {data_a.get('error', 'Unknown error')}")
        pytest.fail(f"First file analysis failed: {data_a.get('error')}")
    
    print("\n  LLM Response (First File):")
    print("  " + "-"*76)
    for line in data_a['llm_response'].split('\n'):
        print(f"  {line}")
    print("  " + "-"*76)
    
    # Check tables after first file
    tables_response_1 = client.get("/tables")
    assert tables_response_1.status_code == 200
    tables_data_1 = tables_response_1.json()
    
    print(f"\n  Tables after first file: {len(tables_data_1['tables'])}")
    for table in tables_data_1['tables']:
        print(f"    - {table['table_name']}: {table['row_count']} rows")
    
    # Filter to only user data tables (exclude system and test tables)
    user_tables_1 = [
        t for t in tables_data_1['tables']
        if t['table_name'] not in ['file_imports', 'table_metadata', 'import_history', 'mapping_errors', 'import_duplicates', 'import_jobs', 'mapping_chunk_status']
        and not t['table_name'].startswith('test_')
        and not t['table_name'].startswith('uploads')
    ]
    
    print(f"\n  User data tables after first file: {len(user_tables_1)}")
    for table in user_tables_1:
        print(f"    - {table['table_name']}: {table['row_count']} rows")
    
    assert len(user_tables_1) == 1, \
        f"Expected 1 user data table after first file, got {len(user_tables_1)}"
    
    first_table_name = user_tables_1[0]['table_name']
    first_table_rows = user_tables_1[0]['row_count']
    
    print(f"\n  ✓ First file processed successfully")
    print(f"    Table: {first_table_name}")
    print(f"    Rows: {first_table_rows}")
    
    # ============================================================================
    # STEP 2: PROCESS SECOND FILE (client-list-b.csv)
    # ============================================================================
    print("\n" + "="*80)
    print("STEP 2: Processing second file (client-list-b.csv)")
    print("="*80)
    
    # Read second file
    with open("tests/csv/client-list-b.csv", "rb") as f:
        file_content_b = f.read()
    
    print(f"  File size: {len(file_content_b)} bytes")
    
    # Analyze and process second file with full automation
    files_b = {"file": ("client-list-b.csv", file_content_b, "text/csv")}
    response_b = client.post(
        "/analyze-file",
        files=files_b,
        data={
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": 5
        }
    )
    
    print(f"  Response status: {response_b.status_code}")
    assert response_b.status_code == 200, f"Second file analysis failed: {response_b.text}"
    
    data_b = response_b.json()
    print(f"  Analysis success: {data_b['success']}")
    print(f"  LLM iterations used: {data_b['iterations_used']}/{data_b['max_iterations']}")
    
    if not data_b['success']:
        print(f"  ERROR: {data_b.get('error', 'Unknown error')}")
        pytest.fail(f"Second file analysis failed: {data_b.get('error')}")
    
    print("\n  LLM Response (Second File):")
    print("  " + "-"*76)
    for line in data_b['llm_response'].split('\n'):
        print(f"  {line}")
    print("  " + "-"*76)
    
    # ============================================================================
    # STEP 3: VERIFICATION
    # ============================================================================
    print("\n" + "="*80)
    print("STEP 3: Verification")
    print("="*80)
    
    # Check tables after second file
    tables_response_2 = client.get("/tables")
    assert tables_response_2.status_code == 200
    tables_data_2 = tables_response_2.json()
    
    print(f"\n  Tables after second file: {len(tables_data_2['tables'])}")
    for table in tables_data_2['tables']:
        print(f"    - {table['table_name']}: {table['row_count']} rows")
    
    # Filter to only user data tables (exclude system and test tables)
    user_tables_2 = [
        t for t in tables_data_2['tables'] 
        if t['table_name'] not in ['file_imports', 'table_metadata', 'import_history', 'mapping_errors', 'import_duplicates', 'import_jobs', 'mapping_chunk_status'] 
        and not t['table_name'].startswith('test_')
        and not t['table_name'].startswith('uploads')
    ]
    
    print(f"\n  User data tables after second file: {len(user_tables_2)}")
    for table in user_tables_2:
        print(f"    - {table['table_name']}: {table['row_count']} rows")
    
    # CRITICAL ASSERTION: Only one user data table should exist
    assert len(user_tables_2) == 1, \
        f"ERROR: Expected 1 user data table, got {len(user_tables_2)}. " \
        f"LLM should have merged files into same table!"
    
    final_table_name = user_tables_2[0]['table_name']
    final_table_rows = user_tables_2[0]['row_count']
    
    print(f"\n  ✓ Only ONE table exists (as expected)")
    print(f"    Table: {final_table_name}")
    print(f"    Total rows: {final_table_rows}")
    print(f"    Rows from file A: ~{first_table_rows}")
    print(f"    Rows from file B: ~{final_table_rows - first_table_rows}")
    
    # Verify table name consistency
    assert final_table_name == first_table_name, \
        f"Table name changed! Was '{first_table_name}', now '{final_table_name}'"
    
    # Verify row count increased
    assert final_table_rows > first_table_rows, \
        f"Row count didn't increase! Was {first_table_rows}, now {final_table_rows}"
    
    # Get table schema
    schema_response = client.get(f"/tables/{final_table_name}/schema")
    assert schema_response.status_code == 200
    schema_data = schema_response.json()
    
    print(f"\n  Table Schema ({len(schema_data['columns'])} columns):")
    for col in schema_data['columns'][:10]:  # Show first 10 columns
        nullable = "NULL" if col['nullable'] else "NOT NULL"
        print(f"    - {col['name']}: {col['type']} ({nullable})")
    if len(schema_data['columns']) > 10:
        print(f"    ... and {len(schema_data['columns']) - 10} more columns")
    
    # Get sample data
    data_response = client.get(f"/tables/{final_table_name}?limit=5")
    assert data_response.status_code == 200
    data_result = data_response.json()
    
    print(f"\n  Sample Data (first 5 rows):")
    if data_result['data']:
        # Show a few key columns
        sample_cols = ['id', 'name', 'email', 'company_name']
        available_cols = [col for col in sample_cols if col in data_result['data'][0]]
        
        for i, row in enumerate(data_result['data'][:5], 1):
            print(f"    Row {i}:")
            for col in available_cols:
                value = row.get(col, 'N/A')
                if isinstance(value, str) and len(value) > 50:
                    value = value[:47] + "..."
                print(f"      {col}: {value}")
    
    # ============================================================================
    # STEP 4: VERIFY IMPORT HISTORY TRACKING
    # ============================================================================
    print("\n" + "="*80)
    print("STEP 4: Verify Import History Tracking")
    print("="*80)
    
    # Get all import history for this table
    history_response = client.get(f"/tables/{final_table_name}/lineage")
    assert history_response.status_code == 200, f"Failed to get import history: {history_response.text}"
    
    history_data = history_response.json()
    print(f"\n  Import lineage retrieved successfully")
    print(f"    Total imports: {history_data['total_imports']}")
    print(f"    Total rows contributed: {history_data['total_rows_contributed']}")
    
    # Should have exactly 2 imports (one for each file)
    assert history_data['total_imports'] == 2, \
        f"Expected 2 imports, got {history_data['total_imports']}"
    
    print(f"\n  ✓ Correct number of imports tracked: 2")
    
    # Verify import details
    imports = history_data['imports']
    
    # Sort by timestamp (oldest first)
    imports_sorted = sorted(imports, key=lambda x: x['import_timestamp'])
    
    print(f"\n  Import Details:")
    for i, imp in enumerate(imports_sorted, 1):
        print(f"\n    Import {i}:")
        print(f"      ID: {imp['import_id']}")
        print(f"      Timestamp: {imp['import_timestamp']}")
        print(f"      File: {imp['file_name']}")
        print(f"      Source: {imp['source_type']}")
        print(f"      Status: {imp['status']}")
        print(f"      Rows inserted: {imp['rows_inserted']}")
        print(f"      Duration: {imp['duration_seconds']:.2f}s")
        
        if imp.get('parsing_time_seconds'):
            print(f"      Parsing time: {imp['parsing_time_seconds']:.2f}s")
        if imp.get('insert_time_seconds'):
            print(f"      Insert time: {imp['insert_time_seconds']:.2f}s")
    
    # Verify first import
    first_import = imports_sorted[0]
    assert first_import['file_name'] == 'client-list-a.csv', \
        f"First import should be client-list-a.csv, got {first_import['file_name']}"
    assert first_import['status'] == 'success', \
        f"First import should be successful, got {first_import['status']}"
    assert first_import['rows_inserted'] == first_table_rows, \
        f"First import rows mismatch: expected {first_table_rows}, got {first_import['rows_inserted']}"
    
    print(f"\n  ✓ First import tracked correctly")
    
    # Verify second import
    second_import = imports_sorted[1]
    assert second_import['file_name'] == 'client-list-b.csv', \
        f"Second import should be client-list-b.csv, got {second_import['file_name']}"
    assert second_import['status'] == 'success', \
        f"Second import should be successful, got {second_import['status']}"
    
    expected_second_rows = final_table_rows - first_table_rows
    assert second_import['rows_inserted'] == expected_second_rows, \
        f"Second import rows mismatch: expected {expected_second_rows}, got {second_import['rows_inserted']}"
    
    print(f"  ✓ Second import tracked correctly")
    
    # Verify total rows match
    total_tracked_rows = sum(imp['rows_inserted'] for imp in imports)
    assert total_tracked_rows == final_table_rows, \
        f"Total tracked rows ({total_tracked_rows}) doesn't match table rows ({final_table_rows})"
    
    print(f"  ✓ Total tracked rows match table rows: {total_tracked_rows}")
    
    # Test import statistics endpoint
    stats_response = client.get("/import-statistics", params={"table_name": final_table_name})
    assert stats_response.status_code == 200
    stats_data = stats_response.json()
    
    print(f"\n  Import Statistics:")
    print(f"    Total imports: {stats_data['total_imports']}")
    print(f"    Successful: {stats_data['successful_imports']}")
    print(f"    Failed: {stats_data['failed_imports']}")
    print(f"    Total rows inserted: {stats_data['total_rows_inserted']}")
    print(f"    Avg duration: {stats_data['avg_duration_seconds']:.2f}s")
    
    assert stats_data['total_imports'] >= 2, \
        f"Statistics should show at least 2 imports"
    assert stats_data['successful_imports'] >= 2, \
        f"Statistics should show at least 2 successful imports"
    
    print(f"\n  ✓ Import statistics calculated correctly")
    
    # Test individual import detail endpoint
    first_import_detail = client.get(f"/import-history/{first_import['import_id']}")
    assert first_import_detail.status_code == 200
    detail_data = first_import_detail.json()
    
    print(f"\n  Individual Import Detail Retrieved:")
    print(f"    Import ID: {detail_data['import_record']['import_id']}")
    print(f"    File: {detail_data['import_record']['file_name']}")
    print(f"    Table: {detail_data['import_record']['table_name']}")
    print(f"    File hash: {detail_data['import_record']['file_hash'][:16]}...")
    
    assert detail_data['import_record']['file_name'] == 'client-list-a.csv'
    assert detail_data['import_record']['table_name'] == final_table_name
    
    print(f"  ✓ Individual import details retrieved correctly")
    
    # ============================================================================
    # FINAL RESULTS
    # ============================================================================
    print("\n" + "="*80)
    print("TEST RESULTS")
    print("="*80)
    print(f"  ✓ First file processed: {first_table_rows} rows")
    print(f"  ✓ Second file merged: {final_table_rows - first_table_rows} rows added")
    print(f"  ✓ Total rows in table: {final_table_rows}")
    print(f"  ✓ Only ONE table created: {final_table_name}")
    print(f"  ✓ LLM successfully merged similar files with different schemas")
    print(f"  ✓ Import history tracked: 2 imports recorded")
    print(f"  ✓ Import lineage verified: {total_tracked_rows} total rows")
    print(f"  ✓ Import statistics calculated correctly")
    print(f"  ✓ Individual import details retrievable")
    print("="*80)
    
    # ============================================================================
    # CLEANUP
    # ============================================================================
    print("\n" + "="*80)
    print("CLEANUP: Removing test data")
    print("="*80)
    
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{final_table_name}" CASCADE'))
            conn.execute(text("DELETE FROM file_imports WHERE file_name LIKE '%client-list%'"))
            conn.execute(text("DELETE FROM import_history WHERE file_name LIKE '%client-list%'"))
            print(f"  ✓ Cleaned up table: {final_table_name}")
            print(f"  ✓ Cleaned up import history")
    except Exception as e:
        print(f"  Warning during cleanup: {e}")
    
    print("="*80)
    print("TEST COMPLETED SUCCESSFULLY")
    print("="*80 + "\n")
