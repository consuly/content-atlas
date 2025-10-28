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
from app.database import get_engine

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
            # Clean up any tables that might have been created from these files
            # Look for tables with names containing "client" or "contact"
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND (table_name LIKE '%client%' OR table_name LIKE '%contact%')
            """))
            
            existing_tables = [row[0] for row in result]
            for table_name in existing_tables:
                print(f"  Dropping existing table: {table_name}")
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
            
            # Clean up file_imports records
            conn.execute(text("DELETE FROM file_imports WHERE file_name LIKE '%client-list%'"))
            print("  Cleaned up file_imports records")
            
            # Clean up table_metadata records for client tables
            conn.execute(text("DELETE FROM table_metadata WHERE table_name LIKE '%client%'"))
            print("  Cleaned up table_metadata records")
            
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
        if t['table_name'] not in ['file_imports', 'table_metadata'] 
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
        if t['table_name'] not in ['file_imports', 'table_metadata'] 
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
            print(f"  ✓ Cleaned up table: {final_table_name}")
    except Exception as e:
        print(f"  Warning during cleanup: {e}")
    
    print("="*80)
    print("TEST COMPLETED SUCCESSFULLY")
    print("="*80 + "\n")
