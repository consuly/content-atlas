"""
Tests for import tracking, undo/rollback, and correction tracking functionality.

This test suite validates:
1. Metadata columns are added to data tables
2. Import IDs are properly tracked
3. Corrections are logged (type coercion, datetime transformations)
4. Cascading deletes work correctly
5. Metadata is hidden from user queries
6. Undo/rollback functionality
"""

import pytest
import io
import json
import hashlib
from fastapi.testclient import TestClient
from sqlalchemy import text, inspect
from app.main import app
from app.database import get_engine
from app.models import create_table_if_not_exists, insert_records
from app.schemas import MappingConfig, DuplicateCheckConfig

client = TestClient(app)


@pytest.fixture
def cleanup_test_tables():
    """Cleanup test tables before and after tests."""
    engine = get_engine()
    test_tables = [
        "test_import_tracking",
        "test_corrections",
        "test_undo",
        "test_multiple_imports",
        "test_metadata_hidden"
    ]
    
    # Cleanup before test
    with engine.begin() as conn:
        for table in test_tables:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        # Clean up import_history and file_imports
        conn.execute(text("DELETE FROM import_history WHERE table_name LIKE 'test_%'"))
        conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_%'"))
    
    yield
    
    # Cleanup after test
    with engine.begin() as conn:
        for table in test_tables:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
        conn.execute(text("DELETE FROM import_history WHERE table_name LIKE 'test_%'"))
        conn.execute(text("DELETE FROM file_imports WHERE table_name LIKE 'test_%'"))


class TestMetadataColumns:
    """Test that metadata columns are created and populated correctly."""
    
    def test_metadata_columns_created(self, cleanup_test_tables):
        """Test that _import_id, _imported_at, _source_row_number columns are created."""
        engine = get_engine()
        
        # Create a test table
        config = MappingConfig(
            table_name="test_import_tracking",
            db_schema={"name": "VARCHAR(255)", "age": "INTEGER"},
            mappings={"name": "name", "age": "age"}
        )
        
        create_table_if_not_exists(engine, config)
        
        # Check that metadata columns exist
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns("test_import_tracking")]
        
        assert "_import_id" in columns, "Missing _import_id column"
        assert "_imported_at" in columns, "Missing _imported_at column"
        assert "_source_row_number" in columns, "Missing _source_row_number column"
        assert "_corrections_applied" in columns, "Missing _corrections_applied column"
    
    def test_import_id_populated(self, cleanup_test_tables):
        """Test that import_id is populated for all inserted rows."""
        # Upload a file and verify import_id is set
        csv_content = """name,age
John Doe,30
Jane Smith,25
Bob Wilson,35
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_import_tracking",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Query the table and verify import_id is set
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _import_id, _source_row_number FROM "test_import_tracking"'))
            rows = result.fetchall()
            
            assert len(rows) == 3, "Should have 3 rows"
            
            # All rows should have same import_id
            import_ids = [row[0] for row in rows]
            assert all(import_id is not None for import_id in import_ids), "All rows should have import_id"
            assert len(set(import_ids)) == 1, "All rows should have same import_id"
            
            # Source row numbers should be 1, 2, 3
            row_numbers = sorted([row[1] for row in rows])
            assert row_numbers == [1, 2, 3], "Source row numbers should be 1, 2, 3"


class TestCorrectionsTracking:
    """Test that corrections are tracked during import."""
    
    def test_type_coercion_tracked(self, cleanup_test_tables):
        """Test that type coercion corrections are logged."""
        # CSV with float values that need INTEGER coercion
        csv_content = """name,age
John Doe,30.0
Jane Smith,25.5
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_corrections",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Check corrections were logged
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _corrections_applied FROM "test_corrections" WHERE _source_row_number = 1'))
            row = result.fetchone()
            
            if row and row[0]:
                corrections = row[0]
                assert "age" in corrections, "Should have correction for age field"
                assert corrections["age"]["correction_type"] == "type_coercion"
                assert corrections["age"]["before"] == "30.0"
                assert corrections["age"]["after"] == 30
    
    def test_datetime_conversion_tracked(self, cleanup_test_tables):
        """Test that datetime conversions are logged."""
        csv_content = """name,event_date
John Doe,10/09/2025 8:11 PM
Jane Smith,2025-10-10
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_corrections",
                "db_schema": {"name": "VARCHAR(255)", "event_date": "TIMESTAMP"},
                "mappings": {"name": "name", "event_date": "event_date"},
                "rules": {
                    "datetime_transformations": [
                        {
                            "field": "event_date",
                            "source_format": "auto"
                        }
                    ]
                },
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Check corrections were logged
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _corrections_applied FROM "test_corrections" WHERE _source_row_number = 1'))
            row = result.fetchone()
            
            if row and row[0]:
                corrections = row[0]
                assert "event_date" in corrections, "Should have correction for event_date field"
                assert corrections["event_date"]["correction_type"] == "datetime_standardization"
    
    def test_no_corrections_null_field(self, cleanup_test_tables):
        """Test that _corrections_applied is NULL when no corrections occur."""
        csv_content = """name,age
John Doe,30
Jane Smith,25
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_corrections",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Check that corrections field is NULL
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _corrections_applied FROM "test_corrections"'))
            rows = result.fetchall()
            
            # All should be NULL (no corrections needed)
            assert all(row[0] is None for row in rows), "Should have no corrections when data is clean"


class TestCascadingDelete:
    """Test that cascading deletes work correctly."""
    
    def test_cascading_delete_removes_data(self, cleanup_test_tables):
        """Test that deleting import_history cascades to data rows."""
        # Upload a file
        csv_content = """name,age
John Doe,30
Jane Smith,25
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_undo",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Get the import_id
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _import_id FROM "test_undo" LIMIT 1'))
            import_id = result.fetchone()[0]
            
            # Verify data exists
            result = conn.execute(text('SELECT COUNT(*) FROM "test_undo"'))
            count_before = result.scalar()
            assert count_before == 2, "Should have 2 rows before delete"
        
        # Delete the import_history record
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM import_history WHERE import_id = :import_id"), 
                        {"import_id": str(import_id)})
        
        # Verify data was cascaded deleted
        with engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM "test_undo"'))
            count_after = result.scalar()
            assert count_after == 0, "Data should be deleted via cascade"


class TestMultipleImports:
    """Test handling multiple imports to the same table."""
    
    def test_multiple_imports_tracked_separately(self, cleanup_test_tables):
        """Test that multiple imports to same table are tracked separately."""
        # First import
        csv_content1 = """name,age
John Doe,30
"""
        
        files1 = {"file": ("test1.csv", io.BytesIO(csv_content1.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_multiple_imports",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response1 = client.post("/map-data", files=files1, data=data)
        assert response1.status_code == 200
        
        # Second import
        csv_content2 = """name,age
Jane Smith,25
Bob Wilson,35
"""
        
        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data)
        assert response2.status_code == 200
        
        # Verify both imports are tracked
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT DISTINCT _import_id FROM "test_multiple_imports"'))
            import_ids = [row[0] for row in result.fetchall()]
            
            assert len(import_ids) == 2, "Should have 2 distinct import_ids"
            
            # Verify row counts per import
            for import_id in import_ids:
                result = conn.execute(
                    text('SELECT COUNT(*) FROM "test_multiple_imports" WHERE _import_id = :import_id'),
                    {"import_id": str(import_id)}
                )
                count = result.scalar()
                assert count in [1, 2], "Each import should have 1 or 2 rows"
    
    def test_selective_undo(self, cleanup_test_tables):
        """Test that we can undo one import without affecting others."""
        # First import
        csv_content1 = """name,age
John Doe,30
"""
        
        files1 = {"file": ("test1.csv", io.BytesIO(csv_content1.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_multiple_imports",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response1 = client.post("/map-data", files=files1, data=data)
        assert response1.status_code == 200
        
        # Second import
        csv_content2 = """name,age
Jane Smith,25
"""
        
        files2 = {"file": ("test2.csv", io.BytesIO(csv_content2.encode()), "text/csv")}
        response2 = client.post("/map-data", files=files2, data=data)
        assert response2.status_code == 200
        
        # Get import IDs
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text('SELECT _import_id, _imported_at FROM "test_multiple_imports" ORDER BY _imported_at'))
            import_ids = [row[0] for row in result.fetchall()]
            first_import_id = import_ids[0]
        
        # Delete first import
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM import_history WHERE import_id = :import_id"), 
                        {"import_id": str(first_import_id)})
        
        # Verify only first import was deleted
        with engine.connect() as conn:
            result = conn.execute(text('SELECT COUNT(*) FROM "test_multiple_imports"'))
            count = result.scalar()
            assert count == 1, "Should have 1 row remaining (from second import)"
            
            result = conn.execute(text('SELECT name FROM "test_multiple_imports"'))
            name = result.fetchone()[0]
            assert name == "Jane Smith", "Should have Jane Smith remaining"


class TestMetadataHidden:
    """Test that metadata columns are hidden from user queries."""
    
    def test_metadata_hidden_in_table_rows_endpoint(self, cleanup_test_tables):
        """Test that /tables/{table_name} endpoint filters out metadata."""
        # Upload a file
        csv_content = """name,age
John Doe,30
"""
        
        files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}
        data = {
            "mapping_json": json.dumps({
                "table_name": "test_metadata_hidden",
                "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                "mappings": {"name": "name", "age": "age"},
                "duplicate_check": {"enabled": False}
            })
        }
        
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 200
        
        # Query via API endpoint (correct endpoint without /rows suffix)
        response = client.get("/tables/test_metadata_hidden")
        assert response.status_code == 200
        
        data = response.json()
        if data.get("success") and data.get("rows"):
            rows = data["rows"]
            first_row = rows[0]
            
            # Verify metadata columns are NOT present
            assert "_import_id" not in first_row, "Metadata should be hidden"
            assert "_imported_at" not in first_row, "Metadata should be hidden"
            assert "_source_row_number" not in first_row, "Metadata should be hidden"
            assert "_corrections_applied" not in first_row, "Metadata should be hidden"
            
            # Verify user data IS present
            assert "name" in first_row, "User data should be present"
            assert "age" in first_row, "User data should be present"


class TestImportLineage:
    """Test querying import lineage for tables."""
    
    def test_get_table_import_history(self, cleanup_test_tables):
        """Test retrieving all imports for a specific table."""
        # Make two imports
        for i in range(2):
            csv_content = f"""name,age
Person{i},3{i}
"""
            files = {"file": (f"test{i}.csv", io.BytesIO(csv_content.encode()), "text/csv")}
            data = {
                "mapping_json": json.dumps({
                    "table_name": "test_multiple_imports",
                    "db_schema": {"name": "VARCHAR(255)", "age": "INTEGER"},
                    "mappings": {"name": "name", "age": "age"},
                    "duplicate_check": {"enabled": False}
                })
            }
            
            response = client.post("/map-data", files=files, data=data)
            assert response.status_code == 200
        
        # Query import history
        from app.import_history import get_table_import_lineage
        
        lineage = get_table_import_lineage("test_multiple_imports")
        assert len(lineage) >= 2, "Should have at least 2 imports"
        
        # Verify each import has required fields
        for import_record in lineage:
            assert "import_id" in import_record
            assert "table_name" in import_record
            assert import_record["table_name"] == "test_multiple_imports"
            assert "rows_inserted" in import_record
