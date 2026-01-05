"""
Tests for validation failure tracking and resolution.
"""

import pytest
import json
import uuid
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from app.db.session import get_engine
from app.domain.imports.history import (
    start_import_tracking,
    record_validation_failures,
    create_import_history_table
)

client = TestClient(app)

@pytest.fixture
def cleanup_validation_tables():
    """Cleanup validation tables before and after tests."""
    engine = get_engine()
    
    # Ensure tables exist
    create_import_history_table()
    
    # Cleanup before test
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM import_validation_failures"))
        conn.execute(text("DELETE FROM import_history WHERE table_name LIKE 'test_validation_%'"))
    
    yield
    
    # Cleanup after test
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM import_validation_failures"))
        conn.execute(text("DELETE FROM import_history WHERE table_name LIKE 'test_validation_%'"))

class TestValidationFailures:
    """Test validation failure tracking and resolution."""
    
    def test_list_validation_failures(self, cleanup_validation_tables):
        """Test listing validation failures via API."""
        # Create a dummy import
        import_id = start_import_tracking(
            source_type="test",
            file_name="test.csv",
            table_name="test_validation_list"
        )
        
        # Record some failures
        failures = [
            {
                "record_number": 1,
                "record": {"name": "John", "email": "invalid-email"},
                "validation_errors": [{"column": "email", "error_type": "format", "message": "Invalid email"}]
            },
            {
                "record_number": 2,
                "record": {"name": "Jane", "age": "not-a-number"},
                "validation_errors": [{"column": "age", "error_type": "type", "message": "Invalid integer"}]
            }
        ]
        record_validation_failures(import_id, failures)
        
        # Test API endpoint
        response = client.get(f"/import-history/{import_id}/validation-failures")
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert len(data["failures"]) == 2
        assert data["total_count"] == 2
        
        # Verify content
        f1 = next(f for f in data["failures"] if f["record_number"] == 1)
        assert f1["record"]["name"] == "John"
        assert f1["validation_errors"][0]["column"] == "email"
        
    def test_get_validation_failure_detail(self, cleanup_validation_tables):
        """Test getting details of a specific failure."""
        import_id = start_import_tracking(
            source_type="test",
            file_name="test.csv",
            table_name="test_validation_detail"
        )
        
        failures = [
            {
                "record_number": 1,
                "record": {"name": "John", "email": "invalid-email"},
                "validation_errors": [{"column": "email", "error_type": "format", "message": "Invalid email"}]
            }
        ]
        record_validation_failures(import_id, failures)
        
        # Get the failure ID
        response = client.get(f"/import-history/{import_id}/validation-failures")
        failure_id = response.json()["failures"][0]["id"]
        
        # Test detail endpoint
        response = client.get(f"/import-history/{import_id}/validation-failures/{failure_id}")
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert data["failure"]["id"] == failure_id
        assert data["failure"]["record"]["name"] == "John"
        assert data["table_name"] == "test_validation_detail"

    def test_resolve_validation_failure_discard(self, cleanup_validation_tables):
        """Test resolving a failure by discarding it."""
        import_id = start_import_tracking(
            source_type="test",
            file_name="test.csv",
            table_name="test_validation_resolve"
        )
        
        failures = [
            {
                "record_number": 1,
                "record": {"name": "John", "email": "invalid-email"},
                "validation_errors": [{"column": "email", "error_type": "format", "message": "Invalid email"}]
            }
        ]
        record_validation_failures(import_id, failures)
        
        # Get failure ID
        response = client.get(f"/import-history/{import_id}/validation-failures")
        failure_id = response.json()["failures"][0]["id"]
        
        # Resolve as discarded
        response = client.post(
            f"/import-history/{import_id}/validation-failures/{failure_id}/resolve",
            json={"action": "discarded", "note": "Bad data"}
        )
        assert response.status_code == 200
        data = response.json()
        
        assert data["success"] is True
        assert data["failure"]["resolution_action"] == "discarded"
        assert data["failure"]["resolved_at"] is not None
        
        # Verify it's filtered out from list by default
        response = client.get(f"/import-history/{import_id}/validation-failures")
        assert len(response.json()["failures"]) == 0
        
        # Verify it appears if include_resolved=True
        response = client.get(f"/import-history/{import_id}/validation-failures?include_resolved=true")
        assert len(response.json()["failures"]) == 1
