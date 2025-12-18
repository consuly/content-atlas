"""
Tests for the SQL generation endpoint (/api/v1/generate-sql).
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from app.main import app
from app.core.api_key_auth import ApiKey, get_api_key_from_header


# Mock API key for testing
def mock_get_api_key():
    """Override API key dependency."""
    mock_key = ApiKey(
        id="test-id",
        key_hash="test-hash",
        app_name="Test App",
        description="Test Key",
        created_by="test",
        is_active=True
    )
    return mock_key


@pytest.fixture
def client():
    """Test client with overridden API key dependency."""
    app.dependency_overrides[get_api_key_from_header] = mock_get_api_key
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def mock_sql_generator():
    """Mock the SQL generator to avoid actual LLM calls."""
    with patch("app.api.routers.public_api.generate_sql_from_prompt") as mock:
        yield mock


def test_generate_sql_success(client, mock_sql_generator):
    """Test successful SQL generation."""
    # Mock successful generation
    mock_sql_generator.return_value = {
        "success": True,
        "sql_query": 'SELECT "email", "company_name" FROM "clients-list" LIMIT 10000',
        "tables_referenced": ["clients-list"],
        "explanation": "Selecting email and company columns from clients table"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={"prompt": "Get top 10000 clients with email and company"},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["success"] is True
    assert data["sql_query"] == 'SELECT "email", "company_name" FROM "clients-list" LIMIT 10000'
    assert data["tables_referenced"] == ["clients-list"]
    assert "email" in data["explanation"]
    assert data["error"] is None


def test_generate_sql_with_table_hints(client, mock_sql_generator):
    """Test SQL generation with table hints."""
    mock_sql_generator.return_value = {
        "success": True,
        "sql_query": 'SELECT * FROM "clients-list" WHERE "state" = \'CA\'',
        "tables_referenced": ["clients-list"],
        "explanation": "Filtering clients by California state"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={
            "prompt": "Get clients from California",
            "table_hints": ["clients-list"]
        },
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["success"] is True
    assert "clients-list" in data["sql_query"]
    
    # Verify table_hints was passed to generator
    mock_sql_generator.assert_called_once()
    call_args = mock_sql_generator.call_args
    assert call_args[1]["table_hints"] == ["clients-list"]


def test_generate_sql_failure(client, mock_sql_generator):
    """Test SQL generation failure."""
    mock_sql_generator.return_value = {
        "success": False,
        "error": "Could not generate SQL: ambiguous table reference"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={"prompt": "Get some data"},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200  # Still 200, but success=False in body
    data = response.json()
    
    assert data["success"] is False
    assert "ambiguous" in data["error"]
    assert data["sql_query"] is None


def test_generate_sql_missing_prompt(client):
    """Test that missing prompt returns validation error."""
    response = client.post(
        "/api/v1/generate-sql",
        json={},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 422  # Validation error


def test_generate_sql_system_exception(client, mock_sql_generator):
    """Test handling of unexpected exceptions."""
    mock_sql_generator.side_effect = Exception("Database connection failed")
    
    response = client.post(
        "/api/v1/generate-sql",
        json={"prompt": "Get clients"},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 500
    assert "SQL generation failed" in response.json()["detail"]


def test_generate_sql_empty_table_hints(client, mock_sql_generator):
    """Test that empty table_hints is handled correctly."""
    mock_sql_generator.return_value = {
        "success": True,
        "sql_query": 'SELECT * FROM "test-table"',
        "tables_referenced": ["test-table"],
        "explanation": "Query generated"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={
            "prompt": "Get data",
            "table_hints": []
        },
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    
    # Verify empty list was passed
    call_args = mock_sql_generator.call_args
    assert call_args[1]["table_hints"] == []


def test_generate_sql_protected_table_blocked(client, mock_sql_generator):
    """Test that attempts to query protected tables are blocked."""
    mock_sql_generator.return_value = {
        "success": False,
        "error": "Cannot access protected system table: users"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={"prompt": "Get all users"},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["success"] is False
    assert "protected" in data["error"].lower()


def test_generate_sql_non_select_blocked(client, mock_sql_generator):
    """Test that non-SELECT queries are blocked."""
    mock_sql_generator.return_value = {
        "success": False,
        "error": "Generated query is not a SELECT statement"
    }
    
    response = client.post(
        "/api/v1/generate-sql",
        json={"prompt": "Delete all records"},
        headers={"X-API-Key": "test-key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["success"] is False
    assert "SELECT" in data["error"]
