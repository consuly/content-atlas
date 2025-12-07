import os
import re
import time
import urllib.parse
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_query_database_structured_output_fallback():
    """Test that agent can provide ideas and fall back to LLM response if structured output fails."""
    # Database should already be populated from test_map_storage_data_real_file

    # Test the query-database endpoint with ideas request
    response = client.post("/query-database", json={
        "prompt": "Give me ideas on information we can extract that can help the clients optimize their next campaign"
    })
    assert response.status_code == 200
    data = response.json()

    # Validate basic response structure
    assert data["success"] == True
    assert "response" in data
    assert len(data["response"]) > 0

    # Check if structured output was generated
    if "structured_response" in data and data["structured_response"]:
        # If structured output exists, validate it has expected fields
        structured = data["structured_response"]
        assert "explanation" in structured
        assert "sql_query" in structured
        assert "execution_time_seconds" in structured
        assert "rows_returned" in structured
        assert "csv_data" in structured
    else:
        # If no structured output, ensure we got a valid LLM response
        # The agent should return the raw LLM response when structured output isn't available
        assert isinstance(data["response"], str)
        assert len(data["response"]) > 50  # Should be a meaningful response

        # May or may not have executed SQL depending on the query type
        # This test focuses on the fallback behavior, not SQL execution


@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_query_database_error_handling():
    """Test error handling for invalid queries and security."""
    # Test with non-SELECT query (agent should still respond safely)
    response = client.post("/query-database", json={
        "prompt": "Delete all data from the database"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True  # Agent handles gracefully
    assert "response" in data
    assert len(data["response"]) > 0  # Should provide some explanation

    # Test with SQL injection attempt
    response = client.post("/query-database", json={
        "prompt": "Show me data; DROP TABLE users; --"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    assert "response" in data
    assert len(data["response"]) > 0

    # Test with empty prompt
    response = client.post("/query-database", json={
        "prompt": ""
    })
    assert response.status_code == 200
    data = response.json()
    assert "success" in data  # Agent responds with some success status
    assert "response" in data  # Should provide some response

    # Test with very long prompt
    long_prompt = "Show me " + "data " * 1000
    response = client.post("/query-database", json={
        "prompt": long_prompt
    })
    assert response.status_code == 200
    data = response.json()
    assert data["success"] == True
    assert "response" in data
