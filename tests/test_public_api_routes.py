"""
Tests for the public API routes to ensure authentication is enforced and
basic responses work when dependencies are stubbed. We set SKIP_DB_INIT to
avoid hitting a real database during these fast unit tests.
"""

import os
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

# Avoid database bootstrap for these tests
os.environ["SKIP_DB_INIT"] = "1"

from app.main import app  # noqa: E402
from app.core.api_key_auth import get_api_key_from_header  # noqa: E402
from app.db.session import get_db  # noqa: E402
from app.api.routers import public_api  # noqa: E402

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_dependency_overrides():
    """Ensure dependency overrides are cleared between tests."""
    original_overrides = app.dependency_overrides.copy()
    yield
    app.dependency_overrides = original_overrides


def test_public_tables_requires_api_key():
    """Requests without X-API-Key should be rejected with 401, not 500."""
    response = client.get("/api/v1/tables")
    assert response.status_code == 401
    detail = response.json()["detail"]
    assert "API key required" in detail


def test_public_query_succeeds_with_stubbed_dependencies(monkeypatch):
    """Public query should work with a valid API key when dependencies are stubbed."""
    # Stub auth dependency to avoid real DB lookups
    app.dependency_overrides[get_api_key_from_header] = lambda: SimpleNamespace(
        id="stub-api-key", app_name="test-client"
    )

    # Stub DB dependency to avoid real engine creation
    def fake_db():
        yield SimpleNamespace()

    app.dependency_overrides[get_db] = fake_db

    # Stub the query agent to return predictable data without hitting LLM/DB
    fake_result = {
        "success": True,
        "response": "Stubbed response",
        "executed_sql": "SELECT 1",
        "data_csv": "value\n1",
        "execution_time_seconds": 0.01,
        "rows_returned": 1,
        "error": None,
    }
    monkeypatch.setattr(public_api, "query_database_with_agent", lambda *args, **kwargs: fake_result)

    payload = {"prompt": "test prompt", "thread_id": "thread-1"}
    response = client.post(
        "/api/v1/query",
        json=payload,
        headers={"X-API-Key": "atlas_live_sk_stub"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["response"] == fake_result["response"]
    assert data["executed_sql"] == fake_result["executed_sql"]
    assert data["rows_returned"] == fake_result["rows_returned"]
