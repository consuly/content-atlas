import uuid
import os

import pytest
from fastapi.testclient import TestClient
import requests

from app.main import app
from app.domain.queries.history import save_query_message, list_query_threads, get_query_conversation


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(scope="module")
def ensure_db_ready():
    """
    Ensure the database is writable for the real persistence layer.
    Skip tests if connections fail so we don't mask runtime issues with stubs.
    """
    thread_id = str(uuid.uuid4())
    try:
        save_query_message(thread_id, "user", "ping")
    except Exception as exc:  # pragma: no cover - guard against missing DB locally
        pytest.skip(f"Database unavailable for query conversation integration tests: {exc}")

    yield


def test_query_conversation_endpoints_exist(client):
    # Latest conversation should return 200 and a success payload
    for path in ("/query-conversations/latest", "/api/v1/query-conversations/latest"):
        latest_resp = client.get(path)
        assert latest_resp.status_code == 200
        data = latest_resp.json()
        assert data["success"] is True

    # Listing conversations should not 404 even if empty
    for path in ("/query-conversations", "/api/v1/query-conversations"):
        list_resp = client.get(path)
        assert list_resp.status_code == 200
        data = list_resp.json()
        assert data["success"] is True
        assert isinstance(data["conversations"], list)


def test_query_conversation_roundtrip_real_persistence(client, ensure_db_ready):
    """
    Use the real persistence layer (not monkeypatch) to verify the API surface matches frontend expectations.
    """
    thread_id = str(uuid.uuid4())

    # Seed a conversation using the real DB helpers to mimic frontend flow
    save_query_message(thread_id, "user", "hello world")
    save_query_message(thread_id, "assistant", "hi there", executed_sql="select 1", data_csv="a\n1")

    # Validate GET by thread (base and versioned)
    for path in (
        f"/query-conversations/{thread_id}",
        f"/api/v1/query-conversations/{thread_id}",
    ):
        conv_resp = client.get(path)
        assert conv_resp.status_code == 200
        payload = conv_resp.json()
        assert payload["success"] is True
        conversation = payload["conversation"]
        assert conversation["thread_id"] == thread_id
        assert len(conversation["messages"]) == 2
        roles = [m["role"] for m in conversation["messages"]]
        assert roles == ["user", "assistant"]

    # Latest should surface the most recent thread
    latest_resp = client.get("/query-conversations/latest")
    assert latest_resp.status_code == 200
    latest = latest_resp.json()
    assert latest["success"] is True
    assert latest["conversation"]["thread_id"] == thread_id

    # List endpoint should include the thread
    list_resp = client.get("/query-conversations?limit=5&offset=0")
    assert list_resp.status_code == 200
    list_payload = list_resp.json()
    assert list_payload["success"] is True
    ids = [conv["thread_id"] for conv in list_payload["conversations"]]
    assert thread_id in ids


@pytest.mark.integration
def test_query_conversation_frontend_base_url_roundtrip():
    """
    Mirror the frontend fetch logic against a running server on the same base
    URL the UI uses (default http://127.0.0.1:8000).

    No extra config is required; set QUERY_CONVERSATION_BASE_URL only if you
    intentionally run the API elsewhere. If the server is not reachable, the
    test is skipped with a clear message.
    """
    base_url = os.getenv("QUERY_CONVERSATION_BASE_URL", "http://127.0.0.1:8000")
    base = base_url.rstrip("/")
    urls = [
        f"{base}/query-conversations/latest",
        f"{base}/api/v1/query-conversations/latest",
    ]

    errors = []
    for url in urls:
        try:
            resp = requests.get(url, timeout=5)
        except requests.exceptions.ConnectionError as exc:
            pytest.skip(f"Query conversation API not reachable at {base_url} (connection error: {exc})")
        except Exception as exc:
            errors.append(f"{url}: request failed ({exc})")
            continue

        if resp.status_code == 404:
            errors.append(f"{url}: 404 not found")
            continue

        if resp.ok:
            data = resp.json()
            assert data["success"] is True
            return  # Found a working endpoint matching frontend expectations

        errors.append(f"{url}: unexpected status {resp.status_code}")

    pytest.fail(
        "Query conversation endpoints not reachable via frontend base URL. "
        "Ensure the backend is serving at the same base as VITE_API_URL. "
        f"Tried:\n" + "\n".join(errors)
    )
