import pytest
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_instruction_crud_cycle():
    create_resp = client.post(
        "/llm-instructions",
        json={"title": "My Rules", "content": "Always keep phone numbers as text."},
    )
    assert create_resp.status_code == 200
    created = create_resp.json()
    instruction_id = created["id"]
    assert created["title"] == "My Rules"
    assert "content" in created

    list_resp = client.get("/llm-instructions")
    assert list_resp.status_code == 200
    payload = list_resp.json()
    assert payload["success"] is True
    ids = [instr["id"] for instr in payload["instructions"]]
    assert instruction_id in ids

    get_resp = client.get(f"/llm-instructions/{instruction_id}")
    assert get_resp.status_code == 200
    fetched = get_resp.json()
    assert fetched["id"] == instruction_id

    update_resp = client.patch(
        f"/llm-instructions/{instruction_id}",
        json={"title": "Updated Rules", "content": "Drop rows with empty email."},
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()
    assert updated["title"] == "Updated Rules"
    assert updated["content"] == "Drop rows with empty email."

    delete_resp = client.delete(f"/llm-instructions/{instruction_id}")
    assert delete_resp.status_code == 200
    assert delete_resp.json()["success"] is True

    missing_resp = client.get(f"/llm-instructions/{instruction_id}")
    assert missing_resp.status_code == 404


def test_update_requires_payload():
    create_resp = client.post(
        "/llm-instructions",
        json={"title": "Temp", "content": "Keep all rows."},
    )
    assert create_resp.status_code == 200
    instruction_id = create_resp.json()["id"]

    bad_resp = client.patch(f"/llm-instructions/{instruction_id}", json={})
    assert bad_resp.status_code == 400

    # Cleanup
    client.delete(f"/llm-instructions/{instruction_id}")
