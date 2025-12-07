import json
import os
import uuid
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready


client = TestClient(app)


def _upload_fixture(csv_path: str) -> str:
    """Upload a CSV via the public endpoint and return the uploaded file ID."""
    with open(csv_path, "rb") as handle:
        response = client.post(
            "/upload-to-b2",
            data={"allow_duplicate": "true"},
            files={
                "file": (
                    os.path.basename(csv_path),
                    handle,
                    "text/csv",
                )
            },
        )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is True
    uploaded = payload["files"][0]
    return uploaded["id"]


@pytest.fixture(autouse=True)
def fake_storage_storage(monkeypatch):
    """
    Patch B2 helpers so uploads store bytes in-memory and downloads read from that store,
    mirroring the real controller flow used by the frontend.
    """
    storage: Dict[str, bytes] = {}

    def fake_upload(file_content: bytes, file_name: str, folder: str = "uploads"):
        path = f"{folder}/{file_name}"
        storage[path] = bytes(file_content)
        return {
            "file_id": str(uuid.uuid4()),
            "file_name": file_name,
            "file_path": path,
            "size": len(file_content),
        }

    def fake_download(file_path: str) -> bytes:
        if file_path not in storage:
            raise FileNotFoundError(f"File {file_path} not found in fake B2 bucket")
        return storage[file_path]

    def fake_delete(file_path: str) -> bool:
        storage.pop(file_path, None)
        return True

    # Patch the routers that call these helpers.
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.api.routers.uploads.delete_file_from_storage", fake_delete)
    monkeypatch.setattr("app.integrations.b2.download_file_from_storage", fake_download)
    monkeypatch.setattr("app.main.download_file_from_storage", fake_download, raising=False)

    return storage


@pytest.mark.not_b2
def test_marketing_agency_auto_process_recovers_via_llm_plan(monkeypatch, fake_storage_storage):
    """
    Full-stack regression test that mirrors the frontend workflow:
    - Upload the US CSV, auto-process successfully.
    - Upload the Texas CSV, allow the backend to adapt the schema and merge automatically.
    - Ensure duplicate reporting survives the entire flow.
    """
    # Disable shortcut fixtures so the real analysis/LLM path is used.
    monkeypatch.setattr(
        "app.core.config.settings.enable_marketing_fixture_shortcuts",
        False,
        raising=False,
    )
    monkeypatch.setattr(
        "app.core.config.settings.enable_auto_retry_failed_imports",
        True,
        raising=False,
    )

    engine = ensure_system_tables_ready()
    with engine.begin() as conn:
        for table in (
            "marketing_agency_contacts",
            "marketing_agency_leads_us",
            "clients_list",
        ):
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))

    with engine.begin() as conn:
        for table in (
            "file_imports",
            "table_metadata",
            "import_history",
            "mapping_errors",
            "mapping_chunk_status",
            "import_duplicates",
            "import_jobs",
            "uploaded_files",
        ):
            conn.execute(text(f'DELETE FROM "{table}"'))

    us_file_id = _upload_fixture("tests/csv/Marketing Agency - US.csv")
    tx_file_id = _upload_fixture("tests/csv/Marketing Agency - Texas.csv")

    auto_payload = {
        "analysis_mode": "auto_always",
        "conflict_resolution": "llm_decide",
        "max_iterations": "5",
    }

    response_us = client.post(
        "/analyze-file",
        data={"file_id": us_file_id, **auto_payload},
    )
    assert response_us.status_code == 200, response_us.text
    data_us = response_us.json()
    assert data_us["success"] is True
    assert "AUTO-EXECUTION COMPLETED" in data_us["llm_response"]

    response_tx = client.post(
        "/analyze-file",
        data={"file_id": tx_file_id, **auto_payload},
    )
    assert response_tx.status_code == 200, response_tx.text
    data_tx = response_tx.json()
    assert data_tx["success"] is True
    assert "AUTO-EXECUTION COMPLETED" in data_tx["llm_response"]
    auto_result = data_tx.get("auto_execution_result")
    assert auto_result and auto_result.get("table_name"), "Expected auto execution result with table name"
    target_table = auto_result["table_name"]

    # Verify duplicates were recorded for the Texas import
    with engine.connect() as conn:
        latest_import = conn.execute(
            text(
                """
                SELECT import_id, duplicates_found
                FROM import_history
                WHERE table_name = :table_name
                ORDER BY import_timestamp DESC
                LIMIT 1
                """
            ),
            {"table_name": target_table},
        ).mappings().one()

        assert latest_import["duplicates_found"] == 2

        duplicate_rows_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM import_duplicates
                WHERE import_id = :import_id
                """
            ),
            {"import_id": latest_import["import_id"]},
        ).scalar()

        assert duplicate_rows_count == 2

    # Confirm duplicate API mirrors the stored rows
    duplicates_response = client.get(
        f"/import-history/{latest_import['import_id']}/duplicates",
        params={"limit": 10},
    )
    assert duplicates_response.status_code == 200, duplicates_response.text
    duplicates_payload = duplicates_response.json()
    assert duplicates_payload["success"] is True
    assert duplicates_payload["total_count"] == 2
    assert len(duplicates_payload["duplicates"]) == 2

    print(
        "Duplicate rows via API:",
        json.dumps(duplicates_payload["duplicates"], indent=2),
    )

    # --- Deletion with optional table cleanup ---
    with engine.connect() as conn:
        imports: List[Dict] = conn.execute(
            text(
                """
                SELECT import_id, file_name
                FROM import_history
                WHERE table_name = :table_name
                ORDER BY import_timestamp ASC
                """
            ),
            {"table_name": target_table},
        ).mappings().all()

        imports_by_name = {row["file_name"]: row["import_id"] for row in imports}
        assert "Marketing Agency - US.csv" in imports_by_name
        assert "Marketing Agency - Texas.csv" in imports_by_name

        us_import_id = imports_by_name["Marketing Agency - US.csv"]
        tx_import_id = imports_by_name["Marketing Agency - Texas.csv"]

        us_rows_before = conn.execute(
            text(f'SELECT COUNT(*) FROM "{target_table}" WHERE _import_id = :import_id'),
            {"import_id": us_import_id},
        ).scalar()
        tx_rows_before = conn.execute(
            text(f'SELECT COUNT(*) FROM "{target_table}" WHERE _import_id = :import_id'),
            {"import_id": tx_import_id},
        ).scalar()
        assert us_rows_before > 0
        assert tx_rows_before > 0

    delete_response = client.delete(
        f"/uploaded-files/{tx_file_id}",
        params={"delete_table_data": "true"},
    )
    assert delete_response.status_code == 200, delete_response.text
    delete_payload = delete_response.json()
    assert delete_payload["success"] is True
    assert delete_payload["data_deleted"] is True
    assert delete_payload["rows_removed"] == tx_rows_before
    assert delete_payload["table_name"] == target_table
    import_ids = [str(value) for value in delete_payload.get("import_ids") or []]
    assert str(tx_import_id) in import_ids

    with engine.connect() as conn:
        # Texas rows should be gone; US rows should remain.
        us_rows_after = conn.execute(
            text(f'SELECT COUNT(*) FROM "{target_table}" WHERE _import_id = :import_id'),
            {"import_id": us_import_id},
        ).scalar()
        tx_rows_after = conn.execute(
            text(f'SELECT COUNT(*) FROM "{target_table}" WHERE _import_id = :import_id'),
            {"import_id": tx_import_id},
        ).scalar()

        assert us_rows_after == us_rows_before
        assert tx_rows_after == 0

        # Import history should retain US but drop Texas.
        remaining_imports = conn.execute(
            text(
                """
                SELECT import_id, file_name
                FROM import_history
                WHERE table_name = :table_name
                """
            ),
            {"table_name": target_table},
        ).mappings().all()
        remaining_ids = {row["import_id"] for row in remaining_imports}
        assert us_import_id in remaining_ids
        assert tx_import_id not in remaining_ids
