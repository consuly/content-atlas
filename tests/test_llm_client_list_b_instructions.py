import csv
import io
import os
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text

from app.main import app
from app.db.session import get_engine
from app.core.config import settings


client = TestClient(app)
FIXTURE_DIR = Path("tests/csv")
INPUT_FILE = FIXTURE_DIR / "client-list-b.csv"
EXPECTED_FILE = FIXTURE_DIR / "client-list-b-instruction-output.csv"
SPECIAL_INSTRUCTION = (
    "If you have multiple emails in a row create a new entry for each email in only one column "
    "so we have only one email returned per row. So if there is 4 emails in a row it will generate "
    "4 rows with each a different email but the same information around. Skip columns that are not "
    "helpful for a client contact list like IDs value or status from a previous export."
)


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def _load_expected_rows() -> list[dict]:
    with EXPECTED_FILE.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [
            {k: (v or "").strip() for k, v in row.items()}
            for row in reader
        ]


def _fetch_table_rows(table_name: str, expected_headers: list[str]) -> list[dict]:
    engine = get_engine()
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        pytest.fail(f"Mapped table '{table_name}' not found after auto execution.")

    actual_columns = {col["name"] for col in inspector.get_columns(table_name)}
    normalized_actual = {_normalize_key(col): col for col in actual_columns}

    column_map = {}
    missing = []
    for header in expected_headers:
        normalized = _normalize_key(header)
        actual_name = normalized_actual.get(normalized)
        if not actual_name:
            missing.append(header)
            continue
        column_map[header] = actual_name

    if missing:
        pytest.fail(
            f"Mapped table missing expected columns: {missing}. "
            f"Available columns: {sorted(actual_columns)}"
        )

    select_clause = ", ".join(f'"{actual}"' for actual in column_map.values())
    query = text(f'SELECT {select_clause} FROM "{table_name}"')

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    normalized_rows = []
    for row in rows:
        row_dict = dict(row._mapping)
        normalized_rows.append({
            header: (row_dict[column_map[header]] or "")
            for header in expected_headers
        })
    return normalized_rows


def _compare_rows(actual_rows: list[dict], expected_rows: list[dict]) -> None:
    def sort_key(row: dict) -> tuple[str, str]:
        return (
            (row.get("Contact Full Name") or "").lower(),
            (row.get("email") or "").lower(),
        )

    actual_sorted = sorted(actual_rows, key=sort_key)
    expected_sorted = sorted(expected_rows, key=sort_key)

    if len(actual_sorted) != len(expected_sorted):
        pytest.fail(
            f"Row count mismatch: expected {len(expected_sorted)} rows, "
            f"got {len(actual_sorted)} rows."
        )

    for idx, (actual, expected) in enumerate(zip(actual_sorted, expected_sorted)):
        if actual != expected:
            pytest.fail(
                f"Row {idx + 1} does not match expected output.\n"
                f"Expected: {expected}\nActual:   {actual}"
            )


def _reset_clients_list_state():
    """Ensure a clean slate so the LLM treats this as a first-time mapping."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "clients_list" CASCADE'))
        for table in ("import_history", "mapping_errors", "table_metadata", "import_duplicates"):
            try:
                conn.execute(text(f"DELETE FROM {table} WHERE table_name = 'clients_list'"))
            except Exception:
                # Table might not exist in some local setups; ignore so the test can proceed.
                pass


@pytest.fixture(scope="session")
def require_llm():
    """Skip when no Anthropic API key is configured."""
    if not settings.anthropic_api_key or not settings.anthropic_api_key.strip():
        pytest.skip("Anthropic API key not configured; real LLM tests require ANTHROPIC_API_KEY")
    return settings.anthropic_api_key


@pytest.mark.skipif(os.getenv("CI"), reason="Skip expensive LLM tests in CI")
def test_llm_mapping_with_email_explode_instruction(monkeypatch, require_llm):
    """
    Ensure the LLM honors the explode-email instruction and produces the expected mapping output.

    The test disables the deterministic client-list fixtures so the request flows through
    the regular LLM analysis path with the provided instruction.
    """
    # Disable deterministic client-list shortcut so the LLM processes the request.
    from app.api import routers as api_routers

    monkeypatch.setattr(
        api_routers.analysis,
        "_handle_client_list_special_case",
        lambda **_: None,
    )

    _reset_clients_list_state()

    with INPUT_FILE.open("rb") as f:
        file_bytes = f.read()

    response = client.post(
        "/analyze-file",
        files={"file": (INPUT_FILE.name, io.BytesIO(file_bytes), "text/csv")},
        data={
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": 5,
            "llm_instruction": SPECIAL_INSTRUCTION,
            "target_table_name": "clients_list",
            "target_table_mode": "new",
            # Force the LLM path to only split columns it explicitly names.
            "require_explicit_multi_value": True,
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload.get("success") is True, payload

    auto_result = payload.get("auto_execution_result")
    if not auto_result or not auto_result.get("success"):
        pytest.fail(f"Auto execution missing or failed: {payload}")

    mapping_errors = auto_result.get("mapping_errors") or []
    if mapping_errors:
        pytest.fail(f"Auto execution reported mapping errors: {mapping_errors}")

    if (auto_result.get("records_processed") or 0) == 0:
        pytest.fail(f"No records were processed. Auto execution result: {auto_result}")

    table_name = auto_result.get("table_name")
    if not table_name:
        pytest.fail(f"No table name returned in auto execution result: {auto_result}")

    expected_rows = _load_expected_rows()

    try:
        actual_rows = _fetch_table_rows(table_name, list(expected_rows[0].keys()))
        _compare_rows(actual_rows, expected_rows)
    finally:
        # Clean up the created table to avoid polluting subsequent tests.
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
