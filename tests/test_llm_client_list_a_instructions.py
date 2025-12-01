import io
import os
import re
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text

from app.main import app
from app.db.session import get_engine
from app.core.config import settings


client = TestClient(app)
FIXTURE_DIR = Path("tests/csv")
# Use the richer client-list-a fixture so the LLM has multiple email fields to explode.
INPUT_FILE = FIXTURE_DIR / "client-list-a.csv"
SPECIAL_INSTRUCTION = (
    "If you have multiple emails in a row create a new entry for each email in only one column so we have only one email returned per row. So if there is 4 emails in a row it will generate 4 rows with each a different email but the same information around. Skip columns like IDs value or status from a previous export and keep all columns that define the client contact like URL, and position."
)


def _normalize_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def _fetch_table_rows(table_name: str) -> list[dict]:
    engine = get_engine()
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        pytest.fail(f"Mapped table '{table_name}' not found after auto execution.")

    query = text(f'SELECT * FROM "{table_name}"')

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    return [dict(row._mapping) for row in rows]


def _extract_email_column(rows: list[dict]) -> str:
    if not rows:
        pytest.fail("Mapped table returned no rows; expected email-split output.")

    normalized_columns = {_normalize_key(col): col for col in rows[0].keys()}
    email_column = normalized_columns.get("email") or normalized_columns.get("emailaddress")

    if not email_column:
        pytest.fail(
            "Email column not found in mapped table. "
            f"Available columns: {sorted(rows[0].keys())}"
        )

    return email_column


def _reset_clients_list_state(table_name: str):
    """Ensure a clean slate so the LLM treats this as a first-time mapping."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        for table in ("import_history", "mapping_errors", "table_metadata", "import_duplicates"):
            try:
                conn.execute(text(f"DELETE FROM {table} WHERE table_name = '{table_name}'"))
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
def test_llm_mapping_with_email_explode_instruction(require_llm):
    """
    Ensure the LLM honors the explode-email instruction and returns rows with a single email each.
    """
    target_table_name = f"clients_list_{uuid.uuid4().hex}"
    _reset_clients_list_state(target_table_name)

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
            "target_table_name": target_table_name,
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

    table_name = auto_result.get("table_name") or target_table_name
    if not table_name:
        pytest.fail(f"No table name returned in auto execution result: {auto_result}")

    try:
        actual_rows = _fetch_table_rows(table_name)
        email_column = _extract_email_column(actual_rows)

        if len(actual_rows) != 78:
            pytest.fail(f"Expected 78 rows after email explosion, got {len(actual_rows)}")

        for idx, row in enumerate(actual_rows, start=1):
            email_value = (row.get(email_column) or "").strip()
            if not email_value:
                pytest.fail(f"Row {idx} missing email value: {row}")

            multiple_delimiters = any(sep in email_value for sep in (",", ";", "|"))
            multiple_at_symbols = len(re.findall(r"@", email_value)) != 1
            if multiple_delimiters or multiple_at_symbols:
                pytest.fail(
                    "Row contains multiple emails; expected a single email per row. "
                    f"Row {idx}: {email_value}"
                )
    finally:
        _reset_clients_list_state(table_name)
