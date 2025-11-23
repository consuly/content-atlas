import copy
import os
from datetime import datetime

import pytest

os.environ.setdefault("SKIP_DB_INIT", "1")

from app.integrations.auto_import import (
    coerce_records_to_expected_types,
    normalize_expected_type,
)


def test_normalize_expected_type_aliases():
    assert normalize_expected_type("numeric") == "DECIMAL"
    assert normalize_expected_type("Decimal") == "DECIMAL"
    assert normalize_expected_type("float") == "DECIMAL"
    assert normalize_expected_type("integer") == "INTEGER"
    assert normalize_expected_type("bigint") == "BIGINT"
    assert normalize_expected_type("date") == "DATE"
    assert normalize_expected_type("timestamp") == "TIMESTAMP"
    assert normalize_expected_type("boolean") == "BOOLEAN"
    assert normalize_expected_type("text") == "TEXT"
    assert normalize_expected_type("string") == "TEXT"
    assert normalize_expected_type("unknown-type") == "TEXT"
    assert normalize_expected_type(None) == "TEXT"
    assert normalize_expected_type("") == "TEXT"


def test_coerce_records_to_expected_types_round_trips():
    raw_records = [
        {"amount": "12.5", "created_at": "2024-04-01 12:45", "active": "true", "name": "Alice"},
        {"amount": "bad", "created_at": "not a date", "active": "NO", "name": "Bob"},
    ]
    expected_types = {
        "amount": "decimal",
        "created_at": "timestamp",
        "active": "boolean",
        "name": "text",
        "missing": "text",
    }

    # Keep a pristine copy for regression detection
    original_records = copy.deepcopy(raw_records)

    converted_records, summary = coerce_records_to_expected_types(raw_records, expected_types)

    # Ensure original records are untouched
    assert raw_records == original_records

    # Validate numeric coercion with graceful failure
    assert isinstance(converted_records[0]["amount"], float)
    assert pytest.approx(converted_records[0]["amount"], rel=1e-6) == 12.5
    assert converted_records[1]["amount"] is None
    assert summary["amount"]["expected_type"] == "DECIMAL"
    assert summary["amount"]["status"] == "converted"
    assert summary["amount"]["coerced_values"] == 1

    # Validate timestamp coercion and failure handling
    assert isinstance(converted_records[0]["created_at"], datetime)
    assert converted_records[0]["created_at"].year == 2024
    assert converted_records[1]["created_at"] is None
    assert summary["created_at"]["expected_type"] == "TIMESTAMP"
    assert summary["created_at"]["status"] == "converted"
    assert summary["created_at"]["coerced_values"] == 1

    # Validate boolean coercion
    assert converted_records[0]["active"] is True
    assert converted_records[1]["active"] is False
    assert summary["active"]["status"] == "converted"
    assert "coerced_values" not in summary["active"]

    # Text coercion should keep the data as str while preserving nullability
    assert converted_records[0]["name"] == "Alice"
    assert converted_records[1]["name"] == "Bob"
    assert summary["name"]["status"] == "converted"

    # Columns missing in the source should be reported
    assert summary["missing"]["status"] == "missing_source_column"


def test_coerce_records_to_bigint():
    raw_records = [{"id": "120000000000000000"}, {"id": "5"}]
    expected_types = {"id": "bigint"}

    converted, summary = coerce_records_to_expected_types(raw_records, expected_types)

    assert int(converted[0]["id"]) == 120000000000000000
    assert int(converted[1]["id"]) == 5
    assert summary["id"]["expected_type"] == "BIGINT"
    assert summary["id"]["status"] == "converted"
