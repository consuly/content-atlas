import pytest

from app.api.schemas.shared import MappingConfig
from app.domain.imports.orchestrator import (
    _columns_cover_mapping,
    _maybe_reparse_generated_headerless_records,
)
from app.domain.imports.processors.csv_processor import process_csv


def _build_mapping(expected_sources):
    return MappingConfig(
        table_name="clients_list",
        db_schema={dst: "TEXT" for dst in expected_sources.keys()},
        mappings=expected_sources,
    )


def test_reparse_headerless_records_with_named_mapping():
    csv_bytes = b"Name,Email\nAlice,alice@example.com\nBob,bob@example.com\n"
    headerless_records = process_csv(csv_bytes, has_header=False)
    mapping = _build_mapping({"name": "Name", "email": "Email"})

    reparsed_records, reparsed = _maybe_reparse_generated_headerless_records(
        records=headerless_records,
        mapping_config=mapping,
        file_content=csv_bytes,
        csv_has_header=True,
    )

    assert reparsed is True
    assert _columns_cover_mapping(reparsed_records, mapping)
    assert set(reparsed_records[0].keys()) == {"Name", "Email"}


def test_skip_reparse_when_mapping_targets_generated_columns():
    csv_bytes = b"Name,Email\nAlice,alice@example.com\n"
    headerless_records = process_csv(csv_bytes, has_header=False)
    mapping = _build_mapping({"name": "col_0", "email": "col_1"})

    reparsed_records, reparsed = _maybe_reparse_generated_headerless_records(
        records=headerless_records,
        mapping_config=mapping,
        file_content=csv_bytes,
        csv_has_header=True,
    )

    assert reparsed is False
    assert reparsed_records == headerless_records
