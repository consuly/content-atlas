import io

import pandas as pd

from app.domain.imports.orchestrator import (
    _records_look_like_mappings,
    _count_file_rows,
)


def test_records_validator_accepts_dict_like_sequences():
    assert _records_look_like_mappings([{"a": 1}, {"b": 2}]) is True
    assert _records_look_like_mappings([None, {"c": 3}]) is True


def test_records_validator_rejects_strings_and_lists():
    assert _records_look_like_mappings(["row as string"]) is False
    assert _records_look_like_mappings([["cell_1", "cell_2"]]) is False
    assert _records_look_like_mappings("totally invalid") is False


def test_row_counter_detects_csv_header():
    content = "\n".join(
        [
            "name,age",
            "alice,30",
            "bob,25",
        ]
    ).encode("utf-8")

    result = _count_file_rows(content, "csv", header_present=None)

    assert result.total_rows == 3
    assert result.header_rows == 1
    assert result.data_rows == 2
    assert result.detected_header is True


def test_row_counter_respects_header_override_for_headerless_csv():
    content = "\n".join(
        [
            "1,2",
            "3,4",
        ]
    ).encode("utf-8")

    result = _count_file_rows(content, "csv", header_present=False)

    assert result.total_rows == 2
    assert result.header_rows == 0
    assert result.data_rows == 2
    assert result.detected_header is False


def test_row_counter_handles_excel_with_header_row():
    df = pd.DataFrame({"name": ["alice", "bob"], "age": [30, 25]})
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    content = buffer.getvalue()

    result = _count_file_rows(content, "excel")

    assert result.total_rows == 3  # header + two data rows
    assert result.header_rows == 1
    assert result.data_rows == 2
    assert result.detected_header is True
    assert result.header_row_index == 0
