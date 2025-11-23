import os

from app.domain.imports.processors.csv_processor import process_excel, process_large_excel


def _load_workbook_bytes() -> bytes:
    workbook_path = os.path.join("tests", "xlsx", "test-2-tabs.xlsx")
    with open(workbook_path, "rb") as handle:
        return handle.read()


def test_process_excel_defaults_to_first_sheet():
    records = process_excel(_load_workbook_bytes())

    assert len(records) == 20
    assert records[0]["sales oct"] is not None
    assert "sales nov" not in records[0]


def test_process_excel_respects_sheet_name():
    records = process_excel(_load_workbook_bytes(), sheet_name="Clients Nov")

    assert len(records) == 20
    assert records[0]["sales nov"] is not None
    assert "sales oct" not in records[0]


def test_process_large_excel_defaults_to_first_sheet():
    records = process_large_excel(_load_workbook_bytes())

    assert len(records) == 20
    assert records[0]["sales oct"] is not None
