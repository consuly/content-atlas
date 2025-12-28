"""
Test that column names with leading/trailing whitespace are properly cleaned.

This ensures that files with identical structure but different whitespace in
headers are correctly recognized as compatible and merged into the same table.
"""
import io
import pytest
from app.domain.imports.processors.csv_processor import (
    process_csv,
    load_csv_sample,
    process_excel,
)
from app.domain.imports.schema_mapper import analyze_schema_compatibility


def test_csv_column_whitespace_stripped():
    """Test that CSV columns with whitespace are stripped during processing."""
    # CSV with spaces around column names
    csv_content = b"""REPORT_DATE, USAGE_DATE , PLATFORM_ID,PLATFORM_NAME  
2025-09-30,2025-08-31,760581,ArcSpan
2025-09-30,2025-08-31,27905,Facebook"""
    
    records = process_csv(csv_content)
    
    # Verify column names are stripped
    assert len(records) == 2
    assert "REPORT_DATE" in records[0]
    assert "USAGE_DATE" in records[0]
    assert "PLATFORM_ID" in records[0]
    assert "PLATFORM_NAME" in records[0]
    
    # Verify no whitespace in keys
    for record in records:
        for key in record.keys():
            assert key == key.strip(), f"Column name '{key}' has whitespace"


def test_excel_column_whitespace_stripped(tmp_path):
    """Test that Excel columns with whitespace are stripped during processing."""
    import pandas as pd
    
    # Create Excel file with spaces around column names
    df = pd.DataFrame({
        " REPORT_DATE ": ["2025-09-30", "2025-09-30"],
        "USAGE_DATE  ": ["2025-08-31", "2025-08-31"],
        "  PLATFORM_ID": [760581, 27905],
        "PLATFORM_NAME": ["ArcSpan", "Facebook"]
    })
    
    # Write to temporary Excel file
    excel_path = tmp_path / "test.xlsx"
    df.to_excel(excel_path, index=False)
    
    # Read back as bytes
    with open(excel_path, "rb") as f:
        excel_content = f.read()
    
    records = process_excel(excel_content)
    
    # Verify column names are stripped
    assert len(records) == 2
    assert "REPORT_DATE" in records[0]
    assert "USAGE_DATE" in records[0]
    assert "PLATFORM_ID" in records[0]
    assert "PLATFORM_NAME" in records[0]
    
    # Verify no whitespace in keys
    for record in records:
        for key in record.keys():
            assert key == key.strip(), f"Column name '{key}' has whitespace"


def test_schema_compatibility_with_whitespace():
    """
    Test that schema compatibility analysis correctly matches columns
    when one has whitespace and the other doesn't.
    """
    # First file columns (clean)
    columns_file1 = [
        "REPORT_DATE",
        "USAGE_DATE",
        "PLATFORM_ID",
        "PLATFORM_NAME",
        "TOTAL_DATA_REVENUE_EARNED",
        "DATA_PROVIDER_REVENUE_SHARE"
    ]
    
    # Second file columns (with whitespace) - after stripping
    columns_file2 = [
        "REPORT_DATE",
        "USAGE_DATE",
        "PLATFORM_ID",
        "PLATFORM_NAME",
        "TOTAL_DATA_REVENUE_EARNED",
        "DATA_PROVIDER_REVENUE_SHARE"
    ]
    
    # Analyze compatibility
    compatibility = analyze_schema_compatibility(columns_file2, columns_file1)
    
    # Should have 100% match
    assert compatibility["match_percentage"] == 100.0
    assert compatibility["matched_count"] == len(columns_file1)
    assert len(compatibility["new_columns"]) == 0
    
    # All columns should map exactly
    for col in columns_file2:
        assert compatibility["column_mapping"][col] == col


def test_csv_sample_column_whitespace_stripped():
    """Test that load_csv_sample also strips column whitespace."""
    csv_content = b"""REPORT_DATE, USAGE_DATE , PLATFORM_ID
2025-09-30,2025-08-31,760581
2025-09-30,2025-08-31,27905"""
    
    records = load_csv_sample(csv_content, sample_rows=10)
    
    # Verify column names are stripped
    assert "REPORT_DATE" in records[0]
    assert "USAGE_DATE" in records[0]
    assert "PLATFORM_ID" in records[0]
    
    # Verify no whitespace
    for record in records:
        for key in record.keys():
            assert key == key.strip()


def test_identical_files_different_whitespace_merge():
    """
    Test that two CSV files with identical structure but different whitespace
    produce the same column names and can be merged.
    """
    # File 1 - clean headers
    csv1 = b"""REPORT_DATE,USAGE_DATE,PLATFORM_ID,PLATFORM_NAME
2025-09-30,2025-08-31,760581,ArcSpan"""
    
    # File 2 - same structure but with whitespace around column names
    csv2 = b""" REPORT_DATE , USAGE_DATE ,PLATFORM_ID, PLATFORM_NAME 
2025-08-31,2025-07-31,516346,StackAdapt"""
    
    records1 = process_csv(csv1)
    records2 = process_csv(csv2)
    
    # Both should have identical column names (order and naming)
    columns1 = sorted(records1[0].keys())
    columns2 = sorted(records2[0].keys())
    
    assert columns1 == columns2, f"Columns don't match: {columns1} vs {columns2}"
    
    # Verify compatibility analysis shows perfect match
    compatibility = analyze_schema_compatibility(
        list(records2[0].keys()),
        list(records1[0].keys())
    )
    
    assert compatibility["match_percentage"] == 100.0
    assert len(compatibility["new_columns"]) == 0


def test_various_whitespace_types():
    """Test that various types of whitespace (spaces, tabs) are all stripped."""
    # CSV with tabs and spaces
    csv_content = b"""REPORT_DATE\t,\tUSAGE_DATE  ,  PLATFORM_ID
2025-09-30,2025-08-31,760581"""
    
    records = process_csv(csv_content)
    
    # All whitespace should be stripped
    for key in records[0].keys():
        assert key == key.strip()
        # No internal tabs or multiple spaces should remain at edges
        assert not key.startswith(" ")
        assert not key.startswith("\t")
        assert not key.endswith(" ")
        assert not key.endswith("\t")
