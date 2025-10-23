import pandas as pd
from typing import List, Dict, Any
import io
from io import StringIO


def process_csv(file_content: bytes) -> List[Dict[str, Any]]:
    """Process CSV file and return list of dictionaries."""
    df = pd.read_csv(io.BytesIO(file_content))
    records = df.to_dict('records')

    # Convert pandas NaT values to None for database compatibility
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def process_excel(file_content: bytes) -> List[Dict[str, Any]]:
    """Process Excel file and return list of dictionaries."""
    # Try openpyxl first (works for both .xlsx and .xls in many cases)
    try:
        df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
    except Exception:
        # Fallback to default pandas engine
        try:
            df = pd.read_excel(io.BytesIO(file_content))
        except Exception as e:
            raise ValueError(f"Could not read Excel file: {str(e)}")

    records = df.to_dict('records')

    # Convert pandas NaT values to None for database compatibility
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def process_large_excel(file_content: bytes, chunk_size: int = 10000) -> List[Dict[str, Any]]:
    """
    Process large Excel files in chunks to avoid memory issues.

    Args:
        file_content: Excel file as bytes
        chunk_size: Number of rows to process at once

    Returns:
        List of dictionaries containing all processed records
    """
    try:
        # Use chunked reading for large files
        chunks = pd.read_excel(io.BytesIO(file_content), engine='openpyxl', chunksize=chunk_size)
        all_records = []

        for chunk in chunks:
            # Process each chunk
            records = chunk.to_dict('records')
            # Convert pandas NaT values to None for database compatibility
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            all_records.extend(records)

        return all_records

    except Exception as e:
        # Fallback to regular processing if chunking fails
        print(f"Chunked processing failed, falling back to regular processing: {str(e)}")
        return process_excel(file_content)


def extract_excel_sheets_to_csv(file_content: bytes, rows: int = 100) -> Dict[str, str]:
    """
    Extract top N rows from each sheet in Excel file and return as CSV strings.

    Args:
        file_content: Excel file as bytes
        rows: Number of rows to extract from each sheet (default 100)

    Returns:
        Dict with sheet names as keys and CSV strings as values
    """
    # Read all sheets
    try:
        sheets_dict = pd.read_excel(io.BytesIO(file_content), sheet_name=None, engine='openpyxl')
    except Exception:
        # Fallback to default pandas engine
        try:
            sheets_dict = pd.read_excel(io.BytesIO(file_content), sheet_name=None)
        except Exception as e:
            raise ValueError(f"Could not read Excel file: {str(e)}")

    result = {}
    for sheet_name, df in sheets_dict.items():
        # Take top N rows (or all if less than N)
        df_subset = df.head(rows)

        # Convert to CSV string
        csv_buffer = StringIO()
        df_subset.to_csv(csv_buffer, index=False)
        result[sheet_name] = csv_buffer.getvalue()

    return result
