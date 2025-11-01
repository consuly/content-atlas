import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
import io
from io import StringIO
import csv
import logging

logger = logging.getLogger(__name__)


def extract_raw_csv_rows(file_content: bytes, num_rows: int = 20) -> List[List[str]]:
    """
    Extract raw CSV rows without making any assumptions about headers.
    
    This function reads the CSV file as-is and returns the first N rows
    as lists of strings, preserving the exact structure of the file.
    This is useful for LLM analysis to determine if the file has headers
    and what the semantic meaning of each column is.
    
    Args:
        file_content: CSV file content as bytes
        num_rows: Number of rows to extract (default 20)
        
    Returns:
        List of rows, where each row is a list of string values
    """
    try:
        # Decode bytes to string
        text_content = file_content.decode('utf-8')
        
        # Use csv.reader to parse without assumptions
        csv_reader = csv.reader(StringIO(text_content))
        
        # Extract first N rows
        raw_rows = []
        for i, row in enumerate(csv_reader):
            if i >= num_rows:
                break
            raw_rows.append(row)
        
        logger.info(f"Extracted {len(raw_rows)} raw CSV rows for analysis")
        return raw_rows
        
    except Exception as e:
        logger.error(f"Error extracting raw CSV rows: {e}")
        return []


def detect_csv_header(file_content: bytes) -> bool:
    """
    Detect if a CSV file has a header row.
    
    Uses heuristics to determine if the first row is a header:
    - Headers typically contain only strings (no pure numbers)
    - Headers have consistent data types
    - Headers are often shorter than data rows
    
    Args:
        file_content: CSV file content as bytes
        
    Returns:
        True if header detected, False if first row appears to be data
    """
    try:
        # Read first 3 rows without assuming header
        df_sample = pd.read_csv(io.BytesIO(file_content), nrows=3, header=None)
        
        if len(df_sample) == 0:
            return True  # Empty file, assume header
        
        first_row = df_sample.iloc[0]
        
        # Check if first row contains only strings (typical for headers)
        all_strings = all(isinstance(val, str) for val in first_row)
        
        # Check if first row has no numeric values
        has_numbers = any(isinstance(val, (int, float)) and not isinstance(val, bool) 
                         for val in first_row)
        
        # If all strings and no numbers, likely a header
        if all_strings and not has_numbers:
            logger.info("CSV header detected: first row contains only strings")
            return True
        
        # If we have more rows, compare first row to second row
        if len(df_sample) > 1:
            second_row = df_sample.iloc[1]
            
            # Check if data types are different between rows
            # Headers typically have consistent string types, data rows vary
            first_types = [type(val).__name__ for val in first_row]
            second_types = [type(val).__name__ for val in second_row]
            
            # If first row is all strings but second row has mixed types, likely header
            if all(t == 'str' for t in first_types) and any(t != 'str' for t in second_types):
                logger.info("CSV header detected: first row is strings, second row has mixed types")
                return True
        
        # Default: assume no header if we can't determine
        logger.info("CSV appears to have no header: first row contains data-like values")
        return False
        
    except Exception as e:
        logger.warning(f"Error detecting CSV header: {e}, assuming header exists")
        return True  # Default to assuming header exists


def process_csv_headerless(file_content: bytes) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Process CSV file without headers and return records with generated column names.
    
    Args:
        file_content: CSV file content as bytes
        
    Returns:
        Tuple of (records, column_names) where column_names are generated (col_0, col_1, etc.)
    """
    # Read CSV without header
    df = pd.read_csv(io.BytesIO(file_content), header=None)
    
    # Generate column names: col_0, col_1, col_2, etc.
    column_names = [f"col_{i}" for i in range(len(df.columns))]
    df.columns = column_names
    
    logger.info(f"Processed headerless CSV with {len(df)} rows and {len(column_names)} columns")
    logger.info(f"Generated column names: {column_names}")
    
    records = df.to_dict('records')

    # Convert pandas NaT values to None for database compatibility
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records, column_names


def process_csv(file_content: bytes, has_header: Optional[bool] = None) -> List[Dict[str, Any]]:
    """
    Process CSV file and return list of dictionaries.
    
    Args:
        file_content: CSV file content as bytes
        has_header: If specified, use this value. If None, auto-detect header.
    
    Automatically detects if the file has a header row and processes accordingly.
    """
    # Use provided value or auto-detect if file has header
    if has_header is None:
        has_header = detect_csv_header(file_content)
    
    if has_header:
        # Standard processing with header
        df = pd.read_csv(io.BytesIO(file_content))
        logger.info(f"Processed CSV with header: {len(df)} rows, columns: {list(df.columns)}")
    else:
        # Process without header and use generated column names
        records, column_names = process_csv_headerless(file_content)
        logger.info(f"Processed CSV without header: {len(records)} rows, columns: {column_names}")
        return records
    
    records = df.to_dict('records')

    # Convert pandas NaT values to None for database compatibility
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def process_excel(file_content: bytes) -> List[Dict[str, Any]]:
    """Process Excel file and return list of dictionaries using optimized settings."""
    # Use openpyxl with read_only mode for better performance on large files
    try:
        # read_only=True significantly speeds up reading large Excel files
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


def process_large_excel(file_content: bytes, chunk_size: int = 20000) -> List[Dict[str, Any]]:
    """
    Process large Excel files using optimized pandas settings.
    
    Note: pandas read_excel doesn't support chunksize parameter like read_csv does,
    so we use optimized reading settings instead.

    Args:
        file_content: Excel file as bytes
        chunk_size: Not used for Excel (kept for API compatibility)

    Returns:
        List of dictionaries containing all processed records
    """
    try:
        # Use openpyxl engine with optimized settings for large files
        # Note: We read the entire file at once since Excel doesn't support chunked reading
        # but openpyxl is optimized for memory efficiency
        df = pd.read_excel(
            io.BytesIO(file_content), 
            engine='openpyxl'
        )
        
        records = df.to_dict('records')

        # Convert pandas NaT values to None for database compatibility
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        return records

    except Exception as e:
        # Fallback to regular processing if optimized reading fails
        print(f"Optimized Excel processing failed, falling back to regular processing: {str(e)}")
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
