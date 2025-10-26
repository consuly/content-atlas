from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import io
import logging
from datetime import datetime
from .schemas import MappingConfig

logger = logging.getLogger(__name__)


def map_data(records: List[Dict[str, Any]], config: MappingConfig) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Map input data according to the configuration.

    Returns:
        Tuple of (mapped_records, list_of_all_errors)
    """
    mapped_records = []
    all_errors = []

    for record in records:
        mapped_record = {}
        for output_col, input_field in config.mappings.items():
            mapped_record[output_col] = record.get(input_field)

        # Apply rules if any
        if config.rules:
            mapped_record, record_errors = apply_rules(mapped_record, config.rules)
            all_errors.extend(record_errors)

        mapped_records.append(mapped_record)

    return mapped_records, all_errors


def apply_rules(record: Dict[str, Any], rules: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply transformation rules to the record.

    Returns:
        Tuple of (transformed_record, list_of_errors)
    """
    errors = []
    transformations = rules.get('transformations', [])
    datetime_transformations = rules.get('datetime_transformations', [])

    # Apply general transformations
    for transformation in transformations:
        # Example: {"type": "uppercase", "field": "name"}
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in record and record[field]:
                record[field] = record[field].upper()

    # Apply datetime transformations
    for dt_transformation in datetime_transformations:
        field = dt_transformation.get('field')
        source_format = dt_transformation.get('source_format')
        target_format = dt_transformation.get('target_format', 'ISO8601')

        if field in record:
            original_value = record[field]
            standardized_value = standardize_datetime(original_value, source_format)

            if standardized_value is None and original_value is not None and str(original_value).strip():
                # Conversion failed for a non-empty value
                error_msg = f"Failed to convert datetime field '{field}' with value '{original_value}'"
                errors.append(error_msg)
                logger.warning(error_msg)

            # Always update the record (None for failed conversions, standardized value for success)
            record[field] = standardized_value

    return record, errors


def standardize_datetime(value: Any, source_format: Optional[str] = None) -> Optional[str]:
    """
    Standardize datetime values to ISO 8601 format.

    Args:
        value: The datetime value to standardize
        source_format: Optional strftime format string (e.g., '%m/%d/%Y %I:%M %p')
                      If None, pandas will attempt to infer the format

    Returns:
        ISO 8601 formatted string or None if conversion fails or value is empty
    """
    # Handle NULL, empty, or whitespace-only values
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None

    try:
        # Convert to string if not already
        if not isinstance(value, str):
            value = str(value)

        # Parse datetime using specified format or auto-detection
        if source_format and source_format != "auto":
            # Try explicit format first
            dt = pd.to_datetime(value, format=source_format, errors='coerce')
            # If explicit format fails, try auto-detection as fallback
            if pd.isna(dt):
                dt = pd.to_datetime(value, errors='coerce')
        else:
            # Auto-detect format
            dt = pd.to_datetime(value, errors='coerce')

        # Check if parsing was successful
        if pd.isna(dt):
            logger.warning(f"Failed to parse datetime value: '{value}'")
            return None

        # Convert to ISO 8601 format
        # For date-only values, return just the date part
        # For datetime values, return full ISO format
        if dt.time() == datetime.min.time():
            # Date only (no time component)
            return dt.strftime('%Y-%m-%d')
        else:
            # Date and time
            return dt.isoformat()

    except Exception as e:
        logger.warning(f"Error standardizing datetime '{value}': {str(e)}")
        return None


def detect_column_type(series: pd.Series, has_datetime_transformation: bool = False) -> str:
    """Detect the appropriate SQL type for a pandas Series with conservative approach for data consolidation."""
    # Try to infer type from sample values
    sample_values = series.dropna().head(100)  # Sample first 100 non-null values

    if len(sample_values) == 0:
        return "TEXT"

    # Check if pandas has already detected this as datetime (e.g., from Excel date serial numbers)
    if pd.api.types.is_datetime64_any_dtype(series):
        # Series is already datetime-like, so it should be TIMESTAMP
        return "TIMESTAMP"

    # Check if all values are numeric - use DECIMAL for all numeric data for maximum flexibility
    try:
        pd.to_numeric(sample_values, errors='raise')
        # Use DECIMAL for all numeric data to handle mixed formats (integers, floats, etc.)
        # This enables easy data consolidation and merging across different datasets
        return "DECIMAL"
    except (ValueError, TypeError):
        pass

    # Check if they look like dates - use TEXT for flexibility in date formats
    try:
        pd.to_datetime(sample_values, errors='raise', format='mixed')

        # If datetime transformation rules exist, test if conversion works
        if has_datetime_transformation:
            # Test conversion on sample values
            conversion_success = True
            for val in sample_values.head(10):  # Test first 10 values
                if standardize_datetime(val) is None and pd.notna(val):
                    conversion_success = False
                    break

            if conversion_success:
                return "TIMESTAMP"
            else:
                return "TEXT"  # Fallback when conversion uncertain
        else:
            return "TEXT"  # Use TEXT instead of TIMESTAMP for format flexibility

    except (ValueError, TypeError):
        pass

    # Use TEXT for all string columns to avoid length issues
    return "TEXT"


def detect_mapping_from_file(file_content: bytes, file_name: str) -> tuple[str, MappingConfig, List[str], int]:
    """
    Detect mapping configuration from CSV or Excel file content.

    Returns:
        tuple: (file_type, mapping_config, columns_found, rows_sampled)
    """
    # Detect file type
    if file_name.endswith('.csv'):
        file_type = 'csv'
        df = pd.read_csv(io.BytesIO(file_content))
    elif file_name.endswith(('.xlsx', '.xls')):
        file_type = 'excel'
        # Try openpyxl first (works for both .xlsx and .xls in many cases)
        try:
            df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
        except Exception:
            # Fallback to default pandas engine
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception as e:
                raise ValueError(f"Could not read Excel file: {str(e)}")
    else:
        raise ValueError("Unsupported file type. Only CSV and Excel files are supported.")

    # Get column names
    columns_found = df.columns.tolist()
    rows_sampled = len(df)

    # Generate table name from filename (remove extension and sanitize)
    table_name = file_name.split('.')[0].replace('-', '_').replace(' ', '_').lower()
    # Ensure it's a valid SQL identifier
    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '', table_name)
    if not table_name:
        table_name = 'auto_detected_table'
    if not table_name[0].isalpha():
        table_name = 'table_' + table_name

    # Add timestamp to make table name unique for testing
    import time
    table_name = f"{table_name}_{int(time.time())}"

    # Detect schema types
    db_schema = {}
    mappings = {}

    # Check if datetime transformations are defined (this would be passed in from LLM/user)
    # For now, we'll assume no transformations are defined during auto-detection
    # The LLM integration point would be to ask for format when datetime is detected
    has_datetime_transformations = False

    for col in columns_found:
        # Clean column name for SQL
        clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        if not clean_col[0].isalpha() and clean_col[0] != '_':
            clean_col = 'col_' + clean_col

        db_schema[clean_col] = detect_column_type(df[col], has_datetime_transformations)
        mappings[clean_col] = col  # Map clean name to original name

    return file_type, MappingConfig(
        table_name=table_name,
        db_schema=db_schema,
        mappings=mappings,
        rules={}
    ), columns_found, rows_sampled
