from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import io
import logging
from datetime import datetime
from .schemas import MappingConfig
from .date_utils import parse_flexible_date, detect_date_column

logger = logging.getLogger(__name__)


def map_data(records: List[Dict[str, Any]], config: MappingConfig) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Map input data according to the configuration.
    
    Optimized for performance:
    - Pre-computes mapping items once (not per record)
    - Uses list comprehension for fast path (no rules)
    - Minimizes dictionary operations
    - Automatically converts date columns based on schema type

    Returns:
        Tuple of (mapped_records, list_of_all_errors)
    """
    all_errors = []
    
    # Pre-compute mapping items ONCE (not N times in loop)
    # Convert to tuple for faster iteration
    mapping_items = tuple(config.mappings.items())
    
    # Identify date/timestamp columns from schema for automatic conversion
    date_columns = set()
    if config.db_schema:
        for col_name, col_type in config.db_schema.items():
            if col_type and ('TIMESTAMP' in col_type.upper() or 'DATE' in col_type.upper()):
                date_columns.add(col_name)
    
    # Check if we need to apply rules or date conversions
    has_rules = bool(config.rules)
    has_date_columns = bool(date_columns)
    
    # Fast path: no rules and no date columns to convert
    if not has_rules and not has_date_columns:
        # Use list comprehension - significantly faster than append loop
        mapped_records = [
            {output_col: record.get(input_field) 
             for output_col, input_field in mapping_items}
            for record in records
        ]
        return mapped_records, all_errors
    
    # Process records with rules and/or date conversion
    mapped_records = []
    for record in records:
        # Use dict comprehension for mapping (faster than loop with assignment)
        mapped_record = {output_col: record.get(input_field) 
                        for output_col, input_field in mapping_items}
        
        # Apply automatic date conversion for TIMESTAMP/DATE columns
        if has_date_columns:
            for col_name in date_columns:
                if col_name in mapped_record:
                    original_value = mapped_record[col_name]
                    if original_value is not None:
                        # Defensive check: skip obviously non-date values
                        value_str = str(original_value).strip()
                        
                        # Skip if value contains email pattern
                        if '@' in value_str:
                            logger.info(f"Skipping date conversion for '{col_name}': value '{value_str}' appears to be an email")
                            mapped_record[col_name] = original_value
                            continue
                        
                        # Skip if value looks like a name (single word with capital letter, no numbers)
                        if value_str and value_str[0].isupper() and value_str.isalpha() and len(value_str) < 30:
                            logger.info(f"Skipping date conversion for '{col_name}': value '{value_str}' appears to be a name")
                            mapped_record[col_name] = original_value
                            continue
                        
                        # Try to convert the date
                        converted_value = parse_flexible_date(original_value)
                        if converted_value is None and value_str:
                            # Conversion failed for non-empty value
                            error_msg = f"Failed to convert datetime field '{col_name}' with value '{original_value}'"
                            all_errors.append(error_msg)
                            logger.warning(error_msg)
                        mapped_record[col_name] = converted_value
        
        # Apply rules if present
        if has_rules:
            mapped_record, record_errors = apply_rules(mapped_record, config.rules)
            all_errors.extend(record_errors)
        
        mapped_records.append(mapped_record)

    return mapped_records, all_errors


def apply_rules_vectorized(df: pd.DataFrame, rules: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Apply transformation rules to the DataFrame using vectorized operations.
    Much faster than row-by-row processing.

    Returns:
        Tuple of (transformed_dataframe, list_of_errors)
    """
    errors = []
    transformations = rules.get('transformations', [])
    datetime_transformations = rules.get('datetime_transformations', [])

    # Apply general transformations (vectorized)
    for transformation in transformations:
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in df.columns:
                # Vectorized uppercase operation
                df[field] = df[field].astype(str).str.upper()

    # Apply datetime transformations (vectorized)
    for dt_transformation in datetime_transformations:
        field = dt_transformation.get('field')
        source_format = dt_transformation.get('source_format')
        
        if field in df.columns:
            # Vectorized datetime conversion
            if source_format and source_format != "auto":
                # Try explicit format first
                converted = pd.to_datetime(df[field], format=source_format, errors='coerce')
                # If many failed, try auto-detection as fallback
                if converted.isna().sum() > len(df) * 0.5:  # If >50% failed
                    converted = pd.to_datetime(df[field], errors='coerce')
            else:
                # Auto-detect format
                converted = pd.to_datetime(df[field], errors='coerce')
            
            # Count conversion failures for non-empty values
            original_non_empty = df[field].notna() & (df[field].astype(str).str.strip() != '')
            conversion_failed = original_non_empty & converted.isna()
            failed_count = conversion_failed.sum()
            
            if failed_count > 0:
                error_msg = f"Failed to convert {failed_count} datetime values in field '{field}'"
                errors.append(error_msg)
                logger.warning(error_msg)
            
            # Format to ISO 8601
            # For date-only values, use date format; for datetime, use full ISO format
            df[field] = converted.apply(lambda x: 
                x.strftime('%Y-%m-%d') if pd.notna(x) and x.time() == datetime.min.time()
                else x.isoformat() if pd.notna(x)
                else None
            )

    return df, errors


def apply_rules(record: Dict[str, Any], rules: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply transformation rules to the record (legacy row-by-row version).

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
    Standardize datetime values to ISO 8601 format using flexible date parsing.

    Args:
        value: The datetime value to standardize
        source_format: Optional strftime format string (e.g., '%m/%d/%Y %I:%M %p')
                      If None, will attempt to infer the format

    Returns:
        ISO 8601 formatted string (YYYY-MM-DDTHH:MM:SSZ) or None if conversion fails or value is empty
    """
    # Use the new flexible date parser which handles multiple formats
    # and always returns ISO 8601 with timezone
    return parse_flexible_date(value)


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

    # Convert to string for pattern matching
    sample_str = sample_values.astype(str)
    
    # Check for phone number patterns (must be TEXT, not NUMERIC)
    # Common formats: 415.610.7325, 415-610-7325, (415) 610-7325, etc.
    import re
    phone_patterns = [
        r'^\d{3}\.\d{3}\.\d{4}$',  # 415.610.7325
        r'^\d{3}-\d{3}-\d{4}$',    # 415-610-7325
        r'^\(\d{3}\)\s*\d{3}-\d{4}$',  # (415) 610-7325
        r'^\d{3}\s+\d{3}\s+\d{4}$',  # 415 610 7325
        r'^\+?\d{1,3}[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',  # International formats
    ]
    for pattern in phone_patterns:
        if sample_str.str.match(pattern, na=False).any():
            return "TEXT"
    
    # Check for percentage values (e.g., "98%", "2%")
    if sample_str.str.contains('%', regex=False, na=False).any():
        return "TEXT"
    
    # Check for email patterns
    if sample_str.str.contains('@', regex=False, na=False).any():
        return "TEXT"

    # Check if all values are numeric - use DECIMAL for all numeric data for maximum flexibility
    try:
        pd.to_numeric(sample_values, errors='raise')
        # Use DECIMAL for all numeric data to handle mixed formats (integers, floats, etc.)
        # This enables easy data consolidation and merging across different datasets
        return "DECIMAL"
    except (ValueError, TypeError):
        pass

    # Check if they look like dates
    try:
        pd.to_datetime(sample_values, errors='raise', format='mixed')

        # Test if our flexible date parser can handle these values
        conversion_success = True
        for val in sample_values.head(10):  # Test first 10 values
            if parse_flexible_date(val) is None and pd.notna(val):
                conversion_success = False
                break

        if conversion_success:
            # Our date parser can handle these values, use TIMESTAMP
            # This enables automatic date conversion during mapping
            return "TIMESTAMP"
        else:
            # Date parsing uncertain, use TEXT for safety
            return "TEXT"

    except (ValueError, TypeError):
        pass

    # Use TEXT for all string columns to avoid length issues
    return "TEXT"


def detect_mapping_from_file(file_content: bytes, file_name: str, return_records: bool = False, has_header: Optional[bool] = None) -> tuple[str, MappingConfig, List[str], int, Optional[List[Dict[str, Any]]]]:
    """
    Detect mapping configuration from CSV or Excel file content.
    
    Args:
        file_content: Raw file content
        file_name: Name of the file
        return_records: If True, also return the parsed records to avoid re-parsing
        has_header: For CSV files, explicitly specify if file has headers (None = auto-detect)
    
    Returns:
        tuple: (file_type, mapping_config, columns_found, rows_sampled, records)
        Note: records will be None if return_records=False
    """
    # Detect file type
    if file_name.endswith('.csv'):
        file_type = 'csv'
        # Use has_header parameter if provided, otherwise pandas will auto-detect
        if has_header is None:
            df = pd.read_csv(io.BytesIO(file_content))
        elif has_header:
            df = pd.read_csv(io.BytesIO(file_content), header=0)
        else:
            # No header - read without header and generate column names
            df = pd.read_csv(io.BytesIO(file_content), header=None)
            df.columns = [f'col_{i}' for i in range(len(df.columns))]
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

    # Convert DataFrame to records if requested (to avoid re-parsing later)
    records = None
    if return_records:
        records = df.to_dict('records')
        # Convert pandas NaT values to None for database compatibility
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
    
    return file_type, MappingConfig(
        table_name=table_name,
        db_schema=db_schema,
        mappings=mappings,
        rules={}
    ), columns_found, rows_sampled, records
