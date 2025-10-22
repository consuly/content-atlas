from typing import List, Dict, Any
import pandas as pd
import io
from .schemas import MappingConfig


def map_data(records: List[Dict[str, Any]], config: MappingConfig) -> List[Dict[str, Any]]:
    """Map input data according to the configuration."""
    mapped_records = []

    for record in records:
        mapped_record = {}
        for output_col, input_field in config.mappings.items():
            mapped_record[output_col] = record.get(input_field)

        # Apply rules if any
        if config.rules:
            mapped_record = apply_rules(mapped_record, config.rules)

        mapped_records.append(mapped_record)

    return mapped_records


def apply_rules(record: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    """Apply transformation rules to the record."""
    # For now, basic implementation
    # Can extend with more complex rules
    transformations = rules.get('transformations', [])

    for transformation in transformations:
        # Example: {"type": "uppercase", "field": "name"}
        if transformation.get('type') == 'uppercase':
            field = transformation.get('field')
            if field in record and record[field]:
                record[field] = record[field].upper()

    return record


def detect_column_type(series: pd.Series) -> str:
    """Detect the appropriate SQL type for a pandas Series."""
    # Try to infer type from sample values
    sample_values = series.dropna().head(100)  # Sample first 100 non-null values

    if len(sample_values) == 0:
        return "VARCHAR(255)"

    # Check if all values are numeric
    try:
        pd.to_numeric(sample_values, errors='raise')
        # Check if they are integers
        if all(pd.to_numeric(sample_values, errors='coerce').dropna() == sample_values.astype(int)):
            return "INTEGER"
        else:
            return "DECIMAL(10,2)"
    except (ValueError, TypeError):
        pass

    # Check if they look like dates
    try:
        pd.to_datetime(sample_values, errors='raise')
        return "TIMESTAMP"
    except (ValueError, TypeError):
        pass

    # Default to VARCHAR with reasonable length
    max_length = max(len(str(val)) for val in sample_values) if len(sample_values) > 0 else 255
    varchar_length = min(max(max_length * 2, 50), 1000)  # Min 50, max 1000, double the max length
    return f"VARCHAR({varchar_length})"


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

    # Detect schema types
    db_schema = {}
    mappings = {}

    for col in columns_found:
        # Clean column name for SQL
        clean_col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        if not clean_col[0].isalpha() and clean_col[0] != '_':
            clean_col = 'col_' + clean_col

        db_schema[clean_col] = detect_column_type(df[col])
        mappings[clean_col] = col  # Map clean name to original name

    return file_type, MappingConfig(
        table_name=table_name,
        db_schema=db_schema,
        mappings=mappings,
        rules={}
    ), columns_found, rows_sampled
