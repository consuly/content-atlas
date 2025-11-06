import hashlib
import json
from sqlalchemy import text, MetaData
from sqlalchemy.engine import Engine
from typing import List, Dict, Any, Tuple, Optional
from decimal import Decimal, InvalidOperation
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from app.api.schemas.shared import MappingConfig

logger = logging.getLogger(__name__)

# System column names that are reserved and cannot be used by user data
SYSTEM_COLUMNS = {
    '_row_id',
    '_import_id',
    '_imported_at',
    '_source_row_number',
    '_corrections_applied'
}


class DuplicateDataException(Exception):
    """Exception raised when duplicate data is detected during upload."""

    def __init__(self, table_name: str, duplicates_found: int, message: str = None):
        self.table_name = table_name
        self.duplicates_found = duplicates_found
        self.message = message or f"Duplicate data detected in table '{table_name}'. {duplicates_found} overlapping records found."
        super().__init__(self.message)


class FileAlreadyImportedException(Exception):
    """Exception raised when the same file has already been imported."""

    def __init__(self, file_hash: str, table_name: str, message: str = None):
        self.file_hash = file_hash
        self.table_name = table_name
        self.message = message or f"File has already been imported to table '{table_name}'."
        super().__init__(self.message)


def sanitize_column_names(db_schema: Dict[str, str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Sanitize user column names to avoid conflicts with system columns.
    
    Args:
        db_schema: Dictionary mapping column names to SQL types
        
    Returns:
        Tuple of (sanitized_schema, rename_mapping)
        - sanitized_schema: New schema with renamed columns
        - rename_mapping: Dict mapping original names to new names
    """
    sanitized_schema = {}
    rename_mapping = {}
    
    for col_name, col_type in db_schema.items():
        if col_name in SYSTEM_COLUMNS:
            # Remove leading underscore and add "source_" prefix
            new_name = f"source_{col_name.lstrip('_')}"
            sanitized_schema[new_name] = col_type
            rename_mapping[col_name] = new_name
            logger.warning(f"Column '{col_name}' conflicts with system column, renamed to '{new_name}'")
        else:
            sanitized_schema[col_name] = col_type
    
    return sanitized_schema, rename_mapping


def create_file_imports_table_if_not_exists(engine: Engine):
    """Create file_imports table to track imported files."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS file_imports (
        id SERIAL PRIMARY KEY,
        file_hash VARCHAR(64) UNIQUE NOT NULL,
        file_name VARCHAR(500),
        table_name VARCHAR(255) NOT NULL,
        imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        record_count INTEGER,
        UNIQUE(file_hash, table_name)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))


def create_table_if_not_exists(engine: Engine, config: MappingConfig):
    """Create table based on schema if it doesn't exist, or recreate if schema doesn't match."""
    table_name = config.table_name

    with engine.begin() as conn:
        # Check if table exists and get its current schema
        table_exists_result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": table_name})

        table_exists = table_exists_result.fetchone() is not None
        print(f"DEBUG: create_table_if_not_exists: Table '{table_name}' exists: {table_exists}")

        if table_exists:
            # Get current column types (excluding metadata columns for comparison)
            current_columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'
                AND column_name NOT LIKE '\\_%'
                ORDER BY column_name
            """), {"table_name": table_name})

            current_schema = {row[0]: row[1].upper() for row in current_columns_result}
            print(f"DEBUG: create_table_if_not_exists: Current schema: {current_schema}")

            # Check if schema matches - normalize both schemas for comparison
            # PostgreSQL stores VARCHAR as 'character varying', so we need to normalize
            def normalize_type(sql_type: str) -> str:
                """Normalize SQL type for comparison."""
                sql_type_upper = sql_type.upper()
                # Normalize VARCHAR variations
                if 'CHARACTER VARYING' in sql_type_upper or 'VARCHAR' in sql_type_upper:
                    return 'VARCHAR'
                return sql_type_upper
            
            current_schema_normalized = {col: normalize_type(col_type) for col, col_type in current_schema.items()}
            expected_schema = {col: normalize_type(col_type) for col, col_type in config.db_schema.items()}
            
            print(f"DEBUG: create_table_if_not_exists: Current schema (normalized): {current_schema_normalized}")
            print(f"DEBUG: create_table_if_not_exists: Expected schema (normalized): {expected_schema}")

            if current_schema_normalized != expected_schema:
                print(f"DEBUG: create_table_if_not_exists: Schema mismatch, dropping and recreating table")
                conn.execute(text(f'DROP TABLE "{table_name}" CASCADE'))
                table_exists = False
            else:
                print(f"DEBUG: create_table_if_not_exists: Schema matches, keeping existing table")

        if not table_exists:
            print(f"DEBUG: create_table_if_not_exists: Creating new table '{table_name}'")
            
            # Sanitize column names to avoid conflicts with system columns
            sanitized_schema, rename_mapping = sanitize_column_names(config.db_schema)
            
            if rename_mapping:
                print(f"DEBUG: create_table_if_not_exists: Renamed columns to avoid conflicts: {rename_mapping}")
            
            # Create table with correct schema
            columns = []
            for col_name, col_type in sanitized_schema.items():
                columns.append(f'"{col_name}" {col_type}')

            columns_sql = ', '.join(columns)

            # Add metadata columns for import tracking
            # These columns enable undo/rollback and change review functionality
            create_sql = f"""
            CREATE TABLE "{table_name}" (
                _row_id SERIAL PRIMARY KEY,
                {columns_sql},
                _import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE,
                _imported_at TIMESTAMP DEFAULT NOW(),
                _source_row_number INTEGER,
                _corrections_applied JSONB
            );
            
            -- Create index on import_id for efficient queries
            CREATE INDEX idx_{table_name}_import_id ON "{table_name}"(_import_id);
            """

            conn.execute(text(create_sql))
            print(f"DEBUG: create_table_if_not_exists: Table '{table_name}' created successfully with metadata columns")


def calculate_file_hash(file_content: bytes) -> str:
    """Calculate SHA-256 hash of file content for duplicate detection."""
    return hashlib.sha256(file_content).hexdigest()


def check_file_already_imported(engine: Engine, file_hash: str, table_name: str) -> bool:
    """Check if file with given hash was already imported to the table."""
    check_sql = """
    SELECT COUNT(*) FROM file_imports
    WHERE file_hash = :file_hash AND table_name = :table_name
    """

    with engine.connect() as conn:
        result = conn.execute(text(check_sql), {"file_hash": file_hash, "table_name": table_name})
        count = result.scalar()
        return count > 0


def record_file_import(engine: Engine, file_hash: str, file_name: str, table_name: str, record_count: int):
    """Record that a file has been imported."""
    insert_sql = """
    INSERT INTO file_imports (file_hash, file_name, table_name, record_count)
    VALUES (:file_hash, :file_name, :table_name, :record_count)
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql), {
            "file_hash": file_hash,
            "file_name": file_name,
            "table_name": table_name,
            "record_count": record_count
        })


def coerce_value_for_sql_type(value: Any, sql_type: str) -> Any:
    """
    Coerce a value to match the expected SQL type for database insertion.

    Args:
        value: The value to coerce
        sql_type: The SQL type (e.g., 'INTEGER', 'DECIMAL', 'TEXT', 'TIMESTAMP')

    Returns:
        The coerced value, or None if the value should be NULL
    """
    # Handle None/NULL values
    if value is None:
        return None

    # Handle NaN values from pandas
    if isinstance(value, float) and math.isnan(value):
        return None

    # Handle empty strings for numeric types
    if isinstance(value, str) and value.strip() == "":
        return None

    # Convert based on SQL type
    sql_type_upper = sql_type.upper()

    if 'INTEGER' in sql_type_upper:
        # Convert floats to integers (e.g., 507.0 -> 507)
        if isinstance(value, float):
            return int(value)
        elif isinstance(value, str):
            # Handle string representations of floats (e.g., "507.0" -> 507)
            try:
                float_val = float(value)
                return int(float_val)
            except (ValueError, TypeError):
                return None
        else:
            try:
                return int(value)
            except (ValueError, TypeError):
                return None

    elif 'DECIMAL' in sql_type_upper or 'NUMERIC' in sql_type_upper:
        if isinstance(value, bool):
            return int(value)

        decimal_value: Optional[Decimal] = None

        if isinstance(value, Decimal):
            decimal_value = value
        elif isinstance(value, int):
            return value
        elif isinstance(value, float):
            if math.isnan(value):
                return None
            decimal_value = Decimal(str(value))
        elif isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return None
            normalized = normalized.replace(',', '')
            if normalized.startswith('$'):
                normalized = normalized[1:]
            if normalized.startswith('(') and normalized.endswith(')'):
                normalized = f"-{normalized[1:-1]}"
            try:
                decimal_value = Decimal(normalized)
            except InvalidOperation:
                return None
        else:
            try:
                decimal_value = Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                return None

        if decimal_value is None:
            return None

        if decimal_value == decimal_value.to_integral():
            return int(decimal_value)

        return decimal_value

    elif 'TEXT' in sql_type_upper or 'VARCHAR' in sql_type_upper or 'CHAR' in sql_type_upper:
        # Convert to string
        return str(value)

    elif 'TIMESTAMP' in sql_type_upper or 'DATE' in sql_type_upper:
        # Keep as-is (should already be standardized by mapper.py)
        return value

    elif 'BOOLEAN' in sql_type_upper or 'BOOL' in sql_type_upper:
        # Convert to boolean
        if isinstance(value, str):
            lower_val = value.lower().strip()
            if lower_val in ('true', '1', 'yes', 'y'):
                return True
            elif lower_val in ('false', '0', 'no', 'n'):
                return False
            else:
                return None
        else:
            return bool(value)

    else:
        # For unknown types, convert to string as fallback
        return str(value)


def _check_for_duplicates_db_side(conn, table_name: str, records: List[Dict[str, Any]], config: MappingConfig) -> Tuple[List[int], int]:
    """
    Check for duplicate records using database-side queries and return indices of duplicates.
    Much faster than loading all data into pandas for large existing datasets.
    
    Returns:
        Tuple of (duplicate_indices, total_duplicates_count)
        - duplicate_indices: List of indices in the records list that are duplicates
        - total_duplicates_count: Total number of duplicates found
    """
    if not records:
        print("DEBUG: _check_for_duplicates_db_side: No records to check")
        return [], 0

    # Determine which columns to check for uniqueness
    if config.duplicate_check.uniqueness_columns:
        uniqueness_columns = config.duplicate_check.uniqueness_columns
        print(f"DEBUG: _check_for_duplicates_db_side: Using custom uniqueness columns: {uniqueness_columns}")
    else:
        # Default to all columns for exact match
        uniqueness_columns = list(records[0].keys())
        print(f"DEBUG: _check_for_duplicates_db_side: Using all columns for uniqueness: {uniqueness_columns}")

    print(f"DEBUG: _check_for_duplicates_db_side: Checking {len(records)} records for duplicates in table '{table_name}'")

    # Check if table exists and has data
    try:
        table_exists_result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": table_name})
        table_exists = table_exists_result.scalar() > 0
        
        if not table_exists:
            print(f"DEBUG: _check_for_duplicates_db_side: Table '{table_name}' does not exist yet, no duplicates possible")
            return [], 0
            
        # Get row count
        count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        row_count = count_result.scalar()
        
        if row_count == 0:
            print(f"DEBUG: _check_for_duplicates_db_side: Table '{table_name}' is empty, no duplicates possible")
            return [], 0
            
        print(f"DEBUG: _check_for_duplicates_db_side: Table '{table_name}' has {row_count} existing rows")
        
    except Exception as e:
        print(f"DEBUG: _check_for_duplicates_db_side: Error checking table existence: {e}")
        return [], 0

    # Get actual table column types for proper casting
    try:
        schema_result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
            AND column_name = ANY(:columns)
        """), {"table_name": table_name, "columns": uniqueness_columns})
        
        table_column_types = {row[0]: row[1] for row in schema_result}
        print(f"DEBUG: _check_for_duplicates_db_side: Table column types: {table_column_types}")
    except Exception as e:
        print(f"DEBUG: _check_for_duplicates_db_side: Could not get table column types: {e}")
        table_column_types = {}

    # Apply type coercion to records
    coerced_records = []
    for record in records:
        coerced_record = {}
        for col_name, value in record.items():
            if col_name in config.db_schema:
                sql_type = config.db_schema[col_name]
                coerced_record[col_name] = coerce_value_for_sql_type(value, sql_type)
            else:
                coerced_record[col_name] = value
        coerced_records.append(coerced_record)

    # Check duplicates in batches and track which records are duplicates
    batch_size = 1000
    duplicate_indices = []
    
    for batch_start in range(0, len(coerced_records), batch_size):
        batch_end = min(batch_start + batch_size, len(coerced_records))
        batch = coerced_records[batch_start:batch_end]
        
        # Build VALUES clause for batch checking with proper type casting
        # We need to check each record individually to know which ones are duplicates
        for idx, record in enumerate(batch):
            global_idx = batch_start + idx
            
            # Build condition for this specific record
            conditions = []
            params = {}
            for col in uniqueness_columns:
                param_name = f"p{global_idx}_{col}"
                value = record.get(col)
                
                # Cast the column to match table column type if available
                if col in table_column_types:
                    table_type = table_column_types[col].upper()
                    if 'TEXT' in table_type or 'CHAR' in table_type:
                        conditions.append(f'CAST("{col}" AS TEXT) = CAST(:{param_name} AS TEXT)')
                    else:
                        conditions.append(f'"{col}" = :{param_name}')
                else:
                    conditions.append(f'"{col}" = :{param_name}')
                
                params[param_name] = value
            
            where_clause = ' AND '.join(conditions)
            
            # Query to check if this specific record exists
            query = text(f"""
                SELECT COUNT(*) FROM "{table_name}"
                WHERE {where_clause}
            """)
            
            try:
                result = conn.execute(query, params)
                count = result.scalar()
                
                if count > 0:
                    duplicate_indices.append(global_idx)
                    
            except Exception as e:
                print(f"DEBUG: _check_for_duplicates_db_side: Error checking record {global_idx}: {e}")
                # Continue checking other records
    
    total_duplicates = len(duplicate_indices)
    print(f"DEBUG: _check_for_duplicates_db_side: Total duplicates found: {total_duplicates}")
    print(f"DEBUG: _check_for_duplicates_db_side: Duplicate indices: {duplicate_indices[:10]}{'...' if len(duplicate_indices) > 10 else ''}")
    
    return duplicate_indices, total_duplicates


def _check_for_duplicates(conn, table_name: str, records: List[Dict[str, Any]], config: MappingConfig):
    """
    Check for duplicate records using Pandas for efficient vectorized operations.
    This approach is much faster and more reliable than row-by-row SQL queries.
    
    NOTE: This is the legacy pandas-based method. For better performance with large
    existing datasets, use _check_for_duplicates_db_side() instead.
    """
    import pandas as pd
    
    if not records:
        print("DEBUG: _check_for_duplicates: No records to check")
        return

    # Determine which columns to check for uniqueness
    if config.duplicate_check.uniqueness_columns:
        uniqueness_columns = config.duplicate_check.uniqueness_columns
        print(f"DEBUG: _check_for_duplicates: Using custom uniqueness columns: {uniqueness_columns}")
    else:
        # Default to all columns for exact match
        uniqueness_columns = list(records[0].keys())
        print(f"DEBUG: _check_for_duplicates: Using all columns for uniqueness: {uniqueness_columns}")

    print(f"DEBUG: _check_for_duplicates: Checking {len(records)} records for duplicates in table '{table_name}'")

    # Convert new records to DataFrame with type coercion applied
    coerced_records = []
    for record in records:
        coerced_record = {}
        for col_name, value in record.items():
            if col_name in config.db_schema:
                sql_type = config.db_schema[col_name]
                coerced_record[col_name] = coerce_value_for_sql_type(value, sql_type)
            else:
                coerced_record[col_name] = value
        coerced_records.append(coerced_record)
    
    new_df = pd.DataFrame(coerced_records)
    print(f"DEBUG: _check_for_duplicates: Created DataFrame with {len(new_df)} rows")

    # Check if table exists and has data
    try:
        table_exists_result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": table_name})
        table_exists = table_exists_result.scalar() > 0
        
        if not table_exists:
            print(f"DEBUG: _check_for_duplicates: Table '{table_name}' does not exist yet, no duplicates possible")
            return
            
        # Get row count
        count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        row_count = count_result.scalar()
        
        if row_count == 0:
            print(f"DEBUG: _check_for_duplicates: Table '{table_name}' is empty, no duplicates possible")
            return
            
        print(f"DEBUG: _check_for_duplicates: Table '{table_name}' has {row_count} existing rows")
        
    except Exception as e:
        print(f"DEBUG: _check_for_duplicates: Error checking table existence: {e}")
        return

    # Load existing data from the table (only the columns we need for comparison)
    try:
        # Build column list for SQL query
        columns_sql = ', '.join([f'"{col}"' for col in uniqueness_columns])
        query = f'SELECT {columns_sql} FROM "{table_name}"'
        print(f"DEBUG: _check_for_duplicates: Loading existing data with query: {query}")
        
        existing_df = pd.read_sql(query, conn)
        print(f"DEBUG: _check_for_duplicates: Loaded {len(existing_df)} existing rows")
        
        if len(existing_df) == 0:
            print("DEBUG: _check_for_duplicates: No existing data to compare against")
            return
            
    except Exception as e:
        print(f"DEBUG: _check_for_duplicates: Error loading existing data: {e}")
        raise

    # Ensure both DataFrames have the same columns for comparison
    new_df_subset = new_df[uniqueness_columns].copy()
    
    # Convert data types to ensure proper comparison
    for col in uniqueness_columns:
        # Handle None/NaN values consistently
        new_df_subset[col] = new_df_subset[col].where(pd.notna(new_df_subset[col]), None)
        existing_df[col] = existing_df[col].where(pd.notna(existing_df[col]), None)
        
        # Convert to same type for comparison
        if col in config.db_schema:
            sql_type = config.db_schema[col].upper()
            if 'INTEGER' in sql_type:
                # Convert to nullable integer type
                new_df_subset[col] = pd.to_numeric(new_df_subset[col], errors='coerce').astype('Int64')
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').astype('Int64')
            elif 'DECIMAL' in sql_type or 'NUMERIC' in sql_type:
                new_df_subset[col] = pd.to_numeric(new_df_subset[col], errors='coerce')
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce')
            else:
                # Convert to string for text comparison
                new_df_subset[col] = new_df_subset[col].astype(str).replace('None', None)
                existing_df[col] = existing_df[col].astype(str).replace('None', None)

    print(f"DEBUG: _check_for_duplicates: Comparing new data against existing data")
    print(f"DEBUG: _check_for_duplicates: New data sample:\n{new_df_subset.head()}")
    print(f"DEBUG: _check_for_duplicates: Existing data sample:\n{existing_df.head()}")

    # Use merge with indicator to find duplicates efficiently
    merged = new_df_subset.merge(
        existing_df,
        on=uniqueness_columns,
        how='left',
        indicator=True
    )
    
    # Count duplicates (rows that exist in both DataFrames)
    duplicates = merged[merged['_merge'] == 'both']
    total_duplicates = len(duplicates)
    
    print(f"DEBUG: _check_for_duplicates: Merge result - total rows: {len(merged)}, duplicates: {total_duplicates}")
    
    if total_duplicates > 0:
        print(f"DEBUG: _check_for_duplicates: Found {total_duplicates} duplicate rows:")
        print(duplicates[uniqueness_columns].head(10))
        
        error_message = config.duplicate_check.error_message or f"Duplicate data detected. {total_duplicates} records overlap with existing data."
        print(f"DEBUG: _check_for_duplicates: Raising DuplicateDataException: {error_message}")
        raise DuplicateDataException(table_name, total_duplicates, error_message)
    else:
        print("DEBUG: _check_for_duplicates: No duplicates found, proceeding with insertion")


def insert_records(engine: Engine, table_name: str, records: List[Dict[str, Any]], config: MappingConfig = None, file_content: bytes = None, file_name: str = None) -> Tuple[int, int]:
    """
    Insert records into the table with enhanced duplicate checking.
    
    For large datasets (>10,000 records), uses chunked processing to optimize memory usage
    and improve performance for duplicate checking and insertion.
    
    Returns:
        Tuple of (records_inserted, duplicates_skipped)
    """
    if not records:
        return 0, 0

    # Initialize duplicates_found to ensure it's always defined
    duplicates_found = 0

    # Create file_imports table if it doesn't exist
    create_file_imports_table_if_not_exists(engine)

    # Debug logging for tests
    print(f"DEBUG: insert_records called for table '{table_name}' with {len(records)} records")
    if config:
        print(f"DEBUG: config provided with db_schema: {config.db_schema}")
        if config.duplicate_check:
            print(f"DEBUG: duplicate_check enabled: {config.duplicate_check.enabled}")
            print(f"DEBUG: check_file_level: {config.duplicate_check.check_file_level}")
            print(f"DEBUG: force_import: {config.duplicate_check.force_import}")
            print(f"DEBUG: allow_duplicates: {config.duplicate_check.allow_duplicates}")
    else:
        print("DEBUG: no config provided")

    # Determine if we should use chunked processing
    # Use chunks for large datasets to optimize memory and performance
    # Increased to 20K for better performance (matches import_orchestrator.py)
    CHUNK_SIZE = 20000
    use_chunked_processing = len(records) > CHUNK_SIZE
    
    if use_chunked_processing:
        print(f"DEBUG: Using chunked processing with chunk size {CHUNK_SIZE} for {len(records)} records")
        # Records passed to insert_records are already mapped (from import_orchestrator)
        # They just need type coercion during insertion
        inserted, duplicates = _insert_records_chunked(engine, table_name, records, config, file_content, file_name, CHUNK_SIZE, pre_mapped=False)
        return inserted, duplicates
    
    # For smaller datasets, use the standard approach
    # Check for duplicates BEFORE starting the insert transaction
    # This ensures we can see committed data from previous transactions
    if config and config.duplicate_check and config.duplicate_check.enabled and not config.duplicate_check.force_import:
        # File-level duplicate check
        if config.duplicate_check.check_file_level and file_content:
            file_hash = calculate_file_hash(file_content)
            print(f"DEBUG: File hash: {file_hash}")
            already_imported = check_file_already_imported(engine, file_hash, table_name)
            print(f"DEBUG: File already imported: {already_imported}")
            if already_imported:
                print("DEBUG: Raising FileAlreadyImportedException")
                raise FileAlreadyImportedException(file_hash, table_name)

        # Row-level duplicate check (unless allow_duplicates is true)
        duplicate_indices = []
        if not config.duplicate_check.allow_duplicates:
            print("DEBUG: Checking for row-level duplicates using database-side method")
            # Use a separate connection to see committed data
            with engine.connect() as check_conn:
                # Use database-side duplicate checking for better performance
                duplicate_indices, duplicates_found = _check_for_duplicates_db_side(check_conn, table_name, records, config)
                
                if duplicates_found > 0:
                    print(f"DEBUG: Found {duplicates_found} duplicates, will skip them and insert only non-duplicates")
                    # Filter out duplicate records
                    records = [rec for idx, rec in enumerate(records) if idx not in duplicate_indices]
                    print(f"DEBUG: After filtering: {len(records)} non-duplicate records remaining")
        
        # If no records left after filtering, return early
        if not records:
            print(f"DEBUG: All {duplicates_found} records were duplicates, nothing to insert")
            return 0, duplicates_found

    # Get import_id from import_history (should be set by import_orchestrator)
    # For now, we'll need to get it from the context or create a temporary one
    import_id = None
    with engine.connect() as conn:
        # Try to get the most recent import_id for this table
        result = conn.execute(text("""
            SELECT import_id FROM import_history 
            WHERE table_name = :table_name 
            AND status = 'in_progress'
            ORDER BY import_timestamp DESC 
            LIMIT 1
        """), {"table_name": table_name})
        row = result.fetchone()
        if row:
            import_id = str(row[0])
    
    if not import_id:
        # Fallback: create a temporary import_id (shouldn't happen in normal flow)
        import uuid
        import_id = str(uuid.uuid4())
        print(f"WARNING: No active import found, using temporary import_id: {import_id}")

    # Now perform the insert in a transaction
    # Get columns from first record, EXCLUDING any metadata that might already exist
    METADATA_COLS = {'_import_id', '_source_row_number', '_corrections_applied', '_imported_at', '_row_id'}
    columns = [col for col in records[0].keys() if col not in METADATA_COLS]
    # Add metadata columns (safe - no duplicates possible)
    columns.extend(['_import_id', '_source_row_number', '_corrections_applied'])
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_sql = ', '.join([f'"{col}"' for col in columns])

    insert_sql = f"""
    INSERT INTO "{table_name}" ({columns_sql})
    VALUES ({placeholders});
    """

    with engine.begin() as conn:
        for row_num, record in enumerate(records, start=1):
            # Apply type coercion based on schema if config is provided
            coerced_record = record.copy()
            corrections = {}
            
            if config and config.db_schema:
                for col_name, value in record.items():
                    if col_name in config.db_schema:
                        sql_type = config.db_schema[col_name]
                        original_value = value
                        coerced_value = coerce_value_for_sql_type(value, sql_type)
                        coerced_record[col_name] = coerced_value
                        
                        # Track corrections
                        if str(original_value) != str(coerced_value):
                            corrections[col_name] = {
                                "before": str(original_value),
                                "after": coerced_value,
                                "correction_type": "type_coercion",
                                "target_type": sql_type
                            }
                            print(f"DEBUG: Coerced column '{col_name}' from {repr(original_value)} ({type(original_value).__name__}) to {repr(coerced_value)} ({type(coerced_value).__name__}) for type {sql_type}")

            # Add metadata
            coerced_record['_import_id'] = import_id
            coerced_record['_source_row_number'] = row_num
            coerced_record['_corrections_applied'] = json.dumps(corrections) if corrections else None

            print(f"DEBUG: Inserting record: {coerced_record}")
            # Insert the coerced record
            conn.execute(text(insert_sql), coerced_record)

        # Record file import if file-level checking is enabled (after successful insert)
        if config and config.duplicate_check and config.duplicate_check.check_file_level and file_content:
            file_hash = calculate_file_hash(file_content)
            print(f"DEBUG: Recording file import with hash: {file_hash}")
            conn.execute(text("""
                INSERT INTO file_imports (file_hash, file_name, table_name, record_count)
                VALUES (:file_hash, :file_name, :table_name, :record_count)
            """), {
                "file_hash": file_hash,
                "file_name": file_name or "",
                "table_name": table_name,
                "record_count": len(records)
            })

    return len(records), duplicates_found


def _check_chunk_for_duplicates(
    engine: Engine,
    table_name: str,
    chunk_records: List[Dict[str, Any]],
    config: MappingConfig,
    chunk_num: int,
    existing_data_cache: Any = None
) -> Tuple[int, int]:
    """
    Check a single chunk for duplicates. Designed to be called in parallel.
    
    Args:
        engine: Database engine
        table_name: Name of the table
        chunk_records: Records in this chunk
        config: Mapping configuration
        chunk_num: Chunk number (for logging)
        existing_data_cache: Pre-loaded existing data (optional, for optimization)
    
    Returns:
        Tuple of (chunk_num, duplicates_found)
    """
    import pandas as pd
    
    logger.info(f"Checking chunk {chunk_num} for duplicates ({len(chunk_records)} records)")
    
    if not chunk_records:
        return (chunk_num, 0)
    
    # Determine which columns to check for uniqueness
    if config.duplicate_check.uniqueness_columns:
        uniqueness_columns = config.duplicate_check.uniqueness_columns
    else:
        uniqueness_columns = list(chunk_records[0].keys())
    
    # Convert chunk records to DataFrame with type coercion
    coerced_records = []
    for record in chunk_records:
        coerced_record = {}
        for col_name, value in record.items():
            if col_name in config.db_schema:
                sql_type = config.db_schema[col_name]
                coerced_record[col_name] = coerce_value_for_sql_type(value, sql_type)
            else:
                coerced_record[col_name] = value
        coerced_records.append(coerced_record)
    
    new_df = pd.DataFrame(coerced_records)
    
    # Load existing data if not cached
    if existing_data_cache is None:
        with engine.connect() as conn:
            # Check if table exists and has data
            table_exists_result = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})
            table_exists = table_exists_result.scalar() > 0
            
            if not table_exists:
                logger.info(f"Chunk {chunk_num}: Table does not exist yet, no duplicates possible")
                return (chunk_num, 0)
            
            # Get row count
            count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            row_count = count_result.scalar()
            
            if row_count == 0:
                logger.info(f"Chunk {chunk_num}: Table is empty, no duplicates possible")
                return (chunk_num, 0)
            
            # Load existing data
            columns_sql = ', '.join([f'"{col}"' for col in uniqueness_columns])
            query = f'SELECT {columns_sql} FROM "{table_name}"'
            existing_df = pd.read_sql(query, conn)
    else:
        existing_df = existing_data_cache
    
    if len(existing_df) == 0:
        logger.info(f"Chunk {chunk_num}: No existing data to compare against")
        return (chunk_num, 0)
    
    # Prepare data for comparison
    new_df_subset = new_df[uniqueness_columns].copy()
    
    # Convert data types to ensure proper comparison
    for col in uniqueness_columns:
        new_df_subset[col] = new_df_subset[col].where(pd.notna(new_df_subset[col]), None)
        existing_df[col] = existing_df[col].where(pd.notna(existing_df[col]), None)
        
        if col in config.db_schema:
            sql_type = config.db_schema[col].upper()
            if 'INTEGER' in sql_type:
                new_df_subset[col] = pd.to_numeric(new_df_subset[col], errors='coerce').astype('Int64')
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').astype('Int64')
            elif 'DECIMAL' in sql_type or 'NUMERIC' in sql_type:
                new_df_subset[col] = pd.to_numeric(new_df_subset[col], errors='coerce')
                existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce')
            else:
                new_df_subset[col] = new_df_subset[col].astype(str).replace('None', None)
                existing_df[col] = existing_df[col].astype(str).replace('None', None)
    
    # Use merge to find duplicates
    merged = new_df_subset.merge(
        existing_df,
        on=uniqueness_columns,
        how='left',
        indicator=True
    )
    
    duplicates = merged[merged['_merge'] == 'both']
    total_duplicates = len(duplicates)
    
    if total_duplicates > 0:
        logger.warning(f"Chunk {chunk_num}: Found {total_duplicates} duplicate rows")
    else:
        logger.info(f"Chunk {chunk_num}: No duplicates found")
    
    return (chunk_num, total_duplicates)


def _check_chunks_parallel(
    engine: Engine,
    table_name: str,
    chunks: List[List[Dict[str, Any]]],
    config: MappingConfig,
    max_workers: int = 4
) -> int:
    """
    Check multiple chunks for duplicates in parallel using database-side queries.
    
    This is much faster than loading all existing data into pandas and doing merges.
    Uses PostgreSQL's IN clause with VALUES for efficient batch checking.
    
    Args:
        engine: Database engine
        table_name: Name of the table
        chunks: List of record chunks
        config: Mapping configuration
        max_workers: Maximum number of parallel workers
    
    Returns:
        Total number of duplicates found across all chunks
    
    Raises:
        DuplicateDataException: If any duplicates are found
    """
    logger.info(f"Starting parallel duplicate check for {len(chunks)} chunks with {max_workers} workers")
    
    # Determine uniqueness columns
    if config.duplicate_check.uniqueness_columns:
        uniqueness_columns = config.duplicate_check.uniqueness_columns
    else:
        # Use first chunk to determine columns
        uniqueness_columns = list(chunks[0][0].keys()) if chunks and chunks[0] else []
    
    if not uniqueness_columns:
        logger.warning("No uniqueness columns specified, skipping duplicate check")
        return 0
    
    # Quick check: does table exist and have data?
    with engine.connect() as conn:
        table_exists_result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": table_name})
        table_exists = table_exists_result.scalar() > 0
        
        if not table_exists:
            logger.info(f"Table '{table_name}' does not exist yet, no duplicates possible")
            return 0
        
        count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
        row_count = count_result.scalar()
        
        if row_count == 0:
            logger.info(f"Table '{table_name}' is empty, no duplicates possible")
            return 0
        
        logger.info(f"Table '{table_name}' has {row_count} existing rows, checking for duplicates")
    
    # Check chunks in parallel using database-side queries
    total_duplicates = 0
    duplicate_chunks = []
    
    def check_chunk_db_side(chunk_num: int, chunk_records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Check a single chunk using database-side query."""
        if not chunk_records:
            return (chunk_num, 0)
        
        logger.info(f"Checking chunk {chunk_num} for duplicates ({len(chunk_records)} records)")
        
        # Apply type coercion to records
        coerced_records = []
        for record in chunk_records:
            coerced_record = {}
            for col_name, value in record.items():
                if col_name in config.db_schema:
                    sql_type = config.db_schema[col_name]
                    coerced_record[col_name] = coerce_value_for_sql_type(value, sql_type)
                else:
                    coerced_record[col_name] = value
            coerced_records.append(coerced_record)
        
        # Check duplicates in batches to avoid query size limits
        batch_size = 1000
        chunk_duplicates = 0
        
        with engine.connect() as conn:
            for batch_start in range(0, len(coerced_records), batch_size):
                batch_end = min(batch_start + batch_size, len(coerced_records))
                batch = coerced_records[batch_start:batch_end]
                
                # Build VALUES clause for batch checking
                values_list = []
                params = {}
                
                for idx, record in enumerate(batch):
                    value_placeholders = []
                    for col in uniqueness_columns:
                        param_name = f"p{chunk_num}_{batch_start}_{idx}_{col}"
                        value_placeholders.append(f":{param_name}")
                        params[param_name] = record.get(col)
                    values_list.append(f"({','.join(value_placeholders)})")
                
                values_clause = ','.join(values_list)
                columns_clause = ','.join([f'"{col}"' for col in uniqueness_columns])
                
                # Query to count duplicates in this batch
                query = text(f"""
                    SELECT COUNT(*) FROM "{table_name}"
                    WHERE ({columns_clause}) IN (VALUES {values_clause})
                """)
                
                try:
                    result = conn.execute(query, params)
                    batch_duplicates = result.scalar()
                    chunk_duplicates += batch_duplicates
                    
                    if batch_duplicates > 0:
                        logger.warning(f"Chunk {chunk_num} batch {batch_start//batch_size + 1} found {batch_duplicates} duplicates")
                except Exception as e:
                    logger.error(f"Error checking chunk {chunk_num} batch: {e}")
                    raise
        
        if chunk_duplicates > 0:
            logger.warning(f"Chunk {chunk_num}: Found {chunk_duplicates} total duplicates")
        else:
            logger.info(f"Chunk {chunk_num}: No duplicates found")
        
        return (chunk_num, chunk_duplicates)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunk checks
        future_to_chunk = {
            executor.submit(check_chunk_db_side, chunk_num + 1, chunk_records): chunk_num
            for chunk_num, chunk_records in enumerate(chunks)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            chunk_num = future_to_chunk[future]
            try:
                result_chunk_num, duplicates_found = future.result()
                if duplicates_found > 0:
                    total_duplicates += duplicates_found
                    duplicate_chunks.append(result_chunk_num)
            except Exception as e:
                logger.error(f"Error checking chunk {chunk_num + 1}: {e}")
                raise
    
    # If duplicates found, raise exception
    if total_duplicates > 0:
        error_message = config.duplicate_check.error_message or \
            f"Duplicate data detected. {total_duplicates} records overlap with existing data in {len(duplicate_chunks)} chunk(s)."
        logger.error(f"Parallel duplicate check failed: {error_message}")
        raise DuplicateDataException(table_name, total_duplicates, error_message)
    
    logger.info(f"Parallel duplicate check completed successfully - no duplicates found")
    return 0


def _insert_records_chunked(engine: Engine, table_name: str, records: List[Dict[str, Any]], config: MappingConfig, file_content: bytes, file_name: str, chunk_size: int, pre_mapped: bool = False):
    """
    Insert records in chunks for better performance with large datasets.
    This approach uses two-phase parallel processing:
    1. Phase 1: Check all chunks for duplicates in parallel (CPU-intensive)
    2. Phase 2: Insert all chunks sequentially using SQL INSERT (I/O-intensive, avoids race conditions)
    
    Args:
        engine: Database engine
        table_name: Target table name
        records: Records to insert (may be pre-mapped or raw)
        config: Mapping configuration
        file_content: Original file content
        file_name: Original file name
        chunk_size: Size of each chunk
        pre_mapped: If True, records are already mapped and type-coerced
    
    This provides significant speedup while maintaining data integrity.
    """
    import uuid
    
    total_records = len(records)
    print(f"DEBUG: _insert_records_chunked: Processing {total_records} {'pre-mapped' if pre_mapped else 'raw'} records in chunks of {chunk_size}")
    
    # File-level duplicate check (once upfront)
    if config and config.duplicate_check and config.duplicate_check.enabled and not config.duplicate_check.force_import:
        if config.duplicate_check.check_file_level and file_content:
            file_hash = calculate_file_hash(file_content)
            print(f"DEBUG: File hash: {file_hash}")
            already_imported = check_file_already_imported(engine, file_hash, table_name)
            print(f"DEBUG: File already imported: {already_imported}")
            if already_imported:
                print("DEBUG: Raising FileAlreadyImportedException")
                raise FileAlreadyImportedException(file_hash, table_name)
    
    # Get import_id from import_history (should be set by import_orchestrator)
    import_id = None
    with engine.connect() as conn:
        # Try to get the most recent import_id for this table
        result = conn.execute(text("""
            SELECT import_id FROM import_history 
            WHERE table_name = :table_name 
            AND status = 'in_progress'
            ORDER BY import_timestamp DESC 
            LIMIT 1
        """), {"table_name": table_name})
        row = result.fetchone()
        if row:
            import_id = str(row[0])
    
    if not import_id:
        # Fallback: create a temporary import_id (shouldn't happen in normal flow)
        import_id = str(uuid.uuid4())
        print(f"WARNING: No active import found, using temporary import_id: {import_id}")
    
    # Split records into chunks
    chunks = []
    for chunk_start in range(0, total_records, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_records)
        chunk_records = records[chunk_start:chunk_end]
        chunks.append(chunk_records)
    
    total_chunks = len(chunks)
    logger.info(f"Split {total_records} records into {total_chunks} chunks of {chunk_size}")
    
    # PHASE 1: Parallel duplicate checking (CPU-intensive)
    if config and config.duplicate_check and config.duplicate_check.enabled and not config.duplicate_check.force_import:
        if not config.duplicate_check.allow_duplicates:
            logger.info("Phase 1: Starting parallel duplicate check")
            # Determine number of workers based on CPU count (max 4 to avoid overwhelming the system)
            import os
            max_workers = min(4, os.cpu_count() or 2)
            logger.info(f"Using {max_workers} parallel workers for duplicate checking")
            
            try:
                _check_chunks_parallel(engine, table_name, chunks, config, max_workers)
                logger.info("Phase 1: Parallel duplicate check completed - no duplicates found")
            except DuplicateDataException as e:
                logger.error(f"Phase 1: Duplicate check failed - {e.message}")
                raise
    
    # PHASE 2: Sequential insertion (I/O-intensive, avoids race conditions)
    logger.info("Phase 2: Starting sequential chunk insertion")
    total_inserted = 0
    
    # Get column names from first record
    if not records:
        return 0
    
    # Get columns from first record, EXCLUDING any metadata that might already exist
    METADATA_COLS = {'_import_id', '_source_row_number', '_corrections_applied', '_imported_at', '_row_id'}
    columns = [col for col in records[0].keys() if col not in METADATA_COLS]
    # Add metadata columns (safe - no duplicates possible)
    columns.extend(['_import_id', '_source_row_number', '_corrections_applied'])
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_sql = ', '.join([f'"{col}"' for col in columns])
    
    insert_sql = f"""
    INSERT INTO "{table_name}" ({columns_sql})
    VALUES ({placeholders});
    """
    
    for chunk_num, chunk_records in enumerate(chunks, start=1):
        print(f"DEBUG: Inserting chunk {chunk_num}/{total_chunks} ({len(chunk_records)} records)")
        
        # Prepare all records with type coercion and metadata
        coerced_chunk = []
        chunk_start_row = (chunk_num - 1) * chunk_size + 1
        
        for idx, record in enumerate(chunk_records):
            if pre_mapped:
                # Records are already mapped and type-coerced
                coerced_record = record.copy()
            else:
                # Apply type coercion
                coerced_record = {}
                if config and config.db_schema:
                    for col_name, value in record.items():
                        if col_name in config.db_schema:
                            sql_type = config.db_schema[col_name]
                            coerced_record[col_name] = coerce_value_for_sql_type(value, sql_type)
                        else:
                            coerced_record[col_name] = value
                else:
                    coerced_record = record.copy()
            
            # Add metadata
            coerced_record['_import_id'] = import_id
            coerced_record['_source_row_number'] = chunk_start_row + idx
            coerced_record['_corrections_applied'] = None  # TODO: Track corrections in chunked mode
            
            coerced_chunk.append(coerced_record)
        
        # Bulk insert the chunk
        with engine.begin() as conn:
            conn.execute(text(insert_sql), coerced_chunk)
        
        total_inserted += len(chunk_records)
        print(f"DEBUG: Inserted chunk {chunk_num}/{total_chunks} - Total inserted: {total_inserted}/{total_records}")
    
    # Record file import after all chunks are successfully inserted
    if config and config.duplicate_check and config.duplicate_check.check_file_level and file_content:
        file_hash = calculate_file_hash(file_content)
        print(f"DEBUG: Recording file import with hash: {file_hash}")
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO file_imports (file_hash, file_name, table_name, record_count)
                VALUES (:file_hash, :file_name, :table_name, :record_count)
            """), {
                "file_hash": file_hash,
                "file_name": file_name or "",
                "table_name": table_name,
                "record_count": total_inserted
            })
    
    print(f"DEBUG: _insert_records_chunked: Completed - {total_inserted} records inserted")
    return total_inserted, 0  # Return tuple: (records_inserted, duplicates_skipped)
