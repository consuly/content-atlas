import hashlib
from sqlalchemy import text, MetaData
from sqlalchemy.engine import Engine
from typing import List, Dict, Any
from decimal import Decimal
import math
from .schemas import MappingConfig


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
            # Get current column types
            current_columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'
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
            # Create table with correct schema
            columns = []
            for col_name, col_type in config.db_schema.items():
                columns.append(f'"{col_name}" {col_type}')

            columns_sql = ', '.join(columns)

            create_sql = f"""
            CREATE TABLE "{table_name}" (
                id SERIAL PRIMARY KEY,
                {columns_sql}
            );
            """

            conn.execute(text(create_sql))
            print(f"DEBUG: create_table_if_not_exists: Table '{table_name}' created successfully")


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
        # Convert to float for better psycopg2 compatibility
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

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


def _check_for_duplicates(conn, table_name: str, records: List[Dict[str, Any]], config: MappingConfig):
    """
    Check for duplicate records using Pandas for efficient vectorized operations.
    This approach is much faster and more reliable than row-by-row SQL queries.
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


def insert_records(engine: Engine, table_name: str, records: List[Dict[str, Any]], config: MappingConfig = None, file_content: bytes = None, file_name: str = None):
    """
    Insert records into the table with enhanced duplicate checking.
    
    For large datasets (>10,000 records), uses chunked processing to optimize memory usage
    and improve performance for duplicate checking and insertion.
    """
    if not records:
        return 0

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
    CHUNK_SIZE = 10000
    use_chunked_processing = len(records) > CHUNK_SIZE
    
    if use_chunked_processing:
        print(f"DEBUG: Using chunked processing with chunk size {CHUNK_SIZE} for {len(records)} records")
        return _insert_records_chunked(engine, table_name, records, config, file_content, file_name, CHUNK_SIZE)
    
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
        if not config.duplicate_check.allow_duplicates:
            print("DEBUG: Checking for row-level duplicates")
            # Use a separate connection to see committed data
            with engine.connect() as check_conn:
                _check_for_duplicates(check_conn, table_name, records, config)

    # Now perform the insert in a transaction
    columns = list(records[0].keys())
    placeholders = ', '.join([f':{col}' for col in columns])
    columns_sql = ', '.join([f'"{col}"' for col in columns])

    insert_sql = f"""
    INSERT INTO "{table_name}" ({columns_sql})
    VALUES ({placeholders});
    """

    with engine.begin() as conn:
        for record in records:
            # Apply type coercion based on schema if config is provided
            coerced_record = record.copy()
            if config and config.db_schema:
                for col_name, value in record.items():
                    if col_name in config.db_schema:
                        sql_type = config.db_schema[col_name]
                        original_value = value
                        coerced_value = coerce_value_for_sql_type(value, sql_type)
                        coerced_record[col_name] = coerced_value
                        # Debug logging for type coercion
                        if str(original_value) != str(coerced_value):
                            print(f"DEBUG: Coerced column '{col_name}' from {repr(original_value)} ({type(original_value).__name__}) to {repr(coerced_value)} ({type(coerced_value).__name__}) for type {sql_type}")

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

    return len(records)


def _insert_records_chunked(engine: Engine, table_name: str, records: List[Dict[str, Any]], config: MappingConfig, file_content: bytes, file_name: str, chunk_size: int):
    """
    Insert records in chunks for better performance with large datasets.
    This approach:
    1. Checks file-level duplicates once upfront
    2. Processes records in chunks to manage memory
    3. Checks each chunk for row-level duplicates
    4. Inserts non-duplicate chunks efficiently using bulk insert
    """
    import pandas as pd
    
    total_records = len(records)
    print(f"DEBUG: _insert_records_chunked: Processing {total_records} records in chunks of {chunk_size}")
    
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
    
    # Process records in chunks
    total_inserted = 0
    total_duplicates_found = 0
    
    for chunk_start in range(0, total_records, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_records)
        chunk_records = records[chunk_start:chunk_end]
        chunk_num = (chunk_start // chunk_size) + 1
        total_chunks = (total_records + chunk_size - 1) // chunk_size
        
        print(f"DEBUG: Processing chunk {chunk_num}/{total_chunks} ({len(chunk_records)} records)")
        
        # Check for duplicates in this chunk
        if config and config.duplicate_check and config.duplicate_check.enabled and not config.duplicate_check.force_import:
            if not config.duplicate_check.allow_duplicates:
                try:
                    with engine.connect() as check_conn:
                        _check_for_duplicates(check_conn, table_name, chunk_records, config)
                except DuplicateDataException as e:
                    # Accumulate duplicate count and continue or raise based on strategy
                    total_duplicates_found += e.duplicates_found
                    print(f"DEBUG: Found {e.duplicates_found} duplicates in chunk {chunk_num}")
                    # For now, raise immediately on first duplicate chunk
                    # In future, could implement partial import strategy
                    raise
        
        # Insert chunk using bulk insert for better performance
        columns = list(chunk_records[0].keys())
        columns_sql = ', '.join([f'"{col}"' for col in columns])
        
        # Prepare all records with type coercion
        coerced_chunk = []
        for record in chunk_records:
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
            coerced_chunk.append(coerced_record)
        
        # Use pandas to_sql for efficient bulk insert
        df = pd.DataFrame(coerced_chunk)
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        
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
    return total_inserted
