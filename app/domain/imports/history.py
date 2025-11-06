"""
Import history tracking for comprehensive data lineage and auditing.

This module provides functionality to track all data imports with detailed metadata,
enabling traceability, auditing, and rollback capabilities.
"""

from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timezone
import uuid
import json
from app.db.session import get_engine
from app.api.schemas.shared import MappingConfig
from decimal import Decimal
from datetime import date
import logging

logger = logging.getLogger(__name__)


def create_import_history_table():
    """
    Create the import_history and mapping_errors tables if they don't exist.
    
    These tables store comprehensive metadata about every import operation,
    enabling full traceability and auditing.
    """
    engine = get_engine()
    
    # Enhanced import_history table with mapping tracking
    create_import_history_sql = """
    CREATE TABLE IF NOT EXISTS import_history (
        import_id UUID PRIMARY KEY,
        import_timestamp TIMESTAMP DEFAULT NOW(),
        
        -- User/Actor Information
        user_id VARCHAR(255),
        user_email VARCHAR(255),
        
        -- Source Information
        source_type VARCHAR(50) NOT NULL,  -- 'local_upload', 'b2_storage', 'api_direct'
        source_path TEXT,  -- Full path or B2 key
        file_name VARCHAR(500),
        file_size_bytes BIGINT,
        file_type VARCHAR(50),  -- 'csv', 'excel', 'json', 'xml'
        file_hash VARCHAR(64),  -- SHA-256 hash
        
        -- Destination Information
        table_name VARCHAR(255) NOT NULL,
        import_strategy VARCHAR(50),  -- 'new_table', 'merge_exact', 'extend_table', 'adapt_data'
        
        -- Configuration
        mapping_config JSONB,  -- Full mapping configuration used
        duplicate_check_enabled BOOLEAN DEFAULT TRUE,
        
        -- Import Outcome
        status VARCHAR(50) NOT NULL,  -- 'success', 'failed', 'partial'
        error_message TEXT,
        warnings TEXT[],
        
        -- Mapping Status (NEW)
        mapping_status VARCHAR(50) DEFAULT 'not_started',  -- 'not_started', 'in_progress', 'completed', 'completed_with_errors', 'failed'
        mapping_started_at TIMESTAMP,
        mapping_completed_at TIMESTAMP,
        mapping_duration_seconds DECIMAL(10, 3),
        mapping_errors_count INTEGER DEFAULT 0,
        
        -- Statistics
        total_rows_in_file INTEGER,
        rows_processed INTEGER,
        rows_inserted INTEGER,
        rows_skipped INTEGER,
        duplicates_found INTEGER,
        data_validation_errors INTEGER,  -- Renamed from validation_errors for clarity
        
        -- Performance Metrics
        duration_seconds DECIMAL(10, 3),
        parsing_time_seconds DECIMAL(10, 3),
        duplicate_check_time_seconds DECIMAL(10, 3),
        insert_time_seconds DECIMAL(10, 3),
        
        -- Additional Context
        analysis_id UUID,  -- Link to AI analysis if used
        task_id UUID,  -- Link to async task if used
        metadata JSONB,  -- Additional flexible metadata
        
        -- Timestamps
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_import_history_timestamp ON import_history(import_timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_import_history_table ON import_history(table_name);
    CREATE INDEX IF NOT EXISTS idx_import_history_status ON import_history(status);
    CREATE INDEX IF NOT EXISTS idx_import_history_mapping_status ON import_history(mapping_status);
    CREATE INDEX IF NOT EXISTS idx_import_history_user ON import_history(user_id);
    CREATE INDEX IF NOT EXISTS idx_import_history_file_hash ON import_history(file_hash);
    """
    
    # New mapping_errors table for detailed error tracking
    create_mapping_errors_sql = """
    CREATE TABLE IF NOT EXISTS mapping_errors (
        id SERIAL PRIMARY KEY,
        import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE,
        
        -- Error Context
        record_number INTEGER,  -- Which record in the file (1-indexed)
        source_field VARCHAR(255),  -- Which source field caused the error
        target_field VARCHAR(255),  -- Target database column
        
        -- Error Details
        error_type VARCHAR(100),  -- 'datetime_conversion', 'type_mismatch', 'missing_required', 'mapping_error', etc.
        error_message TEXT NOT NULL,  -- Full error message
        source_value TEXT,  -- The problematic value (truncated if too long)
        
        -- Metadata
        occurred_at TIMESTAMP DEFAULT NOW(),
        chunk_number INTEGER  -- For parallel processing tracking
    );
    
    -- Indexes for efficient querying
    CREATE INDEX IF NOT EXISTS idx_mapping_errors_import ON mapping_errors(import_id);
    CREATE INDEX IF NOT EXISTS idx_mapping_errors_type ON mapping_errors(error_type);
    CREATE INDEX IF NOT EXISTS idx_mapping_errors_field ON mapping_errors(source_field);
    """

    create_import_duplicates_sql = """
    CREATE TABLE IF NOT EXISTS import_duplicates (
        id SERIAL PRIMARY KEY,
        import_id UUID NOT NULL REFERENCES import_history(import_id) ON DELETE CASCADE,
        record_number INTEGER,
        record_data JSONB NOT NULL,
        detected_at TIMESTAMP DEFAULT NOW(),
        resolved_at TIMESTAMP,
        resolved_by VARCHAR(255),
        resolution_details JSONB
    );

    CREATE INDEX IF NOT EXISTS idx_import_duplicates_import ON import_duplicates(import_id);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_import_history_sql))
            conn.execute(text(create_mapping_errors_sql))
            conn.execute(text(create_import_duplicates_sql))
            conn.execute(text("""
                ALTER TABLE import_duplicates
                ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMP;
            """))
            conn.execute(text("""
                ALTER TABLE import_duplicates
                ADD COLUMN IF NOT EXISTS resolved_by VARCHAR(255);
            """))
            conn.execute(text("""
                ALTER TABLE import_duplicates
                ADD COLUMN IF NOT EXISTS resolution_details JSONB;
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_import_duplicates_resolved
                ON import_duplicates(import_id, resolved_at);
            """))
        logger.info("import_history and mapping_errors tables created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise


def start_import_tracking(
    source_type: str,
    file_name: str,
    table_name: str,
    file_size_bytes: Optional[int] = None,
    file_type: Optional[str] = None,
    file_hash: Optional[str] = None,
    source_path: Optional[str] = None,
    mapping_config: Optional[MappingConfig] = None,
    user_id: Optional[str] = None,
    user_email: Optional[str] = None,
    analysis_id: Optional[str] = None,
    task_id: Optional[str] = None,
    import_strategy: Optional[str] = None
) -> str:
    """
    Start tracking a new import operation.
    
    Args:
        source_type: Type of source ('local_upload', 'b2_storage', 'api_direct')
        file_name: Name of the file being imported
        table_name: Destination table name
        file_size_bytes: Size of file in bytes
        file_type: Type of file ('csv', 'excel', 'json', 'xml')
        file_hash: SHA-256 hash of file content
        source_path: Full path or B2 key
        mapping_config: Mapping configuration used
        user_id: ID of user performing import
        user_email: Email of user performing import
        analysis_id: UUID of AI analysis if used
        task_id: UUID of async task if used
        import_strategy: Strategy used ('new_table', 'merge_exact', etc.)
        
    Returns:
        import_id (UUID string) for tracking this import
    """
    # Defensive guard so imports still work after a DB reset without restarting the API.
    # This recreates import_history/mapping_errors when they were dropped by reset_dev_db.py.
    create_import_history_table()

    engine = get_engine()
    import_id = str(uuid.uuid4())
    
    # Convert mapping config to JSON if provided
    mapping_json = None
    duplicate_check_enabled = True
    if mapping_config:
        mapping_json = mapping_config.model_dump()
        duplicate_check_enabled = mapping_config.check_duplicates
    
    try:
        with engine.begin() as conn:
            insert_sql = """
            INSERT INTO import_history (
                import_id, source_type, file_name, table_name,
                file_size_bytes, file_type, file_hash, source_path,
                mapping_config, duplicate_check_enabled,
                user_id, user_email, analysis_id, task_id,
                import_strategy, status
            ) VALUES (
                :import_id, :source_type, :file_name, :table_name,
                :file_size_bytes, :file_type, :file_hash, :source_path,
                :mapping_config, :duplicate_check_enabled,
                :user_id, :user_email, :analysis_id, :task_id,
                :import_strategy, 'in_progress'
            )
            """
            
            conn.execute(text(insert_sql), {
                "import_id": import_id,
                "source_type": source_type,
                "file_name": file_name,
                "table_name": table_name,
                "file_size_bytes": file_size_bytes,
                "file_type": file_type,
                "file_hash": file_hash,
                "source_path": source_path,
                "mapping_config": json.dumps(mapping_json) if mapping_json else None,
                "duplicate_check_enabled": duplicate_check_enabled,
                "user_id": user_id,
                "user_email": user_email,
                "analysis_id": analysis_id,
                "task_id": task_id,
                "import_strategy": import_strategy
            })
        
        logger.info(f"Started import tracking: {import_id}")
        return import_id
        
    except Exception as e:
        logger.error(f"Error starting import tracking: {str(e)}")
        raise


def update_mapping_status(
    import_id: str,
    status: str,
    errors_count: int = 0,
    duration_seconds: Optional[float] = None
):
    """
    Update mapping status for an import.
    
    Args:
        import_id: UUID of the import
        status: Mapping status ('in_progress', 'completed', 'completed_with_errors', 'failed')
        errors_count: Number of mapping errors encountered
        duration_seconds: Time spent on mapping
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            # If starting, set started_at
            if status == 'in_progress':
                update_sql = """
                UPDATE import_history SET
                    mapping_status = :status,
                    mapping_started_at = NOW(),
                    updated_at = NOW()
                WHERE import_id = :import_id
                """
                conn.execute(text(update_sql), {
                    "import_id": import_id,
                    "status": status
                })
            else:
                # If completing, set completed_at and duration
                update_sql = """
                UPDATE import_history SET
                    mapping_status = :status,
                    mapping_completed_at = NOW(),
                    mapping_duration_seconds = :duration_seconds,
                    mapping_errors_count = :errors_count,
                    updated_at = NOW()
                WHERE import_id = :import_id
                """
                conn.execute(text(update_sql), {
                    "import_id": import_id,
                    "status": status,
                    "duration_seconds": duration_seconds,
                    "errors_count": errors_count
                })
        
        logger.info(f"Updated mapping status to '{status}' for import {import_id}")
        
    except Exception as e:
        logger.error(f"Error updating mapping status: {str(e)}")
        raise


def record_mapping_errors_batch(
    import_id: str,
    errors: List[Dict[str, Any]]
):
    """
    Batch insert mapping errors for efficiency.
    
    Args:
        import_id: UUID of the import
        errors: List of error dictionaries with keys:
            - record_number: int (optional)
            - error_type: str (optional, defaults to 'mapping_error')
            - error_message: str (required)
            - source_field: str (optional)
            - target_field: str (optional)
            - source_value: str (optional)
            - chunk_number: int (optional)
    """
    if not errors:
        return
    
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            insert_sql = """
            INSERT INTO mapping_errors (
                import_id, record_number, error_type, error_message,
                source_field, target_field, source_value, chunk_number
            ) VALUES (
                :import_id, :record_number, :error_type, :error_message,
                :source_field, :target_field, :source_value, :chunk_number
            )
            """
            
            # Prepare batch insert data
            batch_data = []
            for error in errors:
                # Truncate source_value if too long (keep first 500 chars)
                source_value = error.get("source_value")
                if source_value and len(str(source_value)) > 500:
                    source_value = str(source_value)[:497] + "..."
                
                batch_data.append({
                    "import_id": import_id,
                    "record_number": error.get("record_number"),
                    "error_type": error.get("error_type", "mapping_error"),
                    "error_message": error.get("error_message"),
                    "source_field": error.get("source_field"),
                    "target_field": error.get("target_field"),
                    "source_value": source_value,
                    "chunk_number": error.get("chunk_number")
                })
            
            # Execute batch insert
            for data in batch_data:
                conn.execute(text(insert_sql), data)
        
        logger.info(f"Recorded {len(errors)} mapping errors for import {import_id}")
        
    except Exception as e:
        logger.error(f"Error recording mapping errors: {str(e)}")
        raise


def get_mapping_errors(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    error_type: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Retrieve mapping errors for an import.
    
    Args:
        import_id: UUID of the import
        limit: Maximum number of errors to return
        offset: Number of errors to skip
        error_type: Optional filter by error type
        
    Returns:
        List of mapping error records
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            where_clauses = ["import_id = :import_id"]
            params = {
                "import_id": import_id,
                "limit": limit,
                "offset": offset
            }
            
            if error_type:
                where_clauses.append("error_type = :error_type")
                params["error_type"] = error_type
            
            where_sql = " AND ".join(where_clauses)
            
            query = f"""
            SELECT * FROM mapping_errors
            WHERE {where_sql}
            ORDER BY record_number, id
            LIMIT :limit OFFSET :offset
            """
            
            result = conn.execute(text(query), params)
            
            errors = []
            for row in result:
                errors.append({
                    "id": row[0],
                    "import_id": str(row[1]),
                    "record_number": row[2],
                    "source_field": row[3],
                    "target_field": row[4],
                    "error_type": row[5],
                    "error_message": row[6],
                    "source_value": row[7],
                    "occurred_at": row[8].isoformat() if row[8] else None,
                    "chunk_number": row[9]
                })
            
            return errors
            
    except Exception as e:
        logger.error(f"Error retrieving mapping errors: {str(e)}")
        raise


def complete_import_tracking(
    import_id: str,
    status: str,
    total_rows_in_file: int,
    rows_processed: int,
    rows_inserted: int,
    rows_skipped: int = 0,
    duplicates_found: int = 0,
    validation_errors: int = 0,
    duration_seconds: Optional[float] = None,
    parsing_time_seconds: Optional[float] = None,
    duplicate_check_time_seconds: Optional[float] = None,
    insert_time_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
    warnings: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None
):
    """
    Complete import tracking with final statistics and outcome.
    
    Args:
        import_id: UUID of the import to update
        status: Final status ('success', 'failed', 'partial')
        total_rows_in_file: Total rows in source file
        rows_processed: Number of rows processed
        rows_inserted: Number of rows successfully inserted
        rows_skipped: Number of rows skipped
        duplicates_found: Number of duplicate rows detected
        validation_errors: Number of validation errors
        duration_seconds: Total duration in seconds
        parsing_time_seconds: Time spent parsing file
        duplicate_check_time_seconds: Time spent checking duplicates
        insert_time_seconds: Time spent inserting data
        error_message: Error message if failed
        warnings: List of warning messages
        metadata: Additional metadata to store
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            update_sql = """
            UPDATE import_history SET
                status = :status,
                total_rows_in_file = :total_rows_in_file,
                rows_processed = :rows_processed,
                rows_inserted = :rows_inserted,
                rows_skipped = :rows_skipped,
                duplicates_found = :duplicates_found,
                data_validation_errors = :validation_errors,
                duration_seconds = :duration_seconds,
                parsing_time_seconds = :parsing_time_seconds,
                duplicate_check_time_seconds = :duplicate_check_time_seconds,
                insert_time_seconds = :insert_time_seconds,
                error_message = :error_message,
                warnings = :warnings,
                metadata = :metadata,
                updated_at = NOW()
            WHERE import_id = :import_id
            """
            
            conn.execute(text(update_sql), {
                "import_id": import_id,
                "status": status,
                "total_rows_in_file": total_rows_in_file,
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "rows_skipped": rows_skipped,
                "duplicates_found": duplicates_found,
                "validation_errors": validation_errors,
                "duration_seconds": duration_seconds,
                "parsing_time_seconds": parsing_time_seconds,
                "duplicate_check_time_seconds": duplicate_check_time_seconds,
                "insert_time_seconds": insert_time_seconds,
                "error_message": error_message,
                "warnings": warnings,
                "metadata": json.dumps(metadata) if metadata else None
            })
        
        logger.info(f"Completed import tracking: {import_id} with status {status}")
        
    except Exception as e:
        logger.error(f"Error completing import tracking: {str(e)}")
        raise


def record_duplicate_rows(import_id: str, duplicates: List[Dict[str, Any]]) -> None:
    """
    Persist duplicate records detected during an import so they can be reviewed later.
    """
    if not duplicates:
        return

    engine = get_engine()
    payload = []

    for entry in duplicates:
        record_number = entry.get("record_number")
        record = entry.get("record", {})
        safe_record = _make_json_safe(record)
        # Store as JSON string to avoid DB adapter issues
        safe_record_json = json.dumps(safe_record)
        payload.append({
            "import_id": import_id,
            "record_number": record_number,
            "record_data": safe_record_json
        })

    insert_sql = text("""
        INSERT INTO import_duplicates (import_id, record_number, record_data)
        VALUES (:import_id, :record_number, :record_data)
    """)

    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, payload)
    except Exception as e:
        logger.error(f"Error recording duplicate rows for import {import_id}: {str(e)}")
        raise


def list_duplicate_rows(
    import_id: str,
    limit: int = 100,
    offset: int = 0,
    include_resolved: bool = False
) -> List[Dict[str, Any]]:
    """
    Retrieve duplicate rows that were detected for an import.
    """
    engine = get_engine()
    where_clause = "import_id = :import_id"
    if not include_resolved:
        where_clause += " AND resolved_at IS NULL"
    query = text(f"""
        SELECT id, record_number, record_data, detected_at, resolved_at, resolved_by, resolution_details
        FROM import_duplicates
        WHERE {where_clause}
        ORDER BY COALESCE(record_number, 0), detected_at
        LIMIT :limit OFFSET :offset
    """)

    duplicates: List[Dict[str, Any]] = []
    with engine.connect() as conn:
        try:
            results = conn.execute(query, {
                "import_id": import_id,
                "limit": limit,
                "offset": offset
            })
        except ProgrammingError as exc:
            if "resolved_at" in str(exc):
                logger.warning("Missing resolved columns detected; recreating import tracking tables")
                create_import_history_table()
                results = conn.execute(query, {
                    "import_id": import_id,
                    "limit": limit,
                    "offset": offset
                })
            else:
                raise

        for row in results:
            record_data = row[2]
            if isinstance(record_data, str):
                try:
                    record_data = json.loads(record_data)
                except json.JSONDecodeError:
                    record_data = {}
            if not isinstance(record_data, dict):
                record_data = {}
            record_data = _make_json_safe(record_data)

            resolution_details = row[6]
            if isinstance(resolution_details, str):
                try:
                    resolution_details = json.loads(resolution_details)
                except json.JSONDecodeError:
                    resolution_details = None
            if resolution_details is not None and not isinstance(resolution_details, dict):
                resolution_details = {"value": resolution_details}
            if resolution_details is not None:
                resolution_details = _make_json_safe(resolution_details)
            duplicates.append({
                "id": row[0],
                "record_number": row[1],
                "record": record_data,
                "detected_at": row[3].isoformat() if row[3] else None,
                "resolved_at": row[4].isoformat() if row[4] else None,
                "resolved_by": row[5],
                "resolution_details": resolution_details
            })
    return duplicates


def _load_mapping_config(mapping_config_data: Optional[Dict[str, Any]]) -> Optional[MappingConfig]:
    if not mapping_config_data:
        return None
    try:
        return MappingConfig.model_validate(mapping_config_data)
    except Exception as exc:
        logger.warning("Failed to load mapping config for duplicate merge: %s", exc)
        return None


def _get_uniqueness_columns(mapping_config: Optional[MappingConfig], record: Dict[str, Any]) -> List[str]:
    if mapping_config and mapping_config.duplicate_check and mapping_config.duplicate_check.uniqueness_columns:
        return mapping_config.duplicate_check.uniqueness_columns
    # Default to all record columns, excluding metadata
    return [col for col in record.keys() if not col.startswith("_")]


def _fetch_existing_row(
    conn,
    table_name: str,
    record: Dict[str, Any],
    uniqueness_columns: List[str]
) -> Optional[Tuple[int, Dict[str, Any]]]:
    if not uniqueness_columns:
        return None
    conditions = []
    params: Dict[str, Any] = {}
    for idx, col in enumerate(uniqueness_columns):
        if col not in record:
            continue
        param_name = f"p_{idx}"
        conditions.append(f'"{col}" IS NULL' if record[col] is None else f'"{col}" = :{param_name}')
        if record[col] is not None:
            params[param_name] = record[col]

    if not conditions:
        return None

    where_clause = " AND ".join(conditions)
    query = text(f'SELECT * FROM "{table_name}" WHERE {where_clause} LIMIT 1')
    result = conn.execute(query, params).mappings().fetchone()
    if not result:
        return None

    row_dict = dict(result)
    row_id = row_dict.get("_row_id")
    return row_id, row_dict


def get_duplicate_row_detail(import_id: str, duplicate_id: int) -> Dict[str, Any]:
    """
    Retrieve detailed information for a specific duplicate row, including the
    matching existing row (if found).
    """
    engine = get_engine()

    try:
        with engine.connect() as conn:
            dup_query = text("""
                SELECT id, record_number, record_data, detected_at, resolved_at, resolved_by, resolution_details
                FROM import_duplicates
                WHERE import_id = :import_id AND id = :duplicate_id
            """)
            dup_row = conn.execute(dup_query, {
                "import_id": import_id,
                "duplicate_id": duplicate_id
            }).fetchone()

            if not dup_row:
                raise ValueError("Duplicate record not found")

            record_data = dup_row[2]
            if isinstance(record_data, str):
                try:
                    record_data = json.loads(record_data)
                except json.JSONDecodeError:
                    record_data = {}
            record_data = _make_json_safe(record_data)
            resolution_details = dup_row[6]
            if isinstance(resolution_details, str):
                try:
                    resolution_details = json.loads(resolution_details)
                except json.JSONDecodeError:
                    resolution_details = None
            if resolution_details is not None:
                resolution_details = _make_json_safe(resolution_details)

            duplicate_record = {
                "id": dup_row[0],
                "record_number": dup_row[1],
                "record": record_data,
                "detected_at": dup_row[3].isoformat() if dup_row[3] else None,
                "resolved_at": dup_row[4].isoformat() if dup_row[4] else None,
                "resolved_by": dup_row[5],
                "resolution_details": resolution_details
            }

            import_records = get_import_history(import_id=import_id, limit=1)
            if not import_records:
                raise ValueError("Import history not found for duplicate")

            history_record = import_records[0]
            table_name = history_record["table_name"]
            mapping_config = _load_mapping_config(history_record.get("mapping_config"))

            uniqueness_columns = _get_uniqueness_columns(mapping_config, duplicate_record["record"])

            existing_row_info = _fetch_existing_row(
                conn,
                table_name,
                duplicate_record["record"],
                uniqueness_columns
            )

            existing_row_payload = None
            if existing_row_info:
                row_id, row_values = existing_row_info
                cleaned_record = {
                    key: _make_json_safe(value)
                    for key, value in row_values.items()
                    if not key.startswith("_")
                }
                existing_row_payload = {
                    "row_id": row_id,
                    "record": cleaned_record
                }

            return {
                "duplicate": duplicate_record,
                "existing_row": existing_row_payload,
                "table_name": table_name,
                "uniqueness_columns": uniqueness_columns
            }
    except Exception as e:
        logger.error("Error retrieving duplicate detail: %s", e)
        raise


def resolve_duplicate_row(
    import_id: str,
    duplicate_id: int,
    updates: Dict[str, Any],
    resolved_by: Optional[str] = None,
    note: Optional[str] = None
) -> Dict[str, Any]:
    """
    Apply updates from a duplicate record to the existing row and mark the
    duplicate as resolved.
    """
    engine = get_engine()

    detail = get_duplicate_row_detail(import_id, duplicate_id)
    duplicate_record = detail["duplicate"]
    existing_row = detail["existing_row"]

    if duplicate_record.get("resolved_at"):
        raise ValueError("Duplicate record already resolved")

    valid_updates: Dict[str, Any] = {
        column: value
        for column, value in updates.items()
        if column in duplicate_record["record"]
    }

    with engine.begin() as conn:
        if existing_row is None:
            raise ValueError("No existing row found to merge into")

        row_id = existing_row["row_id"]
        table_name = detail["table_name"]

        # Prepare update statement
        set_clauses = []
        params: Dict[str, Any] = {"row_id": row_id}
        for column, value in valid_updates.items():
            if column.startswith("_"):
                continue
            param_name = f"set_{column}"
            set_clauses.append(f'"{column}" = :{param_name}')
            params[param_name] = value

        updated_columns: List[str] = list(valid_updates.keys())
        if set_clauses:
            update_sql = text(f'''
                UPDATE "{table_name}"
                SET {", ".join(set_clauses)}
                WHERE _row_id = :row_id
            ''')
            conn.execute(update_sql, params)

        resolution_details = {
            "updated_columns": updated_columns,
            "note": note
        }

        conn.execute(text("""
            UPDATE import_duplicates
            SET resolved_at = NOW(),
                resolved_by = :resolved_by,
                resolution_details = :resolution_details
            WHERE id = :duplicate_id
        """), {
            "resolved_by": resolved_by,
            "resolution_details": json.dumps(resolution_details),
            "duplicate_id": duplicate_id
        })

        resolved_timestamp = datetime.now(timezone.utc)
        duplicate_record["resolved_at"] = resolved_timestamp.isoformat()
        duplicate_record["resolved_by"] = resolved_by
        duplicate_record["resolution_details"] = resolution_details

        refreshed_row = conn.execute(text(f'''
            SELECT * FROM "{table_name}"
            WHERE _row_id = :row_id
        '''), {"row_id": row_id}).mappings().fetchone()

        remaining = conn.execute(text("""
            SELECT COUNT(*) FROM import_duplicates
            WHERE import_id = :import_id AND resolved_at IS NULL
        """), {"import_id": import_id}).scalar() or 0

        conn.execute(text("""
            UPDATE import_history
            SET duplicates_found = :remaining
            WHERE import_id = :import_id
        """), {"remaining": remaining, "import_id": import_id})

        cleaned_record = {
            key: _make_json_safe(value)
            for key, value in dict(refreshed_row).items()
            if not key.startswith("_")
        } if refreshed_row else {}

        return {
            "duplicate": duplicate_record,
            "updated_columns": updated_columns,
            "existing_row": {
                "row_id": row_id,
                "record": cleaned_record
            },
            "resolution_details": resolution_details
        }


def get_import_history(
    import_id: Optional[str] = None,
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Retrieve import history records with optional filters.
    
    Args:
        import_id: Filter by specific import ID
        table_name: Filter by table name
        user_id: Filter by user ID
        status: Filter by status
        limit: Maximum number of records to return
        offset: Number of records to skip
        
    Returns:
        List of import history records
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            # Build query with filters
            where_clauses = []
            params = {"limit": limit, "offset": offset}
            
            if import_id:
                where_clauses.append("import_id = :import_id")
                params["import_id"] = import_id
            if table_name:
                where_clauses.append("table_name = :table_name")
                params["table_name"] = table_name
            if user_id:
                where_clauses.append("user_id = :user_id")
                params["user_id"] = user_id
            if status:
                where_clauses.append("status = :status")
                params["status"] = status
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query = f"""
            SELECT * FROM import_history
            WHERE {where_sql}
            ORDER BY import_timestamp DESC
            LIMIT :limit OFFSET :offset
            """
            
            result = conn.execute(text(query), params)
            
            records = []
            for row in result:
                records.append({
                    "import_id": str(row[0]),
                    "import_timestamp": row[1].isoformat() if row[1] else None,
                    "user_id": row[2],
                    "user_email": row[3],
                    "source_type": row[4],
                    "source_path": row[5],
                    "file_name": row[6],
                    "file_size_bytes": row[7],
                    "file_type": row[8],
                    "file_hash": row[9],
                    "table_name": row[10],
                    "import_strategy": row[11],
                    "mapping_config": row[12],
                    "duplicate_check_enabled": row[13],
                    "status": row[14],
                    "error_message": row[15],
                    "warnings": row[16],
                    "mapping_status": row[17],
                    "mapping_started_at": row[18].isoformat() if row[18] else None,
                    "mapping_completed_at": row[19].isoformat() if row[19] else None,
                    "mapping_duration_seconds": float(row[20]) if row[20] else None,
                    "mapping_errors_count": row[21],
                    "total_rows_in_file": row[22],
                    "rows_processed": row[23],
                    "rows_inserted": row[24],
                    "rows_skipped": row[25],
                    "duplicates_found": row[26],
                    "data_validation_errors": row[27],
                    "duration_seconds": float(row[28]) if row[28] else None,
                    "parsing_time_seconds": float(row[29]) if row[29] else None,
                    "duplicate_check_time_seconds": float(row[30]) if row[30] else None,
                    "insert_time_seconds": float(row[31]) if row[31] else None,
                    "analysis_id": str(row[32]) if row[32] else None,
                    "task_id": str(row[33]) if row[33] else None,
                    "metadata": row[34],
                    "created_at": row[35].isoformat() if row[35] else None,
                    "updated_at": row[36].isoformat() if row[36] else None
                })
            
            return records
            
    except Exception as e:
        logger.error(f"Error retrieving import history: {str(e)}")
        raise


def _make_json_safe(value: Any) -> Any:
    """
    Convert Python objects into JSON-serialisable structures, preserving
    as much fidelity as possible.
    """
    if isinstance(value, dict):
        return {key: _make_json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, set):
        return [_make_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        # Keep integers as ints, otherwise convert to string to avoid precision loss
        if value == value.to_integral():
            return int(value)
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode(errors="ignore")
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    # Fallback to string representation for unsupported types
    return str(value)


def get_import_statistics(
    table_name: Optional[str] = None,
    user_id: Optional[str] = None,
    days: int = 30
) -> Dict[str, Any]:
    """
    Get aggregate statistics about imports.
    
    Args:
        table_name: Filter by table name
        user_id: Filter by user ID
        days: Number of days to look back
        
    Returns:
        Dictionary with aggregate statistics
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            where_clauses = ["import_timestamp >= NOW() - INTERVAL ':days days'"]
            params = {"days": days}
            
            if table_name:
                where_clauses.append("table_name = :table_name")
                params["table_name"] = table_name
            if user_id:
                where_clauses.append("user_id = :user_id")
                params["user_id"] = user_id
            
            where_sql = " AND ".join(where_clauses)
            
            query = f"""
            SELECT
                COUNT(*) as total_imports,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_imports,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_imports,
                SUM(rows_inserted) as total_rows_inserted,
                SUM(duplicates_found) as total_duplicates_found,
                AVG(duration_seconds) as avg_duration_seconds,
                COUNT(DISTINCT table_name) as tables_affected,
                COUNT(DISTINCT user_id) as unique_users
            FROM import_history
            WHERE {where_sql}
            """
            
            result = conn.execute(text(query), params)
            row = result.fetchone()
            
            if row:
                return {
                    "total_imports": row[0] or 0,
                    "successful_imports": row[1] or 0,
                    "failed_imports": row[2] or 0,
                    "total_rows_inserted": row[3] or 0,
                    "total_duplicates_found": row[4] or 0,
                    "avg_duration_seconds": float(row[5]) if row[5] else 0.0,
                    "tables_affected": row[6] or 0,
                    "unique_users": row[7] or 0
                }
            
            return {}
            
    except Exception as e:
        logger.error(f"Error retrieving import statistics: {str(e)}")
        raise


def get_table_import_lineage(table_name: str) -> List[Dict[str, Any]]:
    """
    Get all imports that contributed data to a specific table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of import records for this table, ordered by timestamp
    """
    return get_import_history(table_name=table_name, status="success", limit=1000)
