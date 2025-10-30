"""
Import history tracking for comprehensive data lineage and auditing.

This module provides functionality to track all data imports with detailed metadata,
enabling traceability, auditing, and rollback capabilities.
"""

from typing import Dict, Any, Optional, List
from sqlalchemy import text
from datetime import datetime
import uuid
import json
from .database import get_engine
from .schemas import MappingConfig
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
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_import_history_sql))
            conn.execute(text(create_mapping_errors_sql))
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
    engine = get_engine()
    import_id = str(uuid.uuid4())
    
    # Convert mapping config to JSON if provided
    mapping_json = None
    duplicate_check_enabled = True
    if mapping_config:
        mapping_json = mapping_config.model_dump()
        if mapping_config.duplicate_check:
            duplicate_check_enabled = mapping_config.duplicate_check.enabled
    
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
