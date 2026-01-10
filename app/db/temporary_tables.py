"""
Temporary tables management for data transformation workflows.

Temporary tables are user-created tables that:
- Are hidden from RAG/LLM context (unless explicitly referenced)
- Auto-expire after a configurable period (default: 7 days)
- Are not used for additional data imports by default
- Are clearly marked as temporary
"""
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.db.session import get_engine

logger = logging.getLogger(__name__)

DEFAULT_EXPIRATION_DAYS = 7


def create_temporary_tables_tracking_table_if_not_exists(engine: Engine):
    """Create temporary_tables tracking table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS temporary_tables (
        table_name VARCHAR(255) PRIMARY KEY,
        created_at TIMESTAMP DEFAULT NOW(),
        expires_at TIMESTAMP NOT NULL,
        created_by_user_id INTEGER,
        organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
        allow_additional_imports BOOLEAN DEFAULT FALSE,
        purpose TEXT,
        CONSTRAINT fk_organization FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE
    );
    
    CREATE INDEX IF NOT EXISTS idx_temporary_tables_org ON temporary_tables(organization_id);
    CREATE INDEX IF NOT EXISTS idx_temporary_tables_expires ON temporary_tables(expires_at);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("temporary_tables tracking table created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating temporary_tables tracking table: {str(e)}")
        raise


def mark_table_as_temporary(
    table_name: str,
    organization_id: int,
    expires_days: int = DEFAULT_EXPIRATION_DAYS,
    created_by_user_id: Optional[int] = None,
    allow_additional_imports: bool = False,
    purpose: Optional[str] = None,
    engine: Optional[Engine] = None
) -> bool:
    """
    Mark a table as temporary with automatic expiration.
    
    Args:
        table_name: Name of the table to mark as temporary
        organization_id: Organization that owns this table
        expires_days: Number of days until the table expires (default: 7)
        created_by_user_id: User who created this temporary table
        allow_additional_imports: Whether to allow additional data imports to this table
        purpose: Optional description of the table's purpose
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    if engine is None:
        engine = get_engine()
    
    expires_at = datetime.now() + timedelta(days=expires_days)
    
    try:
        with engine.begin() as conn:
            # Check if table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})
            
            if not table_check.fetchone():
                logger.error(f"Cannot mark non-existent table '{table_name}' as temporary")
                return False
            
            # Insert or update tracking record
            upsert_sql = """
            INSERT INTO temporary_tables 
                (table_name, expires_at, created_by_user_id, organization_id, 
                 allow_additional_imports, purpose)
            VALUES 
                (:table_name, :expires_at, :created_by_user_id, :organization_id,
                 :allow_additional_imports, :purpose)
            ON CONFLICT (table_name) DO UPDATE
            SET expires_at = EXCLUDED.expires_at,
                allow_additional_imports = EXCLUDED.allow_additional_imports,
                purpose = EXCLUDED.purpose
            """
            
            conn.execute(text(upsert_sql), {
                "table_name": table_name,
                "expires_at": expires_at,
                "created_by_user_id": created_by_user_id,
                "organization_id": organization_id,
                "allow_additional_imports": allow_additional_imports,
                "purpose": purpose
            })
        
        logger.info(f"Table '{table_name}' marked as temporary, expires at {expires_at}")
        return True
        
    except Exception as e:
        logger.error(f"Error marking table as temporary: {str(e)}")
        return False


def is_temporary_table(table_name: str, engine: Optional[Engine] = None) -> bool:
    """
    Check if a table is marked as temporary.
    
    Args:
        table_name: Name of the table to check
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        True if the table is temporary, False otherwise
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 1 FROM temporary_tables
                WHERE table_name = :table_name
            """), {"table_name": table_name})
            
            return result.fetchone() is not None
            
    except Exception as e:
        logger.error(f"Error checking if table is temporary: {str(e)}")
        return False


def get_temporary_table_info(table_name: str, engine: Optional[Engine] = None) -> Optional[Dict[str, Any]]:
    """
    Get information about a temporary table.
    
    Args:
        table_name: Name of the temporary table
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        Dictionary with temporary table info or None if not found
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, created_at, expires_at, created_by_user_id,
                       organization_id, allow_additional_imports, purpose
                FROM temporary_tables
                WHERE table_name = :table_name
            """), {"table_name": table_name})
            
            row = result.fetchone()
            if row:
                return {
                    "table_name": row[0],
                    "created_at": row[1],
                    "expires_at": row[2],
                    "created_by_user_id": row[3],
                    "organization_id": row[4],
                    "allow_additional_imports": row[5],
                    "purpose": row[6]
                }
            return None
            
    except Exception as e:
        logger.error(f"Error getting temporary table info: {str(e)}")
        return None


def list_temporary_tables(
    organization_id: Optional[int] = None,
    include_expired: bool = False,
    engine: Optional[Engine] = None
) -> List[Dict[str, Any]]:
    """
    List all temporary tables, optionally filtered by organization.
    
    Args:
        organization_id: Optional organization ID to filter by
        include_expired: Whether to include expired tables
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        List of dictionaries with temporary table information
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.connect() as conn:
            where_clauses = []
            params = {}
            
            if organization_id is not None:
                where_clauses.append("organization_id = :organization_id")
                params["organization_id"] = organization_id
            
            if not include_expired:
                where_clauses.append("expires_at > NOW()")
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query = f"""
                SELECT table_name, created_at, expires_at, created_by_user_id,
                       organization_id, allow_additional_imports, purpose
                FROM temporary_tables
                WHERE {where_sql}
                ORDER BY expires_at DESC
            """
            
            result = conn.execute(text(query), params)
            
            tables = []
            for row in result:
                tables.append({
                    "table_name": row[0],
                    "created_at": row[1],
                    "expires_at": row[2],
                    "created_by_user_id": row[3],
                    "organization_id": row[4],
                    "allow_additional_imports": row[5],
                    "purpose": row[6]
                })
            
            return tables
            
    except Exception as e:
        logger.error(f"Error listing temporary tables: {str(e)}")
        return []


def unmark_temporary_table(table_name: str, engine: Optional[Engine] = None) -> bool:
    """
    Remove temporary status from a table (convert to permanent).
    
    Args:
        table_name: Name of the table to convert to permanent
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                DELETE FROM temporary_tables
                WHERE table_name = :table_name
            """), {"table_name": table_name})
        
        logger.info(f"Table '{table_name}' unmarked as temporary (converted to permanent)")
        return True
        
    except Exception as e:
        logger.error(f"Error unmarking temporary table: {str(e)}")
        return False


def extend_temporary_table_expiration(
    table_name: str,
    additional_days: int,
    engine: Optional[Engine] = None
) -> bool:
    """
    Extend the expiration date of a temporary table.
    
    Args:
        table_name: Name of the temporary table
        additional_days: Number of days to extend the expiration
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE temporary_tables
                SET expires_at = expires_at + make_interval(days => :days)
                WHERE table_name = :table_name
            """), {"table_name": table_name, "days": additional_days})
        
        logger.info(f"Extended expiration for table '{table_name}' by {additional_days} days")
        return True
        
    except Exception as e:
        logger.error(f"Error extending temporary table expiration: {str(e)}")
        return False


def cleanup_expired_temporary_tables(engine: Optional[Engine] = None) -> Dict[str, Any]:
    """
    Delete all expired temporary tables and their tracking records.
    
    This should be run periodically (e.g., every 24 hours) to clean up
    expired temporary tables automatically.
    
    Args:
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        Dictionary with cleanup statistics
    """
    if engine is None:
        engine = get_engine()
    
    deleted_count = 0
    failed_count = 0
    deleted_tables = []
    failed_tables = []
    
    try:
        with engine.begin() as conn:
            # Get all expired tables
            result = conn.execute(text("""
                SELECT table_name FROM temporary_tables
                WHERE expires_at <= NOW()
            """))
            
            expired_tables = [row[0] for row in result]
            
            logger.info(f"Found {len(expired_tables)} expired temporary tables to clean up")
            
            for table_name in expired_tables:
                try:
                    # Drop the table
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    
                    # Remove tracking record
                    conn.execute(text("""
                        DELETE FROM temporary_tables
                        WHERE table_name = :table_name
                    """), {"table_name": table_name})
                    
                    deleted_count += 1
                    deleted_tables.append(table_name)
                    logger.info(f"Cleaned up expired temporary table: {table_name}")
                    
                except Exception as e:
                    failed_count += 1
                    failed_tables.append(table_name)
                    logger.error(f"Failed to clean up temporary table '{table_name}': {str(e)}")
        
        return {
            "success": True,
            "deleted_count": deleted_count,
            "failed_count": failed_count,
            "deleted_tables": deleted_tables,
            "failed_tables": failed_tables
        }
        
    except Exception as e:
        logger.error(f"Error during temporary tables cleanup: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "deleted_count": deleted_count,
            "failed_count": failed_count,
            "deleted_tables": deleted_tables,
            "failed_tables": failed_tables
        }


def allows_additional_imports(table_name: str, engine: Optional[Engine] = None) -> bool:
    """
    Check if a temporary table allows additional data imports.
    
    Args:
        table_name: Name of the temporary table
        engine: Optional database engine (will use default if not provided)
        
    Returns:
        True if additional imports are allowed, False otherwise
    """
    if engine is None:
        engine = get_engine()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT allow_additional_imports FROM temporary_tables
                WHERE table_name = :table_name
            """), {"table_name": table_name})
            
            row = result.fetchone()
            return row[0] if row else False
            
    except Exception as e:
        logger.error(f"Error checking import permission for temporary table: {str(e)}")
        return False
