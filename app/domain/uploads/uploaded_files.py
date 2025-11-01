"""
Database operations for tracking uploaded files.
"""
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from typing import List, Dict, Optional, Callable, TypeVar
from app.db.session import get_engine
import threading
import uuid


_table_initialized = False
_table_init_lock = threading.Lock()
_T = TypeVar("_T")


def ensure_uploaded_files_table():
    """Create the uploaded_files table on-demand if it is missing."""
    global _table_initialized
    if _table_initialized:
        return

    with _table_init_lock:
        if _table_initialized:
            return
        create_uploaded_files_table()
        _table_initialized = True


def _reset_table_flag():
    """Mark the uploaded_files table as unavailable so it can be recreated."""
    global _table_initialized
    with _table_init_lock:
        _table_initialized = False


def _is_missing_table_error(error: ProgrammingError) -> bool:
    """Return True if the error indicates the uploaded_files table is missing."""
    origin = getattr(error, "orig", None)
    return getattr(origin, "pgcode", None) == "42P01"


def _run_with_table_retry(operation: Callable[[], _T]) -> _T:
    """Execute a database operation and recreate uploaded_files if it vanished."""
    try:
        return operation()
    except ProgrammingError as error:
        if not _is_missing_table_error(error):
            raise
        _reset_table_flag()
        ensure_uploaded_files_table()
        return operation()


def create_uploaded_files_table():
    """Create the uploaded_files table if it doesn't exist."""
    engine = get_engine()

    # Enable pgcrypto extension for gen_random_uuid()
    enable_extension_sql = """
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    """
    table_ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            file_name VARCHAR(255) NOT NULL,
            b2_file_id VARCHAR(255) NOT NULL,
            b2_file_path VARCHAR(500) NOT NULL,
            file_size BIGINT NOT NULL,
            file_hash VARCHAR(64),
            content_type VARCHAR(100),
            upload_date TIMESTAMP DEFAULT NOW(),
            status VARCHAR(50) DEFAULT 'uploaded',
            mapped_table_name VARCHAR(255),
            mapped_date TIMESTAMP,
            mapped_rows INTEGER,
            user_id VARCHAR(255),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        """,
        """CREATE INDEX IF NOT EXISTS idx_uploaded_files_status ON uploaded_files(status)""",
        """CREATE INDEX IF NOT EXISTS idx_uploaded_files_file_name ON uploaded_files(file_name)""",
        """CREATE INDEX IF NOT EXISTS idx_uploaded_files_file_hash ON uploaded_files(file_hash)""",
        """CREATE INDEX IF NOT EXISTS idx_uploaded_files_upload_date ON uploaded_files(upload_date DESC)"""
    ]

    with engine.begin() as conn:
        conn.execute(text(enable_extension_sql))
        for ddl in table_ddl_statements:
            conn.execute(text(ddl))

    global _table_initialized
    _table_initialized = True
    print("[OK] uploaded_files table created successfully")


def insert_uploaded_file(
    file_name: str,
    b2_file_id: str,
    b2_file_path: str,
    file_size: int,
    content_type: str = None,
    user_id: str = None,
    file_hash: str = None
) -> Dict:
    """Insert a new uploaded file record."""
    ensure_uploaded_files_table()
    engine = get_engine()
    file_id = str(uuid.uuid4())

    insert_sql = """
    INSERT INTO uploaded_files (
        id, file_name, b2_file_id, b2_file_path, file_size, 
        file_hash, content_type, user_id, status
    )
    VALUES (
        :id, :file_name, :b2_file_id, :b2_file_path, :file_size,
        :file_hash, :content_type, :user_id, 'uploaded'
    )
    RETURNING id, file_name, b2_file_id, b2_file_path, file_size, 
              content_type, upload_date, status
    """
    params = {
        "id": file_id,
        "file_name": file_name,
        "b2_file_id": b2_file_id,
        "b2_file_path": b2_file_path,
        "file_size": file_size,
        "file_hash": file_hash,
        "content_type": content_type,
        "user_id": user_id
    }

    def _insert() -> Dict:
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()

            row = result.fetchone()
            return {
                "id": str(row[0]),
                "file_name": row[1],
                "b2_file_id": row[2],
                "b2_file_path": row[3],
                "file_size": row[4],
                "content_type": row[5],
                "upload_date": row[6].isoformat() if row[6] else None,
                "status": row[7]
            }

    return _run_with_table_retry(_insert)


def get_uploaded_files(
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict]:
    """Get list of uploaded files with optional filters."""
    ensure_uploaded_files_table()
    engine = get_engine()

    where_clauses = []
    params = {"limit": limit, "offset": offset}
    
    if status:
        where_clauses.append("status = :status")
        params["status"] = status
    
    if user_id:
        where_clauses.append("user_id = :user_id")
        params["user_id"] = user_id
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query_sql = f"""
    SELECT 
        id, file_name, b2_file_id, b2_file_path, file_size,
        content_type, upload_date, status, mapped_table_name,
        mapped_date, mapped_rows, error_message
    FROM uploaded_files
    WHERE {where_sql}
    ORDER BY upload_date DESC
    LIMIT :limit OFFSET :offset
    """

    def _fetch() -> List[Dict]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), params)

            files: List[Dict] = []
            for row in result:
                files.append({
                    "id": str(row[0]),
                    "file_name": row[1],
                    "b2_file_id": row[2],
                    "b2_file_path": row[3],
                    "file_size": row[4],
                    "content_type": row[5],
                    "upload_date": row[6].isoformat() if row[6] else None,
                    "status": row[7],
                    "mapped_table_name": row[8],
                    "mapped_date": row[9].isoformat() if row[9] else None,
                    "mapped_rows": row[10],
                    "error_message": row[11]
                })

            return files

    return _run_with_table_retry(_fetch)


def get_uploaded_file_by_id(file_id: str) -> Optional[Dict]:
    """Get a specific uploaded file by ID."""
    ensure_uploaded_files_table()
    engine = get_engine()

    query_sql = """
    SELECT 
        id, file_name, b2_file_id, b2_file_path, file_size,
        content_type, upload_date, status, mapped_table_name,
        mapped_date, mapped_rows, error_message
    FROM uploaded_files
    WHERE id = :file_id
    """

    def _fetch() -> Optional[Dict]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), {"file_id": file_id})
            row = result.fetchone()

            if not row:
                return None

            return {
                "id": str(row[0]),
                "file_name": row[1],
                "b2_file_id": row[2],
                "b2_file_path": row[3],
                "file_size": row[4],
                "content_type": row[5],
                "upload_date": row[6].isoformat() if row[6] else None,
                "status": row[7],
                "mapped_table_name": row[8],
                "mapped_date": row[9].isoformat() if row[9] else None,
                "mapped_rows": row[10],
                "error_message": row[11]
            }

    return _run_with_table_retry(_fetch)


def get_uploaded_file_by_name(file_name: str) -> Optional[Dict]:
    """Check if a file with this name already exists."""
    ensure_uploaded_files_table()
    engine = get_engine()

    query_sql = """
    SELECT 
        id, file_name, b2_file_id, b2_file_path, file_size,
        content_type, upload_date, status
    FROM uploaded_files
    WHERE file_name = :file_name
    ORDER BY upload_date DESC
    LIMIT 1
    """

    def _fetch() -> Optional[Dict]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), {"file_name": file_name})
            row = result.fetchone()

            if not row:
                return None

            return {
                "id": str(row[0]),
                "file_name": row[1],
                "b2_file_id": row[2],
                "b2_file_path": row[3],
                "file_size": row[4],
                "content_type": row[5],
                "upload_date": row[6].isoformat() if row[6] else None,
                "status": row[7]
            }

    return _run_with_table_retry(_fetch)


def get_uploaded_file_by_hash(file_hash: str) -> Optional[Dict]:
    """Check if a file with this hash already exists (duplicate detection)."""
    ensure_uploaded_files_table()
    engine = get_engine()

    query_sql = """
    SELECT 
        id, file_name, b2_file_id, b2_file_path, file_size,
        content_type, upload_date, status
    FROM uploaded_files
    WHERE file_hash = :file_hash
    ORDER BY upload_date DESC
    LIMIT 1
    """

    def _fetch() -> Optional[Dict]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), {"file_hash": file_hash})
            row = result.fetchone()

            if not row:
                return None

            return {
                "id": str(row[0]),
                "file_name": row[1],
                "b2_file_id": row[2],
                "b2_file_path": row[3],
                "file_size": row[4],
                "content_type": row[5],
                "upload_date": row[6].isoformat() if row[6] else None,
                "status": row[7]
            }

    return _run_with_table_retry(_fetch)


def update_file_status(
    file_id: str,
    status: str,
    mapped_table_name: Optional[str] = None,
    mapped_rows: Optional[int] = None,
    error_message: Optional[str] = None
) -> bool:
    """Update the status of an uploaded file."""
    ensure_uploaded_files_table()
    engine = get_engine()
    
    update_parts = ["status = :status", "updated_at = NOW()"]
    params = {"file_id": file_id, "status": status}
    
    if status == "mapped" and mapped_table_name:
        update_parts.append("mapped_table_name = :mapped_table_name")
        update_parts.append("mapped_date = NOW()")
        params["mapped_table_name"] = mapped_table_name
        
        if mapped_rows is not None:
            update_parts.append("mapped_rows = :mapped_rows")
            params["mapped_rows"] = mapped_rows
    
    if error_message:
        update_parts.append("error_message = :error_message")
        params["error_message"] = error_message
    
    update_sql = f"""
    UPDATE uploaded_files
    SET {", ".join(update_parts)}
    WHERE id = :file_id
    """

    def _update() -> bool:
        with engine.connect() as conn:
            result = conn.execute(text(update_sql), params)
            conn.commit()
            return result.rowcount > 0

    return _run_with_table_retry(_update)


def delete_uploaded_file(file_id: str) -> bool:
    """Delete an uploaded file record."""
    ensure_uploaded_files_table()
    engine = get_engine()

    delete_sql = "DELETE FROM uploaded_files WHERE id = :file_id"

    def _delete() -> bool:
        with engine.connect() as conn:
            result = conn.execute(text(delete_sql), {"file_id": file_id})
            conn.commit()
            return result.rowcount > 0

    return _run_with_table_retry(_delete)


def get_uploaded_files_count(status: Optional[str] = None) -> int:
    """Get total count of uploaded files."""
    ensure_uploaded_files_table()
    engine = get_engine()

    where_clause = "WHERE status = :status" if status else ""
    params = {"status": status} if status else {}

    query_sql = f"""
    SELECT COUNT(*) FROM uploaded_files {where_clause}
    """

    def _count() -> int:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), params)
            return result.scalar()

    return _run_with_table_retry(_count)
