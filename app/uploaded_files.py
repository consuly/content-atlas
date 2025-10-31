"""
Database operations for tracking uploaded files.
"""
from sqlalchemy import text, Table, Column, String, BigInteger, Integer, DateTime, Text, MetaData
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
from typing import List, Dict, Optional
from .database import get_engine
import uuid


def create_uploaded_files_table():
    """Create the uploaded_files table if it doesn't exist."""
    engine = get_engine()
    
    # Enable pgcrypto extension for gen_random_uuid()
    enable_extension_sql = """
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    """
    
    create_table_sql = """
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
    );
    
    CREATE INDEX IF NOT EXISTS idx_uploaded_files_status ON uploaded_files(status);
    CREATE INDEX IF NOT EXISTS idx_uploaded_files_file_name ON uploaded_files(file_name);
    CREATE INDEX IF NOT EXISTS idx_uploaded_files_file_hash ON uploaded_files(file_hash);
    CREATE INDEX IF NOT EXISTS idx_uploaded_files_upload_date ON uploaded_files(upload_date DESC);
    """
    
    with engine.connect() as conn:
        # First enable the extension
        conn.execute(text(enable_extension_sql))
        # Then create the table
        conn.execute(text(create_table_sql))
        conn.commit()
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
    
    with engine.connect() as conn:
        result = conn.execute(text(insert_sql), {
            "id": file_id,
            "file_name": file_name,
            "b2_file_id": b2_file_id,
            "b2_file_path": b2_file_path,
            "file_size": file_size,
            "file_hash": file_hash,
            "content_type": content_type,
            "user_id": user_id
        })
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


def get_uploaded_files(
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict]:
    """Get list of uploaded files with optional filters."""
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
    
    with engine.connect() as conn:
        result = conn.execute(text(query_sql), params)
        
        files = []
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


def get_uploaded_file_by_id(file_id: str) -> Optional[Dict]:
    """Get a specific uploaded file by ID."""
    engine = get_engine()
    
    query_sql = """
    SELECT 
        id, file_name, b2_file_id, b2_file_path, file_size,
        content_type, upload_date, status, mapped_table_name,
        mapped_date, mapped_rows, error_message
    FROM uploaded_files
    WHERE id = :file_id
    """
    
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


def get_uploaded_file_by_name(file_name: str) -> Optional[Dict]:
    """Check if a file with this name already exists."""
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


def get_uploaded_file_by_hash(file_hash: str) -> Optional[Dict]:
    """Check if a file with this hash already exists (duplicate detection)."""
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


def update_file_status(
    file_id: str,
    status: str,
    mapped_table_name: Optional[str] = None,
    mapped_rows: Optional[int] = None,
    error_message: Optional[str] = None
) -> bool:
    """Update the status of an uploaded file."""
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
    
    with engine.connect() as conn:
        result = conn.execute(text(update_sql), params)
        conn.commit()
        return result.rowcount > 0


def delete_uploaded_file(file_id: str) -> bool:
    """Delete an uploaded file record."""
    engine = get_engine()
    
    delete_sql = "DELETE FROM uploaded_files WHERE id = :file_id"
    
    with engine.connect() as conn:
        result = conn.execute(text(delete_sql), {"file_id": file_id})
        conn.commit()
        return result.rowcount > 0


def get_uploaded_files_count(status: Optional[str] = None) -> int:
    """Get total count of uploaded files."""
    engine = get_engine()
    
    where_clause = "WHERE status = :status" if status else ""
    params = {"status": status} if status else {}
    
    query_sql = f"""
    SELECT COUNT(*) FROM uploaded_files {where_clause}
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query_sql), params)
        return result.scalar()
