"""
Export router for large SQL query result downloads.

This endpoint bypasses the LLM agent and allows direct SQL execution
for exporting large datasets as CSV files.
"""
import re
import time
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from io import StringIO
import csv

from app.db.session import get_engine
from app.core.config import settings
from app.core.api_key_auth import get_api_key_from_header


router = APIRouter(
    prefix="/api/export",
    tags=["export"]
)


# Protected system tables (same as agent.py)
PROTECTED_SYSTEM_TABLES = {
    'import_history',
    'mapping_errors',
    'table_metadata',
    'uploaded_files',
    'users',
    'file_imports',
    'import_jobs',
    'llm_instructions',
    'api_keys',
    'import_duplicates',
    'query_messages',
    'query_threads',
    'table_fingerprints',
}


class ExportQueryRequest(BaseModel):
    """Request model for export query endpoint."""
    sql_query: str = Field(..., description="SQL SELECT query to execute")
    filename: Optional[str] = Field(None, description="Optional filename for the download (defaults to 'export.csv')")


def validate_export_query(sql_query: str) -> tuple[bool, Optional[str]]:
    """
    Validate SQL query for export endpoint.
    
    Returns:
        tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    # Must be a SELECT query (handle queries starting with parentheses for UNION)
    query_stripped = sql_query.strip().upper()
    if not (query_stripped.startswith('SELECT') or query_stripped.startswith('(SELECT')):
        return False, "Only SELECT queries are allowed for security reasons."
    
    # Check for protected system tables
    sql_upper = sql_query.upper()
    for table in PROTECTED_SYSTEM_TABLES:
        table_patterns = [
            rf'\bFROM\s+["\']?{table.upper()}["\']?\b',
            rf'\bJOIN\s+["\']?{table.upper()}["\']?\b',
            rf'\bFROM\s+PUBLIC\.{table.upper()}\b',
            rf'\bJOIN\s+PUBLIC\.{table.upper()}\b'
        ]
        
        for pattern in table_patterns:
            if re.search(pattern, sql_upper):
                return False, f"Access to system table '{table}' is not allowed."
    
    # Check for dangerous operations
    dangerous_patterns = [
        r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b',
        r';\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)',
        r'--.*',
        r'/\*.*?\*/'
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, sql_query, re.IGNORECASE | re.MULTILINE):
            return False, "Query contains forbidden operations."
    
    return True, None


def generate_csv_stream(rows, columns):
    """
    Generator function to stream CSV data row by row.
    
    Args:
        rows: Database result rows
        columns: Column names
        
    Yields:
        CSV data chunks as strings
    """
    buffer = StringIO()
    writer = csv.writer(buffer)
    
    # Write header
    writer.writerow(columns)
    yield buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    
    # Write data rows
    for row in rows:
        writer.writerow(row)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)


@router.post("/query", dependencies=[Depends(get_api_key_from_header)])
async def export_query(request: ExportQueryRequest):
    """
    Execute a SQL query and stream results as CSV download.
    
    This endpoint is designed for large data exports and bypasses the LLM agent.
    It supports queries returning up to 100,000 rows (configurable via EXPORT_ROW_LIMIT).
    
    **Security:**
    - Only SELECT queries allowed
    - No access to protected system tables
    - 120-second timeout (configurable via EXPORT_TIMEOUT_SECONDS)
    - Requires API key authentication
    
    **Example Request:**
    ```json
    {
        "sql_query": "SELECT * FROM customers LIMIT 50000",
        "filename": "customers_export.csv"
    }
    ```
    
    **Response:**
    - Returns a CSV file download
    - Content-Disposition header triggers automatic download
    """
    try:
        # Validate the query
        is_valid, error_message = validate_export_query(request.sql_query)
        if not is_valid:
            raise HTTPException(status_code=400, detail=error_message)
        
        engine = get_engine()
        
        # Determine filename
        filename = request.filename or "export.csv"
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        start_time = time.time()
        
        with engine.connect() as conn:
            # Set timeout based on configuration
            timeout_ms = settings.export_timeout_seconds * 1000
            conn.execute(text(f"SET statement_timeout = '{timeout_ms}'"))
            
            # Execute query
            result = conn.execute(text(request.sql_query))
            columns = list(result.keys())
            
            # Fetch rows based on configuration
            rows = result.fetchmany(settings.export_row_limit)
            
            execution_time = time.time() - start_time
            
            if not rows:
                raise HTTPException(
                    status_code=404,
                    detail=f"Query executed successfully but returned no results. Execution time: {execution_time:.2f}s"
                )
            
            # Return streaming CSV response
            return StreamingResponse(
                generate_csv_stream(rows, columns),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "X-Row-Count": str(len(rows)),
                    "X-Execution-Time": f"{execution_time:.2f}s"
                }
            )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error executing export query: {str(e)}"
        )


@router.get("/health", dependencies=[])
async def export_health():
    """Health check endpoint for export service (no authentication required)."""
    return {
        "status": "healthy",
        "service": "export",
        "max_rows": settings.export_row_limit,
        "timeout_seconds": settings.export_timeout_seconds
    }
