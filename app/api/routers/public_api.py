"""
Public API endpoints with API key authentication for external applications.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db, get_engine
from app.api.schemas.shared import (
    QueryDatabaseRequest, QueryDatabaseResponse,
    TablesListResponse, TableInfo, TableSchemaResponse, ColumnInfo,
    is_reserved_system_table,
)
from app.core.api_key_auth import ApiKey, get_api_key_from_header
from app.domain.queries.agent import query_database_with_agent

router = APIRouter(prefix="/api/v1", tags=["public-api"])


@router.post("/query", response_model=QueryDatabaseResponse)
async def public_query_database_endpoint(
    request: QueryDatabaseRequest,
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    Execute natural language queries against the database (Public API).
    
    This endpoint is designed for external applications to query the database
    using natural language. It requires API key authentication.
    
    Authentication: X-API-Key header
    
    Parameters:
    - prompt: Natural language query
    - thread_id: Optional conversation thread ID for memory continuity
    
    Returns:
    - Query results in CSV format
    - Executed SQL query
    - Execution metadata
    """
    try:
        # Execute query using the same agent as internal endpoint
        result = query_database_with_agent(request.prompt, thread_id=request.thread_id)

        return QueryDatabaseResponse(
            success=result["success"],
            response=result["response"],
            executed_sql=result.get("executed_sql"),
            data_csv=result.get("data_csv"),
            execution_time_seconds=result.get("execution_time_seconds"),
            rows_returned=result.get("rows_returned"),
            error=result.get("error")
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")


@router.get("/tables", response_model=TablesListResponse)
async def public_list_tables_endpoint(
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    List all available tables (Public API).
    
    Authentication: X-API-Key header
    
    Returns:
    - List of table names with row counts
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Query information_schema for user-created tables (exclude system tables)
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name NOT IN ('spatial_ref_sys', 'geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews',
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors', 'import_jobs', 'import_duplicates', 'mapping_chunk_status', 'api_keys', 'query_messages', 'query_threads')
                AND table_name NOT LIKE 'pg_%'
                AND table_name NOT LIKE 'test\_%' ESCAPE '\\'
                ORDER BY table_name
            """))

            tables = []
            for row in result:
                table_name = row[0]

                # Get row count for each table
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
                row_count = count_result.scalar()

                tables.append(TableInfo(
                    table_name=table_name,
                    row_count=row_count
                ))

        return TablesListResponse(success=True, tables=tables)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/{table_name}/schema", response_model=TableSchemaResponse)
async def public_get_table_schema_endpoint(
    table_name: str,
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    Get table schema information (Public API).
    
    Authentication: X-API-Key header
    
    Parameters:
    - table_name: Name of the table
    
    Returns:
    - Table column information
    """
    try:
        if is_reserved_system_table(table_name):
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get column information
            columns_result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            columns = []
            for row in columns_result:
                columns.append(ColumnInfo(
                    name=row[0],
                    type=row[1],
                    nullable=row[2].upper() == 'YES'
                ))

        return TableSchemaResponse(
            success=True,
            table_name=table_name,
            columns=columns
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
