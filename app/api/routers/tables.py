"""
Table management endpoints for querying and inspecting database tables.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db, get_engine
from app.api.schemas.shared import (
    TablesListResponse, TableInfo, TableDataResponse, 
    TableSchemaResponse, ColumnInfo, TableStatsResponse
)

router = APIRouter(prefix="/tables", tags=["tables"])


@router.get("", response_model=TablesListResponse)
async def list_tables(db: Session = Depends(get_db)):
    """
    List all dynamically created tables.
    
    Returns a list of all user-created tables in the database, excluding
    system tables and internal metadata tables.
    
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
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors')
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


@router.get("/{table_name}", response_model=TableDataResponse)
async def query_table(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Query table data with pagination.
    
    Retrieves data from a specific table with pagination support.
    Excludes internal metadata columns (those starting with underscore).
    
    Parameters:
    - table_name: Name of the table to query
    - limit: Maximum number of rows to return (default: 100)
    - offset: Number of rows to skip (default: 0)
    
    Returns:
    - Table data
    - Total row count
    - Pagination info
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get total row count
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
            total_rows = count_result.scalar()

            # Get column names excluding metadata columns (those starting with _)
            columns_result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name NOT LIKE '\\_%'
                ORDER BY ordinal_position
            """), {"table_name": table_name})
            
            user_columns = [row[0] for row in columns_result]
            columns_sql = ', '.join([f'"{col}"' for col in user_columns])

            # Get paginated data (excluding metadata columns)
            data_result = conn.execute(text(f"""
                SELECT {columns_sql} FROM \"{table_name}\"
                ORDER BY _row_id
                LIMIT :limit OFFSET :offset
            """), {"limit": limit, "offset": offset})

            columns = data_result.keys()
            data = [dict(zip(columns, row)) for row in data_result]

        return TableDataResponse(
            success=True,
            table_name=table_name,
            data=data,
            total_rows=total_rows,
            limit=limit,
            offset=offset
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/schema", response_model=TableSchemaResponse)
async def get_table_schema(table_name: str, db: Session = Depends(get_db)):
    """
    Get table column information.
    
    Returns detailed schema information for a specific table, including
    column names, data types, and nullability.
    
    Parameters:
    - table_name: Name of the table
    
    Returns:
    - Table schema with column details
    """
    try:
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


@router.get("/{table_name}/stats", response_model=TableStatsResponse)
async def get_table_stats(table_name: str, db: Session = Depends(get_db)):
    """
    Get basic table statistics.
    
    Returns summary statistics for a table including row count,
    column count, and data type distribution.
    
    Parameters:
    - table_name: Name of the table
    
    Returns:
    - Table statistics
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Validate table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            # Get total rows
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
            total_rows = count_result.scalar()

            # Get column count and data types
            columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name != 'id'  -- Exclude auto-generated id column
                ORDER BY ordinal_position
            """), {"table_name": table_name})

            data_types = {row[0]: row[1] for row in columns_result}
            columns_count = len(data_types)

        return TableStatsResponse(
            success=True,
            table_name=table_name,
            total_rows=total_rows,
            columns_count=columns_count,
            data_types=data_types
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{table_name}/lineage")
async def get_table_lineage(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Get import lineage for a specific table - all imports that contributed data.
    
    Parameters:
    - table_name: Name of the table to get lineage for
    
    Returns:
    - List of imports that contributed to this table
    - Total rows contributed
    """
    from app.domain.imports.history import get_table_import_lineage
    from app.api.schemas.shared import TableLineageResponse, ImportHistoryRecord
    
    try:
        records = get_table_import_lineage(table_name)
        
        # Convert to Pydantic models
        import_records = [ImportHistoryRecord(**record) for record in records]
        
        # Calculate total rows contributed
        total_rows = sum(r.rows_inserted or 0 for r in import_records)
        
        return TableLineageResponse(
            success=True,
            table_name=table_name,
            imports=import_records,
            total_imports=len(import_records),
            total_rows_contributed=total_rows
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve table lineage: {str(e)}")
