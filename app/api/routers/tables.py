"""
Table management endpoints for querying and inspecting database tables.
"""
import csv
import json
from io import StringIO
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db, get_engine
from app.api.schemas.shared import (
    TablesListResponse, TableInfo, TableDataResponse,
    TableSchemaResponse, ColumnInfo, TableStatsResponse,
    is_reserved_system_table,
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
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors', 'import_jobs', 'import_duplicates', 'mapping_chunk_status', 'api_keys', 'query_messages', 'query_threads', 'llm_instructions', 'table_fingerprints')
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
    import_id: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_order: str = "asc",
    filters: Optional[str] = Query(
        default=None,
        description='JSON list of filters: [{"column":"status","operator":"eq","value":"Active"}]',
    ),
    db: Session = Depends(get_db)
):
    """
    Query table data with pagination, optional search, sorting, and filtering.
    
    Parameters:
    - import_id: Optional UUID to filter rows by specific import
    """
    try:
        if is_reserved_system_table(table_name):
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        limit = min(limit, 500)
        operator_map = {"eq": "=", "neq": "!=", "contains": "ILIKE"}

        parsed_filters: List[Dict[str, Any]] = []
        if filters:
            try:
                raw_filters = json.loads(filters)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid filters format. Must be valid JSON.")

            if isinstance(raw_filters, dict):
                raw_filters = [raw_filters]
            if not isinstance(raw_filters, list):
                raise HTTPException(status_code=400, detail="Filters must be provided as a list.")

            for filter_obj in raw_filters:
                if not isinstance(filter_obj, dict):
                    raise HTTPException(status_code=400, detail="Each filter must be an object.")

                column = filter_obj.get("column")
                operator = (filter_obj.get("operator") or "eq").lower()
                value = filter_obj.get("value")

                if not column or value is None:
                    raise HTTPException(status_code=400, detail="Filters require 'column' and 'value'.")
                if operator not in operator_map:
                    raise HTTPException(status_code=400, detail=f"Unsupported operator '{operator}'.")

                parsed_filters.append({"column": column, "operator": operator, "value": value})

        engine = get_engine()
        with engine.connect() as conn:
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            columns_result = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name NOT LIKE '\\_%' ESCAPE '\\'
                ORDER BY ordinal_position
            """), {"table_name": table_name})
            
            user_columns = [row[0] for row in columns_result]
            if not user_columns:
                return TableDataResponse(
                    success=True,
                    table_name=table_name,
                    data=[],
                    total_rows=0,
                    limit=limit,
                    offset=offset
                )

            if sort_by and sort_by not in user_columns:
                raise HTTPException(status_code=400, detail=f"Invalid sort column '{sort_by}'.")

            columns_sql = ', '.join([f'"{col}"' for col in user_columns])

            where_clauses: List[str] = []
            query_params: Dict[str, Any] = {}

            # Filter by import_id if provided
            if import_id:
                where_clauses.append('"_import_id" = :import_id')
                query_params["import_id"] = import_id

            for idx, filter_obj in enumerate(parsed_filters):
                column = filter_obj["column"]
                if column not in user_columns:
                    raise HTTPException(status_code=400, detail=f"Invalid filter column '{column}'.")

                operator = filter_obj["operator"]
                value = filter_obj["value"]
                param_name = f"filter_{idx}"

                if operator == "contains":
                    where_clauses.append(f'CAST("{column}" AS TEXT) ILIKE :{param_name}')
                    query_params[param_name] = f"%{value}%"
                else:
                    sql_operator = operator_map[operator]
                    where_clauses.append(f'"{column}" {sql_operator} :{param_name}')
                    query_params[param_name] = value

            if search:
                query_params["search_value"] = f"%{search}%"
                search_conditions = [
                    f'CAST("{column}" AS TEXT) ILIKE :search_value'
                    for column in user_columns
                ]
                where_clauses.append(f"({' OR '.join(search_conditions)})")

            where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

            order_fragment = 'ORDER BY "_row_id"'
            if sort_by:
                direction = "DESC" if sort_order.lower() == "desc" else "ASC"
                order_fragment = f'ORDER BY "{sort_by}" {direction}'

            count_sql = f'SELECT COUNT(*) FROM "{table_name}" {where_sql}'
            total_rows = conn.execute(text(count_sql), query_params).scalar()

            data_sql = f"""
                SELECT {columns_sql} FROM "{table_name}"
                {where_sql}
                {order_fragment}
                LIMIT :limit OFFSET :offset
            """
            data_params = dict(query_params)
            data_params.update({"limit": limit, "offset": offset})

            data_result = conn.execute(text(data_sql), data_params)
            columns = data_result.keys()
            raw_rows = [dict(zip(columns, row)) for row in data_result]
            data = [
                {key: value for key, value in row.items() if not key.startswith('_')}
                for row in raw_rows
            ]

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


@router.get("/{table_name}/export")
async def export_table(
    table_name: str,
    limit: Optional[int] = Query(
        default=None,
        ge=1,
        le=1_000_000,
        description="Optional maximum number of rows to include in the export",
    ),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Stream the full contents of a user table as CSV.

    The export excludes metadata columns (prefixed with "_") and returns a streaming
    CSV response so large tables do not exhaust memory.
    """
    try:
        if is_reserved_system_table(table_name):
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

        engine = get_engine()
        conn = engine.connect()
        try:
            table_check = conn.execute(
                text(
                    """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = :table_name
                    """
                ),
                {"table_name": table_name},
            )

            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

            columns_result = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table_name
                    AND column_name NOT LIKE '\\_%' ESCAPE '\\'
                    ORDER BY ordinal_position
                    """
                ),
                {"table_name": table_name},
            )

            user_columns = [row[0] for row in columns_result]
            if not user_columns:
                raise HTTPException(status_code=400, detail="Table has no exportable columns.")

            row_id_exists = conn.execute(
                text(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                      AND column_name = '_row_id'
                    """
                ),
                {"table_name": table_name},
            ).first()

            order_fragment = 'ORDER BY "_row_id"' if row_id_exists else ""
            limit_fragment = []
            query_params: Dict[str, Any] = {}
            if limit is not None:
                limit_fragment.append("LIMIT :limit")
                query_params["limit"] = limit
            if offset:
                limit_fragment.append("OFFSET :offset")
                query_params["offset"] = offset

            columns_sql = ", ".join([f'"{col}"' for col in user_columns])
            base_sql = f'SELECT {columns_sql} FROM "{table_name}"'
            sql_parts = [base_sql]
            if order_fragment:
                sql_parts.append(order_fragment)
            if limit_fragment:
                sql_parts.append(" ".join(limit_fragment))

            query = text("\n".join(sql_parts))

            stream_result = conn.execution_options(stream_results=True).execute(query, query_params)

            def row_stream():
                buffer = StringIO()
                writer = csv.writer(buffer)

                try:
                    writer.writerow(user_columns)
                    yield buffer.getvalue()
                    buffer.seek(0)
                    buffer.truncate(0)

                    while True:
                        rows = stream_result.fetchmany(500)
                        if not rows:
                            break
                        for row in rows:
                            # Convert row to list using integer indices
                            writer.writerow([
                                "" if row[i] is None else row[i]
                                for i in range(len(user_columns))
                            ])
                        yield buffer.getvalue()
                        buffer.seek(0)
                        buffer.truncate(0)
                finally:
                    stream_result.close()
                    conn.close()

            filename = f"{table_name}.csv"
            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"'
            }

            return StreamingResponse(
                row_stream(),
                media_type="text/csv",
                headers=headers,
            )
        except HTTPException:
            conn.close()
            raise
        except Exception:
            conn.close()
            raise

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
                AND column_name NOT LIKE '\\_%' ESCAPE '\\'
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

            # Get total rows
            count_result = conn.execute(text(f"SELECT COUNT(*) FROM \"{table_name}\""))
            total_rows = count_result.scalar()

            # Get column count and data types
            columns_result = conn.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table_name
                AND column_name NOT LIKE '\\_%' ESCAPE '\\'
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
        if is_reserved_system_table(table_name):
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

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


@router.delete("/{table_name}")
async def delete_table(
    table_name: str,
    db: Session = Depends(get_db)
):
    """
    Delete a user-created table and clean up all related records.
    
    This endpoint:
    1. Drops the table from the database
    2. Cleans up import_history records
    3. Cleans up file_imports records
    4. Resets uploaded_files status to 'uploaded' (unmapped)
    
    Parameters:
    - table_name: Name of the table to delete
    
    Returns:
    - Success status and cleanup summary
    """
    try:
        # Validate table name and prevent deletion of system tables
        if is_reserved_system_table(table_name):
            raise HTTPException(
                status_code=403,
                detail=f"Cannot delete system table '{table_name}'. This table is protected."
            )
        
        engine = get_engine()
        
        with engine.begin() as conn:
            # Check if table exists
            table_check = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            """), {"table_name": table_name})
            
            if not table_check.fetchone():
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
            
            # Drop the table (CASCADE will handle foreign key constraints)
            conn.execute(text(f'DROP TABLE "{table_name}" CASCADE'))
            
            # Clean up import_history records
            import_history_result = conn.execute(text("""
                DELETE FROM import_history
                WHERE table_name = :table_name
            """), {"table_name": table_name})
            import_history_deleted = import_history_result.rowcount
            
            # Clean up file_imports records
            file_imports_result = conn.execute(text("""
                DELETE FROM file_imports
                WHERE table_name = :table_name
            """), {"table_name": table_name})
            file_imports_deleted = file_imports_result.rowcount
            
            # Reset uploaded_files status to 'uploaded' (unmapped state)
            uploaded_files_result = conn.execute(text("""
                UPDATE uploaded_files
                SET status = 'uploaded',
                    mapped_table_name = NULL,
                    mapped_date = NULL,
                    mapped_rows = NULL,
                    updated_at = NOW()
                WHERE mapped_table_name = :table_name
            """), {"table_name": table_name})
            uploaded_files_reset = uploaded_files_result.rowcount
        
        return {
            "success": True,
            "table_name": table_name,
            "message": f"Table '{table_name}' deleted successfully",
            "cleanup_summary": {
                "import_history_deleted": import_history_deleted,
                "file_imports_deleted": file_imports_deleted,
                "uploaded_files_reset": uploaded_files_reset
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete table: {str(e)}")
