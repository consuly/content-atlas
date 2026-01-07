"""
Public API endpoints with API key authentication for external applications.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db, get_engine
from app.api.schemas.shared import (
    QueryDatabaseRequest, QueryDatabaseResponse,
    GenerateSQLRequest, GenerateSQLResponse,
    TablesListResponse, TableInfo, TableSchemaResponse, ColumnInfo,
    is_reserved_system_table,
)
from app.core.api_key_auth import ApiKey, get_api_key_from_header
from app.domain.queries.agent import query_database_with_agent
from app.domain.queries.sql_generator import generate_sql_from_prompt

router = APIRouter(prefix="/api/v1", tags=["public-api"])


@router.post(
    "/query", 
    response_model=QueryDatabaseResponse,
    summary="Run Natural Language Query",
    description="Translates a natural language prompt into SQL, executes it, and returns both a conversational summary and CSV data."
)
async def public_query_database_endpoint(
    request: QueryDatabaseRequest,
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    **Execute natural language queries against the database.**
    
    This endpoint is designed for external applications to query the database using natural language.
    It automatically handles schema context and maintains conversation history if a `thread_id` is provided.

    **Authentication:**
    Requires `X-API-Key` header.

    **Features:**
    *   **Natural Language to SQL:** Converts English questions into accurate SQL.
    *   **Context Awareness:** Remembers previous questions in the thread.
    *   **Safety:** Read-only access; blocks modification queries.
    
    **Returns:**
    *   `response`: The AI's conversational answer.
    *   `data_csv`: The raw result set in CSV format.
    *   `executed_sql`: The actual SQL that was run.
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


@router.post(
    "/generate-sql",
    response_model=GenerateSQLResponse,
    summary="Generate SQL from Natural Language",
    description="Lightweight endpoint that converts natural language to SQL without executing it. Designed for the probe phase of large export workflows."
)
async def public_generate_sql_endpoint(
    request: GenerateSQLRequest,
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    **Generate SQL from natural language prompt without execution.**
    
    This endpoint is optimized for speed and uses a single LLM call to convert
    natural language descriptions into SQL queries. Unlike `/api/v1/query`, it
    does NOT execute the query or maintain conversation history.
    
    **Use Cases:**
    - Probe phase for large exports (convert NL → SQL, then use `/api/export/query`)
    - SQL preview/validation before execution
    - Batch SQL generation
    
    **Performance:**
    - Single LLM call: 5-15 seconds (vs 60-120s for full agent)
    - No database execution overhead
    - No conversation state

    **Authentication:**
    Requires `X-API-Key` header.

    **Request Body:**
    ```json
    {
        "prompt": "Get top 10000 clients with email and company",
        "table_hints": ["clients-list"]  // optional - narrows schema context
    }
    ```

    **Response:**
    ```json
    {
        "success": true,
        "sql_query": "SELECT email, company_name FROM \"clients-list\" LIMIT 10000",
        "tables_referenced": ["clients-list"],
        "explanation": "Selecting email and company columns from clients table"
    }
    ```
    
    **Errors:**
    - `400`: Invalid request or unsafe SQL generated
    - `500`: LLM or system error
    """
    try:
        # Generate SQL using the lightweight generator
        result = generate_sql_from_prompt(
            prompt=request.prompt,
            table_hints=request.table_hints
        )
        
        return GenerateSQLResponse(
            success=result["success"],
            sql_query=result.get("sql_query"),
            tables_referenced=result.get("tables_referenced"),
            explanation=result.get("explanation"),
            error=result.get("error")
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL generation failed: {str(e)}")


@router.get(
    "/tables", 
    response_model=TablesListResponse,
    summary="List Available Tables",
    description="Lists all user-accessible tables along with their current row counts."
)
async def public_list_tables_endpoint(
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    **List all available reporting tables.**
    
    Returns a list of tables that are available for querying, including their name and current row count.
    System tables and internal metadata tables are excluded.

    **Authentication:**
    Requires `X-API-Key` header.
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
                                     'file_imports', 'table_metadata', 'import_history', 'uploaded_files', 'users', 'mapping_errors', 'import_jobs', 'import_duplicates', 'mapping_chunk_status', 'api_keys', 'query_messages', 'query_threads', 'llm_instructions', 'table_fingerprints', 'row_updates)
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


@router.get(
    "/tables/{table_name}/schema", 
    response_model=TableSchemaResponse,
    summary="Get Table Schema",
    description="Returns detailed column information for a specific table to help with validation or prompt construction."
)
async def public_get_table_schema_endpoint(
    table_name: str,
    api_key: ApiKey = Depends(get_api_key_from_header),
    db: Session = Depends(get_db)
):
    """
    **Get table schema information.**
    
    Returns the column names, data types, and nullability for the specified table.
    Useful for building validation logic or understanding the data structure before querying.

    **Authentication:**
    Requires `X-API-Key` header.

    **Errors:**
    *   `404`: If the table does not exist or is a protected system table.
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
