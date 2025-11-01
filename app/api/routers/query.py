"""
Natural language database query endpoint using LangChain agent.
"""
from fastapi import APIRouter, HTTPException

from app.api.schemas.shared import QueryDatabaseRequest, QueryDatabaseResponse
from app.domain.queries.agent import query_database_with_agent

router = APIRouter(tags=["query"])


@router.post("/query-database", response_model=QueryDatabaseResponse)
async def query_database_endpoint(request: QueryDatabaseRequest):
    """
    Execute natural language queries against the database using LangChain agent with conversation memory.
    
    The agent remembers previous queries within the same thread_id, allowing for:
    - Follow-up questions: "Now filter for California only"
    - References to past results: "What was the total from the last query?"
    - Context-aware queries: "Show products" → "Which of those have low stock?"
    
    Parameters:
    - prompt: Natural language query
    - max_rows: Maximum rows to return (1-10000)
    - thread_id: Optional conversation thread ID for memory continuity
    
    Returns:
    - Natural language response
    - Executed SQL query
    - Results in CSV format
    - Execution metadata
    """
    try:
        # Pass thread_id to maintain conversation memory
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
