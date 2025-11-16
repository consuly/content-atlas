"""
Natural language database query endpoint using LangChain agent.
"""
import logging
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from app.api.schemas.shared import (
    QueryConversationListResponse,
    QueryConversationResponse,
    QueryDatabaseRequest,
    QueryDatabaseResponse,
)
from app.domain.queries.agent import query_database_with_agent
from app.domain.queries.history import (
    get_latest_query_conversation,
    get_query_conversation,
    list_query_threads,
    save_query_message,
)

router = APIRouter(tags=["query"])
router_v1 = APIRouter(prefix="/api/v1", tags=["query"])
logger = logging.getLogger(__name__)


def _log_and_wrap_error(action: str, error: Exception) -> HTTPException:
    """Centralize logging for API errors so frontend errors correlate with server logs."""
    logger.exception("Query conversation endpoint failed during %s: %s", action, error)
    return HTTPException(status_code=500, detail=f"Failed to {action}: {str(error)}")


@router.post("/query-database", response_model=QueryDatabaseResponse)
@router_v1.post("/query-database", response_model=QueryDatabaseResponse)
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
    thread_id = request.thread_id or str(uuid4())

    try:
        save_query_message(thread_id, "user", request.prompt)
    except Exception as log_error:  # pragma: no cover - best-effort logging
        logger.warning("Failed to persist user query message: %s", log_error)

    try:
        # Pass thread_id to maintain conversation memory
        result = query_database_with_agent(request.prompt, thread_id=thread_id)

        try:
            save_query_message(
                thread_id,
                "assistant",
                result.get("response", ""),
                executed_sql=result.get("executed_sql"),
                data_csv=result.get("data_csv"),
                execution_time_seconds=result.get("execution_time_seconds"),
                rows_returned=result.get("rows_returned"),
                error=result.get("error"),
            )
        except Exception as log_error:  # pragma: no cover - best-effort logging
            logger.warning("Failed to persist assistant message: %s", log_error)

        logger.info("Query completed for thread_id=%s with success=%s", thread_id, result["success"])

        return QueryDatabaseResponse(
            success=result["success"],
            response=result["response"],
            thread_id=thread_id,
            executed_sql=result.get("executed_sql"),
            data_csv=result.get("data_csv"),
            execution_time_seconds=result.get("execution_time_seconds"),
            rows_returned=result.get("rows_returned"),
            error=result.get("error")
        )

    except Exception as e:
        raise _log_and_wrap_error("process query", e)


@router.get("/query-conversations/latest", response_model=QueryConversationResponse)
@router_v1.get("/query-conversations/latest", response_model=QueryConversationResponse)
async def get_latest_conversation():
    """Load the most recent Query Database conversation from Postgres."""

    try:
        conversation = get_latest_query_conversation()
        logger.info(
            "Latest conversation fetch %s",
            "hit" if conversation else "returned empty (no conversations yet)",
        )
        return QueryConversationResponse(success=True, conversation=conversation)
    except Exception as e:
        raise _log_and_wrap_error("load latest conversation", e)


@router.get("/query-conversations/{thread_id}", response_model=QueryConversationResponse)
@router_v1.get("/query-conversations/{thread_id}", response_model=QueryConversationResponse)
async def get_conversation(thread_id: str):
    """Load a saved conversation by thread ID."""

    try:
        conversation = get_query_conversation(thread_id)
        # If there's no record for this thread, treat it as empty rather than erroring
        if conversation and not conversation.get("messages"):
            return QueryConversationResponse(success=True, conversation=None)

        return QueryConversationResponse(success=True, conversation=conversation)
    except Exception as e:
        raise _log_and_wrap_error("load conversation", e)


@router.get("/query-conversations", response_model=QueryConversationListResponse)
@router_v1.get("/query-conversations", response_model=QueryConversationListResponse)
async def list_conversations(limit: int = 50, offset: int = 0):
    """List saved query conversations ordered by most recently updated."""

    try:
        conversations = list_query_threads(limit=limit, offset=offset)
        logger.info(
            "Listed %s conversations (limit=%s, offset=%s)",
            len(conversations),
            limit,
            offset,
        )
        return QueryConversationListResponse(success=True, conversations=conversations)
    except Exception as e:
        raise _log_and_wrap_error("list conversations", e)
