"""
Persistent storage helpers for Query Database conversations.

This module records each user/assistant exchange so the UI can load
previous conversations from Postgres instead of relying on localStorage.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from app.db.session import get_engine


_query_history_tables_initialized = False


def ensure_query_history_tables_exist() -> None:
    """
    Idempotently create query conversation tables if they have not been created yet.

    Tests may import persistence helpers without running FastAPI startup, so we
    defensively create the tables before any read/write operations.
    """
    global _query_history_tables_initialized
    if _query_history_tables_initialized:
        return

    create_query_history_tables()
    _query_history_tables_initialized = True


def create_query_history_tables() -> None:
    """Create tables for storing query conversations if they don't exist."""

    engine = get_engine()

    create_threads_sql = """
    CREATE TABLE IF NOT EXISTS query_threads (
        thread_id TEXT PRIMARY KEY,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    """

    create_messages_sql = """
    CREATE TABLE IF NOT EXISTS query_messages (
        id BIGSERIAL PRIMARY KEY,
        thread_id TEXT NOT NULL REFERENCES query_threads(thread_id) ON DELETE CASCADE,
        role VARCHAR(20) NOT NULL,
        content TEXT NOT NULL,
        executed_sql TEXT,
        data_csv TEXT,
        execution_time_seconds DOUBLE PRECISION,
        rows_returned INTEGER,
        error TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_query_messages_thread_created
        ON query_messages(thread_id, created_at);
    """

    with engine.begin() as conn:
        conn.execute(text(create_threads_sql))
        conn.execute(text(create_messages_sql))


def _is_missing_query_table_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        ("query_threads" in message or "query_messages" in message)
        and ("does not exist" in message or "UndefinedTable" in message)
    )


def save_query_message(
    thread_id: str,
    role: str,
    content: str,
    *,
    executed_sql: Optional[str] = None,
    data_csv: Optional[str] = None,
    execution_time_seconds: Optional[float] = None,
    rows_returned: Optional[int] = None,
    error: Optional[str] = None,
) -> None:
    """Persist a single message in a query conversation."""

    ensure_query_history_tables_exist()

    engine = get_engine()

    def _persist_message() -> None:
        with engine.begin() as conn:
            # Ensure thread exists for the message
            conn.execute(
                text(
                    """
                    INSERT INTO query_threads (thread_id)
                    VALUES (:thread_id)
                    ON CONFLICT (thread_id) DO NOTHING;
                    """
                ),
                {"thread_id": thread_id},
            )

            conn.execute(
                text(
                    """
                    INSERT INTO query_messages (
                        thread_id, role, content, executed_sql, data_csv,
                        execution_time_seconds, rows_returned, error
                    )
                    VALUES (
                        :thread_id, :role, :content, :executed_sql, :data_csv,
                        :execution_time_seconds, :rows_returned, :error
                    );
                    """
                ),
                {
                    "thread_id": thread_id,
                    "role": role,
                    "content": content,
                    "executed_sql": executed_sql,
                    "data_csv": data_csv,
                    "execution_time_seconds": execution_time_seconds,
                    "rows_returned": rows_returned,
                    "error": error,
                },
            )

            conn.execute(
                text(
                    """UPDATE query_threads SET updated_at = NOW() WHERE thread_id = :thread_id"""
                ),
                {"thread_id": thread_id},
            )

    try:
        _persist_message()
    except ProgrammingError as exc:
        if not _is_missing_query_table_error(exc):
            raise

        # Tables may not exist if another process cleared the DB; recreate and retry once.
        global _query_history_tables_initialized
        _query_history_tables_initialized = False
        ensure_query_history_tables_exist()
        _persist_message()


def _rows_to_messages(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    for row in rows:
        messages.append(
            {
                "role": row.get("role"),
                "content": row.get("content"),
                "executed_sql": row.get("executed_sql"),
                "data_csv": row.get("data_csv"),
                "execution_time_seconds": row.get("execution_time_seconds"),
                "rows_returned": row.get("rows_returned"),
                "error": row.get("error"),
                "timestamp": row.get("created_at"),
            }
        )
    return messages


def get_query_conversation(thread_id: str, limit: int = 100) -> Dict[str, Any]:
    """Return a conversation for a thread_id ordered by timestamp."""

    ensure_query_history_tables_exist()

    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT role, content, executed_sql, data_csv,
                       execution_time_seconds, rows_returned, error, created_at
                FROM query_messages
                WHERE thread_id = :thread_id
                ORDER BY created_at ASC
                LIMIT :limit
                """
            ),
            {"thread_id": thread_id, "limit": limit},
        ).mappings().all()

        updated_at_row = conn.execute(
            text(
                """
                SELECT updated_at, created_at
                FROM query_threads
                WHERE thread_id = :thread_id
                """
            ),
            {"thread_id": thread_id},
        ).first()

    return {
        "thread_id": thread_id,
        "updated_at": updated_at_row[0] if updated_at_row else None,
        "created_at": updated_at_row[1] if updated_at_row else None,
        "messages": _rows_to_messages(rows),
    }


def get_latest_query_conversation(limit: int = 100) -> Optional[Dict[str, Any]]:
    """Fetch the most recently updated conversation, if any."""

    ensure_query_history_tables_exist()

    engine = get_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT thread_id
                FROM query_threads
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
        ).first()

    if not row:
        return None

    return get_query_conversation(str(row[0]), limit=limit)


def list_query_threads(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Return thread summaries ordered by most recently updated."""

    ensure_query_history_tables_exist()

    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    qt.thread_id,
                    qt.created_at,
                    qt.updated_at,
                    (
                        SELECT content
                        FROM query_messages qm
                        WHERE qm.thread_id = qt.thread_id AND qm.role = 'user'
                        ORDER BY qm.created_at ASC
                        LIMIT 1
                    ) AS first_user_prompt
                FROM query_threads qt
                ORDER BY qt.updated_at DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": limit, "offset": offset},
        ).mappings().all()

    return [dict(row) for row in rows]
