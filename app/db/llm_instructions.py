from typing import List, Dict, Optional
from uuid import uuid4

from sqlalchemy import text

from .session import get_engine
import logging

logger = logging.getLogger(__name__)


def create_llm_instruction_table() -> None:
    """Create the llm_instructions system table if it doesn't exist."""
    engine = get_engine()
    ddl = """
    CREATE TABLE IF NOT EXISTS llm_instructions (
        id UUID PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        last_used_at TIMESTAMP WITH TIME ZONE
    );
    CREATE INDEX IF NOT EXISTS idx_llm_instructions_updated ON llm_instructions(updated_at DESC);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("llm_instructions table ready")


def insert_llm_instruction(title: str, content: str) -> str:
    """Persist an instruction profile and return its UUID."""
    instruction_id = str(uuid4())
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO llm_instructions (id, title, content)
                VALUES (:id, :title, :content)
                """
            ),
            {"id": instruction_id, "title": title, "content": content},
        )
    return instruction_id


def find_llm_instruction_by_content(content: str) -> Optional[Dict[str, str]]:
    """Return the most recently updated instruction that exactly matches the content."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT id, title, content, created_at, updated_at, last_used_at
                FROM llm_instructions
                WHERE content = :content
                ORDER BY COALESCE(last_used_at, updated_at) DESC
                LIMIT 1
                """
            ),
            {"content": content},
        ).mappings().first()
        if not result:
            return None
        record = dict(result)
        record["id"] = str(record["id"])
        return record


def get_llm_instruction(instruction_id: str) -> Optional[Dict[str, str]]:
    """Fetch a single instruction profile."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT id, title, content, created_at, updated_at, last_used_at
                FROM llm_instructions
                WHERE id = :id
                """
            ),
            {"id": instruction_id},
        ).mappings().first()
        if not result:
            return None
        record = dict(result)
        record["id"] = str(record["id"])
        return record


def touch_llm_instruction(instruction_id: str) -> None:
    """Update last_used_at for telemetry/ordering."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE llm_instructions
                SET last_used_at = NOW(), updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": instruction_id},
        )


def list_llm_instructions(limit: int = 50) -> List[Dict[str, str]]:
    """Return recently updated/saved instruction profiles."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT id, title, content, created_at, updated_at, last_used_at
                FROM llm_instructions
                ORDER BY COALESCE(last_used_at, updated_at) DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
        instructions = []
        for row in result.mappings().all():
            record = dict(row)
            record["id"] = str(record["id"])
            instructions.append(record)
        return instructions


def update_llm_instruction(
    instruction_id: str, title: Optional[str] = None, content: Optional[str] = None
) -> Optional[Dict[str, str]]:
    """Update an instruction profile. Returns the updated record or None if missing."""
    if title is None and content is None:
        return None

    updates = []
    params = {"id": instruction_id}
    if title is not None:
        updates.append("title = :title")
        params["title"] = title
    if content is not None:
        updates.append("content = :content")
        params["content"] = content
    updates.append("updated_at = NOW()")

    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"""
                UPDATE llm_instructions
                SET {', '.join(updates)}
                WHERE id = :id
                RETURNING id, title, content, created_at, updated_at, last_used_at
                """
            ),
            params,
        ).mappings().first()
        if not result:
            return None
        record = dict(result)
        record["id"] = str(record["id"])
        return record


def delete_llm_instruction(instruction_id: str) -> bool:
    """Delete an instruction profile. Returns True if a row was deleted."""
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM llm_instructions WHERE id = :id"),
            {"id": instruction_id},
        )
    return result.rowcount > 0
