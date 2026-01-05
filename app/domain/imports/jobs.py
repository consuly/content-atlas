"""
Persistent tracking for long-running import jobs.
"""
from __future__ import annotations

import json
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from app.db.session import get_engine
from app.domain.uploads.uploaded_files import (
    assign_active_job_state,
    update_active_job_state,
    clear_active_job_state,
    update_file_status,
)

_table_initialized = False
_table_init_lock = threading.Lock()


def ensure_import_jobs_table() -> None:
    """Create the import_jobs table on-demand."""
    global _table_initialized
    if _table_initialized:
        return

    with _table_init_lock:
        if _table_initialized:
            return
        _create_import_jobs_table()
        _table_initialized = True


def _reset_table_flag() -> None:
    global _table_initialized
    with _table_init_lock:
        _table_initialized = False


def _is_missing_table_error(error: ProgrammingError) -> bool:
    origin = getattr(error, "orig", None)
    return getattr(origin, "pgcode", None) == "42P01"


def _run_with_table_retry(operation: Callable[[], Any]) -> Any:
    try:
        return operation()
    except ProgrammingError as error:
        if not _is_missing_table_error(error):
            raise
        _reset_table_flag()
        ensure_import_jobs_table()
        return operation()


def _create_import_jobs_table() -> None:
    engine = get_engine()
    create_sql = """
    CREATE TABLE IF NOT EXISTS import_jobs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        file_id UUID NOT NULL REFERENCES uploaded_files(id) ON DELETE CASCADE,
        status VARCHAR(50) NOT NULL DEFAULT 'running',
        stage VARCHAR(50) NOT NULL DEFAULT 'queued',
        progress INTEGER DEFAULT 0,
        retry_attempt INTEGER DEFAULT 1,
        error_message TEXT,
        trigger_source VARCHAR(50),
        analysis_mode VARCHAR(50),
        conflict_mode VARCHAR(50),
        metadata JSONB DEFAULT '{}'::jsonb,
        result_metadata JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        completed_at TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_import_jobs_file ON import_jobs(file_id);
    CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs(status);
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))


def _json_payload(value: Optional[Dict[str, Any]]) -> str:
    return json.dumps(value or {})


def _row_to_job(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "file_id": str(row["file_id"]),
        "status": row["status"],
        "stage": row["stage"],
        "progress": row["progress"],
        "retry_attempt": row["retry_attempt"],
        "error_message": row["error_message"],
        "trigger_source": row["trigger_source"],
        "analysis_mode": row["analysis_mode"],
        "conflict_mode": row["conflict_mode"],
        "metadata": row["metadata"],
        "result_metadata": row["result_metadata"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "completed_at": row["completed_at"],
    }


def create_import_job(
    *,
    file_id: str,
    trigger_source: str,
    analysis_mode: Optional[str] = None,
    conflict_mode: Optional[str] = None,
    stage: str = "analysis",
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create and persist a new import job, attaching it to the file."""
    ensure_import_jobs_table()
    engine = get_engine()
    job_id = str(uuid.uuid4())

    insert_sql = """
    INSERT INTO import_jobs (
        id, file_id, status, stage, progress, retry_attempt, error_message,
        trigger_source, analysis_mode, conflict_mode, metadata
    )
    VALUES (
        :id, :file_id, 'running', :stage, 0, 1, NULL,
        :trigger_source, :analysis_mode, :conflict_mode, CAST(:metadata AS jsonb)
    )
    RETURNING *
    """
    params = {
        "id": job_id,
        "file_id": file_id,
        "stage": stage,
        "trigger_source": trigger_source,
        "analysis_mode": analysis_mode,
        "conflict_mode": conflict_mode,
        "metadata": _json_payload(metadata),
    }

    def _insert() -> Dict[str, Any]:
        with engine.connect() as conn:
            result = conn.execute(text(insert_sql), params)
            conn.commit()
            row = result.mappings().first()
            if not row:
                raise RuntimeError("Failed to create import job")
            return _row_to_job(row)

    job = _run_with_table_retry(_insert)
    assign_active_job_state(file_id, job_id, job["status"], job["stage"])
    return job


def update_import_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    progress: Optional[int] = None,
    error_message: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    result_metadata: Optional[Dict[str, Any]] = None,
    completed: bool = False
) -> Optional[Dict[str, Any]]:
    """Update an existing import job and synchronize file state."""
    ensure_import_jobs_table()
    engine = get_engine()

    update_parts = ["updated_at = NOW()"]
    params: Dict[str, Any] = {"job_id": job_id}

    if status is not None:
        update_parts.append("status = :status")
        params["status"] = status
    if stage is not None:
        update_parts.append("stage = :stage")
        params["stage"] = stage
    if progress is not None:
        update_parts.append("progress = :progress")
        params["progress"] = progress
    if error_message is not None:
        update_parts.append("error_message = :error_message")
        params["error_message"] = error_message
    if metadata is not None:
        update_parts.append("metadata = CAST(:metadata AS jsonb)")
        params["metadata"] = _json_payload(metadata)
    if result_metadata is not None:
        update_parts.append("result_metadata = CAST(:result_metadata AS jsonb)")
        params["result_metadata"] = _json_payload(result_metadata)
    if completed:
        update_parts.append("completed_at = COALESCE(completed_at, NOW())")

    if len(update_parts) == 1:
        return get_import_job(job_id)

    update_sql = f"""
    UPDATE import_jobs
    SET {", ".join(update_parts)}
    WHERE id = :job_id
    RETURNING *
    """

    def _update() -> Optional[Dict[str, Any]]:
        with engine.connect() as conn:
            result = conn.execute(text(update_sql), params)
            conn.commit()
            row = result.mappings().first()
            return _row_to_job(row) if row else None

    job = _run_with_table_retry(_update)
    if job:
        update_active_job_state(
            job["file_id"],
            job["id"],
            job_status=status,
            job_stage=stage,
            progress=progress,
            error_message=error_message,
        )
    return job


def complete_import_job(
    job_id: str,
    *,
    success: bool,
    error_message: Optional[str] = None,
    result_metadata: Optional[Dict[str, Any]] = None,
    mapped_table_name: Optional[str] = None,
    mapped_rows: Optional[int] = None,
    data_validation_errors: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """Mark a job as completed and clear the active job state on the file."""
    status = "succeeded" if success else "failed"
    job = update_import_job(
        job_id,
        status=status,
        stage="completed",
        error_message=error_message,
        result_metadata=result_metadata,
        completed=True,
    )
    if not job:
        return None

    file_id = job["file_id"]
    if success:
        update_file_status(
            file_id,
            "mapped",
            mapped_table_name=mapped_table_name
            or (result_metadata.get("table_name") if result_metadata else None),
            mapped_rows=mapped_rows
            or (result_metadata.get("records_processed") if result_metadata else None),
            expected_active_job_id=job_id,
            data_validation_errors=data_validation_errors
        )
    else:
        update_file_status(
            file_id,
            "failed",
            error_message=error_message,
            expected_active_job_id=job_id,
        )

    clear_active_job_state(file_id, job_id)
    return job


def get_import_job(job_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single job by ID."""
    ensure_import_jobs_table()
    engine = get_engine()

    query_sql = """
    SELECT *
    FROM import_jobs
    WHERE id = :job_id
    """

    def _fetch() -> Optional[Dict[str, Any]]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), {"job_id": job_id})
            row = result.mappings().first()
            return _row_to_job(row) if row else None

    return _run_with_table_retry(_fetch)


def list_import_jobs(
    *,
    file_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    """List jobs, optionally filtered by file."""
    ensure_import_jobs_table()
    engine = get_engine()

    where_clause = "WHERE file_id = :file_id" if file_id else ""
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if file_id:
        params["file_id"] = file_id

    query_sql = f"""
    SELECT *
    FROM import_jobs
    {where_clause}
    ORDER BY created_at DESC
    LIMIT :limit OFFSET :offset
    """

    count_sql = f"""
    SELECT COUNT(*)
    FROM import_jobs
    {where_clause}
    """

    def _fetch() -> Tuple[List[Dict[str, Any]], int]:
        with engine.connect() as conn:
            result = conn.execute(text(query_sql), params)
            rows = result.mappings().all()
            jobs = [_row_to_job(row) for row in rows]

            count_result = conn.execute(text(count_sql), params if file_id else {})
            total = count_result.scalar() or 0
            return jobs, total

    return _run_with_table_retry(_fetch)


def fail_active_job(
    file_id: str,
    job_id: str,
    error_message: str
) -> None:
    """Helper to mark the current job as failed without clearing the error state."""
    update_import_job(
        job_id,
        status="failed",
        stage="completed",
        error_message=error_message,
        completed=True,
    )
    update_file_status(
        file_id,
        "failed",
        error_message=error_message,
        expected_active_job_id=job_id,
    )
    clear_active_job_state(file_id, job_id)
