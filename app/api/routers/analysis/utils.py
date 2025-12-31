"""
Utility functions for file analysis operations.

This module contains helper functions for:
- Table name normalization
- LLM instruction resolution
- Forced table decision application
- Logging (archive debug and mapping failures)
- File naming and fingerprinting
"""
import json
import logging
import mimetypes
import os
import threading
import inspect
from datetime import datetime, timezone
from typing import Optional, Any, List, Dict

import pandas as pd
import io
from fastapi import HTTPException

from app.api.schemas.shared import ensure_safe_table_name
from app.api.dependencies import detect_file_type
from app.domain.imports.processors.csv_processor import (
    extract_raw_csv_rows,
    detect_csv_header,
)
from app.db.llm_instructions import (
    get_llm_instruction,
    find_llm_instruction_by_content,
    insert_llm_instruction,
    update_llm_instruction,
    touch_llm_instruction,
    create_llm_instruction_table,
)
from app.core.config import settings

logger = logging.getLogger(__name__)

# Module-level constants
ARCHIVE_DEBUG_LOG = os.path.join("logs", "archive_debug.jsonl")
MAPPING_FAILURE_LOG = os.path.join("logs", "mapping_failures.jsonl")
_archive_log_lock = threading.Lock()
_failure_log_lock = threading.Lock()


def normalize_forced_table_name(table_name: Optional[str]) -> Optional[str]:
    """Return a sanitized table name or raise if the provided value is blank."""
    if table_name is None:
        return None
    normalized = ensure_safe_table_name(table_name)
    if not normalized:
        raise HTTPException(status_code=400, detail="target_table_name cannot be blank")
    return normalized


def resolve_llm_instruction(
    *,
    llm_instruction: Optional[str],
    llm_instruction_id: Optional[str],
    save_llm_instruction: bool = False,
    llm_instruction_title: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Resolve the instruction text from either the provided string or a saved profile.
    Optionally save the instruction for reuse and return the saved profile id.
    """
    from fastapi.params import Form as FormParam
    
    llm_instruction = llm_instruction if not isinstance(llm_instruction, FormParam) else llm_instruction.default
    llm_instruction_id = (
        llm_instruction_id if not isinstance(llm_instruction_id, FormParam) else llm_instruction_id.default
    )
    llm_instruction_title = (
        llm_instruction_title
        if not isinstance(llm_instruction_title, FormParam)
        else llm_instruction_title.default
    )
    create_llm_instruction_table()
    normalized_instruction = (llm_instruction or "").strip() or None
    resolved_id: Optional[str] = None

    if not normalized_instruction and llm_instruction_id:
        record = get_llm_instruction(llm_instruction_id)
        if not record:
            raise HTTPException(status_code=404, detail="Instruction profile not found")
        normalized_instruction = record.get("content") or None
        resolved_id = record.get("id")
        if resolved_id:
            touch_llm_instruction(resolved_id)

    if save_llm_instruction and normalized_instruction:
        desired_title = (llm_instruction_title or "").strip()
        existing_instruction = find_llm_instruction_by_content(normalized_instruction)

        if existing_instruction:
            resolved_id = existing_instruction.get("id")
            existing_title = existing_instruction.get("title") or ""
            if desired_title and desired_title != existing_title and resolved_id:
                updated = update_llm_instruction(resolved_id, title=desired_title)
                resolved_id = updated["id"] if updated else resolved_id
            if resolved_id:
                touch_llm_instruction(resolved_id)
        else:
            resolved_id = insert_llm_instruction(
                desired_title or "Saved import instruction",
                normalized_instruction,
            )

    return normalized_instruction, resolved_id


def invoke_analyzer(analyze_fn, **kwargs):
    """
    Call the analyzer, dropping kwargs that are not supported by patched test doubles.
    """
    sig = inspect.signature(analyze_fn)
    filtered = {key: value for key, value in kwargs.items() if key in sig.parameters}
    return analyze_fn(**filtered)


def apply_forced_table_decision(
    llm_decision: Dict[str, Any],
    forced_table_name: Optional[str],
    forced_table_mode: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Override the LLM decision with a user-requested target table."""
    if not forced_table_name:
        return llm_decision
    if not llm_decision:
        return None

    updated = dict(llm_decision)
    updated["target_table"] = forced_table_name
    updated["forced_target_table"] = forced_table_name
    if forced_table_mode:
        updated["forced_table_mode"] = forced_table_mode
        if forced_table_mode == "existing" and updated.get("strategy") == "NEW_TABLE":
            updated["strategy"] = "ADAPT_DATA"
        if forced_table_mode == "new" and updated.get("strategy") != "NEW_TABLE":
            updated["strategy"] = "NEW_TABLE"
    return updated


def log_archive_debug(payload: Dict[str, Any]) -> None:
    """
    Append a structured JSON line to the archive debug log.
    Failures should never break the worker.
    """
    try:
        os.makedirs(os.path.dirname(ARCHIVE_DEBUG_LOG), exist_ok=True)
        # Use local timezone if configured, otherwise UTC
        if settings.log_timezone == "local":
            ts = datetime.now().astimezone().isoformat()
        else:
            ts = datetime.now(timezone.utc).isoformat()
        record = {
            "ts": ts,
            **payload,
        }
        with _archive_log_lock:
            with open(ARCHIVE_DEBUG_LOG, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Archive debug log write failed: %s", exc)


def log_mapping_failure(payload: Dict[str, Any]) -> None:
    """
    Append a structured JSON line describing a mapping failure for debugging.
    Errors here should never break the main flow.
    """
    try:
        os.makedirs(os.path.dirname(MAPPING_FAILURE_LOG), exist_ok=True)
        # Use local timezone if configured, otherwise UTC
        if settings.log_timezone == "local":
            ts = datetime.now().astimezone().isoformat()
        else:
            ts = datetime.now(timezone.utc).isoformat()
        record = {
            "ts": ts,
            **payload,
        }
        with _failure_log_lock:
            with open(MAPPING_FAILURE_LOG, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Mapping failure log write failed: %s", exc)


def guess_content_type(file_name: str) -> str:
    """Guess MIME type from filename."""
    content_type, _ = mimetypes.guess_type(file_name)
    return content_type or "application/octet-stream"


def build_archive_entry_name(archive_stem: str, entry_name: str, index: int) -> str:
    """Build a unique name for an archive entry."""
    sanitized = entry_name.replace("\\", "_").replace("/", "_")
    sanitized = sanitized or f"archive_entry_{index:03d}"
    return f"{archive_stem}__{index:03d}__{sanitized}"


def normalize_columns(columns: List[Any]) -> List[str]:
    """Normalize column labels when building structure fingerprints."""
    normalized: List[str] = []
    for index, column in enumerate(columns, start=1):
        if column is None:
            normalized.append(f"col_{index}")
            continue
        token = str(column).strip().lower()
        normalized.append(token or f"col_{index}")
    return normalized


def build_structure_fingerprint(entry_bytes: bytes, entry_name: str) -> Optional[str]:
    """
    Build a lightweight structure fingerprint so similar files can reuse the same mapping decision.

    The fingerprint focuses on column shape (count + normalized labels) to avoid hashing entire content.
    """
    try:
        file_type = detect_file_type(entry_name)
    except Exception:
        return None

    try:
        if file_type == "csv":
            raw_rows = extract_raw_csv_rows(entry_bytes, num_rows=5) or []
            if not raw_rows:
                return None
            try:
                has_header = detect_csv_header(entry_bytes)
            except Exception:
                has_header = True
            header_row = raw_rows[0] if raw_rows else []
            columns = header_row if has_header else [f"col_{idx+1}" for idx in range(len(header_row))]
            normalized = normalize_columns(columns)
            return f"csv:{len(normalized)}:{'|'.join(normalized)}"
        if file_type == "excel":
            df = pd.read_excel(io.BytesIO(entry_bytes), engine="openpyxl", nrows=5)
            normalized = normalize_columns(list(df.columns))
            return f"excel:{len(normalized)}:{'|'.join(normalized)}"
        if file_type == "json":
            return "json"
        if file_type == "xml":
            return "xml"
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Could not build fingerprint for %s: %s", entry_name, exc)
        return None
    return None


def parse_sheet_names_param(raw_sheet_names: Optional[str]) -> Optional[List[str]]:
    """Parse sheet name form input supporting JSON arrays or comma-delimited strings."""
    if not raw_sheet_names:
        return None
    try:
        parsed = json.loads(raw_sheet_names)
        if isinstance(parsed, list):
            names = [str(name).strip() for name in parsed if str(name).strip()]
            return names or None
    except json.JSONDecodeError:
        pass

    names = [part.strip() for part in raw_sheet_names.split(",") if part.strip()]
    return names or None
