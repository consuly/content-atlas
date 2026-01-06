"""
AI-powered file analysis endpoints for intelligent import recommendations.
"""
import asyncio
import io
import json
import logging
import mimetypes
import os
import uuid
import zipfile
import threading
import inspect
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from fastapi.params import Form as FormParam
from sqlalchemy.orm import Session
from typing import Optional, Any, List, Dict, Tuple
from dataclasses import dataclass, field

import pandas as pd
from app.db.session import get_db, get_session_local
from app.api.schemas.shared import (
    AnalyzeFileResponse, AnalyzeB2FileRequest, ExecuteRecommendedImportRequest,
    AnalysisMode, ConflictResolutionMode, MapDataResponse,
    AnalyzeFileInteractiveRequest, AnalyzeFileInteractiveResponse, 
    ExecuteInteractiveImportRequest, AutoExecutionResult,
    ArchiveAutoProcessResponse, ArchiveAutoProcessFileResult, WorkbookSheetsResponse,
    ensure_safe_table_name
)
from app.api.dependencies import detect_file_type, analysis_storage, interactive_sessions
from app.integrations.storage import (
    download_file as _download_file_from_storage,
    upload_file as upload_file_to_storage,
    StorageConnectionError,
)
from app.domain.imports.processors.csv_processor import (
    process_csv,
    process_excel,
    extract_excel_sheet_csv_bytes,
    extract_raw_csv_rows,
    detect_csv_header,
    list_excel_sheets,
    load_csv_sample,
)
from app.domain.imports.processors.json_processor import process_json
from app.domain.imports.processors.xml_processor import process_xml
from app.domain.queries.analyzer import analyze_file_for_import as _analyze_file_for_import, sample_file_data
import app.integrations.auto_import as auto_import
from app.domain.uploads.uploaded_files import (
    get_uploaded_file_by_id,
    update_file_status,
    insert_uploaded_file,
)
from app.domain.imports.jobs import create_import_job, update_import_job, complete_import_job, get_import_job
from app.domain.imports.history import get_import_history, list_duplicate_rows
from app.core.config import settings
from app.domain.imports.jobs import fail_active_job
from app.db.llm_instructions import (
    get_llm_instruction,
    find_llm_instruction_by_content,
    insert_llm_instruction,
    update_llm_instruction,
    touch_llm_instruction,
    create_llm_instruction_table,
)

router = APIRouter(tags=["analysis"])
logger = logging.getLogger(__name__)
_preloaded_file_contents: Dict[str, bytes] = {}
ARCHIVE_SUPPORTED_SUFFIXES = (".csv", ".xlsx", ".xls")

# Debug logging configuration
ARCHIVE_DEBUG_LOG = os.path.join("logs", "archive_debug.jsonl")
MAPPING_FAILURE_LOG = os.path.join("logs", "mapping_failures.jsonl")
_archive_log_lock = threading.Lock()
_failure_log_lock = threading.Lock()


def _normalize_forced_table_name(table_name: Optional[str]) -> Optional[str]:
    """Return a sanitized table name or raise if the provided value is blank."""
    if table_name is None:
        return None
    normalized = ensure_safe_table_name(table_name)
    if not normalized:
        raise HTTPException(status_code=400, detail="target_table_name cannot be blank")
    return normalized


def _resolve_llm_instruction(
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


def _invoke_analyzer(analyze_fn, **kwargs):
    """
    Call the analyzer, dropping kwargs that are not supported by patched test doubles.
    """
    sig = inspect.signature(analyze_fn)
    filtered = {key: value for key, value in kwargs.items() if key in sig.parameters}
    return analyze_fn(**filtered)


def _apply_forced_table_decision(
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


def _log_archive_debug(payload: Dict[str, Any]) -> None:
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


def _log_mapping_failure(payload: Dict[str, Any]) -> None:
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


def _guess_content_type(file_name: str) -> str:
    content_type, _ = mimetypes.guess_type(file_name)
    return content_type or "application/octet-stream"


def _build_archive_entry_name(archive_stem: str, entry_name: str, index: int) -> str:
    sanitized = entry_name.replace("\\", "_").replace("/", "_").replace(" ", "_")
    sanitized = sanitized or f"archive_entry_{index:03d}"
    return f"{archive_stem}__{index:03d}__{sanitized}"


def _normalize_columns(columns: List[Any]) -> List[str]:
    """Normalize column labels when building structure fingerprints."""
    normalized: List[str] = []
    for index, column in enumerate(columns, start=1):
        if column is None:
            normalized.append(f"col_{index}")
            continue
        # Use simple normalization compatible with fingerprinting module
        # Keep alphanumeric characters and lowercase
        token = str(column).strip().lower()
        import re
        token = re.sub(r'[^a-z0-9]', '', token)
        normalized.append(token or f"col_{index}")
    return normalized


def _table_exists(table_name: str) -> bool:
    """
    Check if a table exists in the database.
    
    Used to validate cached decisions - if a table was created by a previous file in the archive,
    we need to check if new files should merge into it or create separate tables.
    """
    try:
        from sqlalchemy import inspect
        from app.db.session import get_engine
        engine = get_engine()
        inspector = inspect(engine)
        return inspector.has_table(table_name)
    except Exception as exc:
        logger.warning("Failed to check table existence for '%s': %s", table_name, exc)
        return False


def _build_structure_fingerprint(entry_bytes: bytes, entry_name: str) -> Optional[str]:
    """
    Build a lightweight structure fingerprint so similar files can reuse the same mapping decision.

    The fingerprint focuses on column shape (count + normalized labels) to avoid hashing entire content.
    
    NOTE: Structure fingerprinting is an optimization that caches LLM decisions for files with
    identical column structures. However, this can cause issues when files with the same structure
    should be consolidated (same semantic meaning) or separated (different semantic meaning).
    The cache validation in _process_entry_bytes() handles these cases.
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
            normalized = _normalize_columns(columns)
            normalized.sort()  # SORT to ensure column order independence
            return f"csv:{len(normalized)}:{'|'.join(normalized)}"
        if file_type == "excel":
            df = pd.read_excel(io.BytesIO(entry_bytes), engine="openpyxl", nrows=5)
            normalized = _normalize_columns(list(df.columns))
            normalized.sort()  # SORT to ensure column order independence
            return f"excel:{len(normalized)}:{'|'.join(normalized)}"
        if file_type == "json":
            return "json"
        if file_type == "xml":
            return "xml"
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Could not build fingerprint for %s: %s", entry_name, exc)
        return None
    return None


def _parse_records_for_execution(file_content: bytes, file_type: str) -> List[Dict[str, Any]]:
    """Parse file bytes into records for cached execution paths."""
    if file_type == "csv":
        return process_csv(file_content, has_header=None)
    if file_type == "excel":
        return process_excel(file_content)
    if file_type == "json":
        return process_json(file_content)
    if file_type == "xml":
        return process_xml(file_content)
    raise ValueError(f"Unsupported file type: {file_type}")


def _execute_cached_archive_decision(
    entry_bytes: bytes, entry_name: str, llm_decision: Dict[str, Any], source_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Execute a cached LLM decision for an archive entry without re-running analysis.
    Returns a summary shape compatible with _summarize_archive_execution.
    """
    try:
        file_type = detect_file_type(entry_name)
        records = _parse_records_for_execution(entry_bytes, file_type)
        execution_result = _get_execute_llm_import_decision()(
            file_content=entry_bytes,
            file_name=entry_name,
            all_records=records,
            llm_decision=llm_decision,
            source_path=source_path,
        )
        response = AnalyzeFileResponse(
            success=execution_result.get("success", False),
            llm_response="Reused cached decision",
            llm_decision=llm_decision,
            iterations_used=0,
            max_iterations=1,
            can_auto_execute=True,
        )
        if execution_result.get("success"):
            response.auto_execution_result = AutoExecutionResult(**execution_result)
        else:
            response.auto_execution_error = execution_result.get("error")
            response.error = execution_result.get("error")
        
        summary = _summarize_archive_execution(response)
        # Include the actual table name used for registry tracking
        if execution_result.get("success"):
            summary["actual_table_name"] = execution_result.get("table_name")
        return summary
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Cached execution failed for %s: %s", entry_name, exc)
        return {
            "status": "failed",
            "message": f"Cached execution failed: {exc}",
            "auto_retry_used": False,
        }


def _summarize_archive_execution(response: AnalyzeFileResponse) -> Dict[str, Any]:
    auto_result = response.auto_execution_result
    retry_result = response.auto_retry_execution_result

    if retry_result and retry_result.success:
        return {
            "status": "processed",
            "table_name": retry_result.table_name,
            "records_processed": retry_result.records_processed,
            "duplicates_skipped": retry_result.duplicates_skipped,
            "validation_errors": retry_result.validation_errors or 0,
            "mapping_errors": len(retry_result.mapping_errors or []),
            "import_id": retry_result.import_id,
            "auto_retry_used": True,
            "message": "Processed via Try Again",
        }

    if auto_result and auto_result.success:
        return {
            "status": "processed",
            "table_name": auto_result.table_name,
            "records_processed": auto_result.records_processed,
            "duplicates_skipped": auto_result.duplicates_skipped,
            "validation_errors": auto_result.validation_errors or 0,
            "mapping_errors": len(auto_result.mapping_errors or []) if auto_result.mapping_errors else 0,
            "import_id": auto_result.import_id,
            "auto_retry_used": False,
            "message": "Processed automatically",
        }

    base_message = (
        response.auto_execution_error
        or (auto_result.error if auto_result else None)
        or response.error
        or "Automatic processing failed"
    )
    retry_details = None
    if response.auto_retry_attempted and not (
        retry_result and retry_result.success
    ):
        retry_details = response.auto_retry_error or "Auto retry could not complete"

    interactive_hint = None
    if retry_details and "interactive assistant requires user input" in retry_details.lower():
        interactive_hint = (
            "Auto-retry paused: the assistant needs your input. "
            "Use the import page to resume failed archive entries or open the Interactive tab to continue."
        )

    message_parts = [base_message]
    if retry_details and retry_details not in (base_message or ""):
        message_parts.append(f"Try Again: {retry_details}")
    if interactive_hint:
        message_parts.append(interactive_hint)
    message = " | ".join(part for part in message_parts if part)

    return {
        "status": "failed",
        "table_name": auto_result.table_name if auto_result else None,
        "records_processed": auto_result.records_processed if auto_result else None,
        "duplicates_skipped": auto_result.duplicates_skipped if auto_result else None,
        "import_id": auto_result.import_id if auto_result else None,
        "auto_retry_used": response.auto_retry_attempted,
        "message": message,
        "llm_response": response.llm_response,
    }


def _process_entry_bytes(
    *,
    entry_bytes: bytes,
    archive_path: str,
    entry_name: str,
    stored_file_name: str,
    archive_folder: str,
    fingerprint_cache: Dict[str, Dict[str, Any]],
    fingerprint_lock: threading.Lock,
    analysis_mode: AnalysisMode,
    conflict_resolution: ConflictResolutionMode,
    auto_execute_confidence_threshold: float,
    max_iterations: int,
    forced_table_name: Optional[str],
    forced_table_mode: Optional[str],
    llm_instruction: Optional[str],
    db_session,
    sheet_name: Optional[str] = None,
    parent_file_id: Optional[str] = None,
) -> ArchiveAutoProcessFileResult:
    """Shared worker logic for archive entries and workbook sheets."""
    analysis_response: Optional[AnalyzeFileResponse] = None
    waited_for_cached_plan_without_decision = False

    # 2. B2 Upload (parallel)
    try:
        upload_result = upload_file_to_storage(
            file_content=entry_bytes,
            file_name=stored_file_name,
            folder=archive_folder,
        )
        uploaded_file = insert_uploaded_file(
            file_name=entry_name,
            b2_file_id=upload_result["file_id"],
            b2_file_path=upload_result["file_path"],
            file_size=len(entry_bytes),
            content_type=_guess_content_type(entry_name),
            user_id=None,
            parent_file_id=parent_file_id,
        )
        uploaded_file_id = uploaded_file["id"]
    except Exception as exc:
        logger.exception("Failed staging entry %s: %s", archive_path, exc)
        return ArchiveAutoProcessFileResult(
            archive_path=archive_path,
            stored_file_name=stored_file_name,
            sheet_name=sheet_name,
            status="failed",
            message=f"Unable to stage file: {exc}",
        )

    # 3. Fingerprinting (parallel calculation, serialized cache access)
    fingerprint: Optional[str] = None
    cached_plan: Optional[Dict[str, Any]] = None
    cached_plan_event: Optional[threading.Event] = None
    is_first_worker_for_plan = False
    try:
        fingerprint = _build_structure_fingerprint(entry_bytes, entry_name)
        if fingerprint:
            with fingerprint_lock:
                cached_plan = fingerprint_cache.get(fingerprint)
                if cached_plan and cached_plan.get("llm_decision"):
                    cached_plan_event = cached_plan.get("event")
                elif cached_plan and cached_plan.get("event"):
                    cached_plan_event = cached_plan["event"]
                else:
                    cached_plan_event = threading.Event()
                    fingerprint_cache[fingerprint] = {"event": cached_plan_event}
                    is_first_worker_for_plan = True
    except Exception as exc:
        logger.debug("Fingerprinting failed for %s: %s", entry_name, exc)

    # 4. Analysis & Execution (parallel analysis, serialized insertion via TableLockManager)
    summary = {}
    analysis_response: Optional[AnalyzeFileResponse] = None  # Track response for error logging
    
    def _run_fresh_analysis(
        override_instruction: Optional[str] = None,
        override_target_table: Optional[str] = None,
        override_target_mode: Optional[str] = None
    ) -> tuple[Optional[AnalyzeFileResponse], Dict[str, Any]]:
        """Run analysis and return both the response and summarized outcome."""
        _preloaded_file_contents[uploaded_file_id] = entry_bytes
        instruction_to_use = override_instruction if override_instruction is not None else llm_instruction
        try:
            local_response = asyncio.run(
                analyze_file_endpoint(
                    file=None,
                    file_id=uploaded_file_id,
                    sample_size=None,
                    analysis_mode=analysis_mode,
                    conflict_resolution=conflict_resolution,
                    auto_execute_confidence_threshold=auto_execute_confidence_threshold,
                    max_iterations=max_iterations,
                    db=db_session,
                    target_table_name=override_target_table if override_target_table else forced_table_name,
                    target_table_mode=override_target_mode if override_target_mode else forced_table_mode,
                    llm_instruction=instruction_to_use,
                    skip_file_duplicate_check=False,
                    require_explicit_multi_value=False,
                )
            )
            return local_response, _summarize_archive_execution(local_response)
        finally:
            _preloaded_file_contents.pop(uploaded_file_id, None)

    try:
        if cached_plan and cached_plan.get("llm_decision"):
            # CACHE AS TABLE HINT: Don't reuse full mapping - use as target table hint only
            # This ensures each file gets fresh LLM analysis with proper column mapping
            # while still consolidating into the same table
            cached_decision = cached_plan["llm_decision"]
            target_table_hint = cached_decision.get("target_table")
            
            # Check if registry has a created table for this fingerprint
            created_table_hint = cached_plan.get("created_table")
            table_hint = created_table_hint or target_table_hint
            
            if table_hint and not forced_table_name:
                logger.info(
                    "FINGERPRINT MATCH: File '%s' matches existing structure. "
                    "Running FRESH LLM analysis with target table hint '%s' to ensure correct column mapping.",
                    entry_name,
                    table_hint
                )
                
                _log_archive_debug({
                    "event": "fingerprint_hint_used",
                    "archive_path": archive_path,
                    "entry_name": entry_name,
                    "fingerprint": fingerprint,
                    "table_hint": table_hint,
                    "reason": "use_hint_for_fresh_analysis"
                })
                
                # Run fresh analysis with table hint
                analysis_response, summary = _run_fresh_analysis(
                    override_target_table=table_hint,
                    override_target_mode="existing"
                )
                
                # EARLY REGISTRY RECORDING: Record table decision immediately after LLM analysis
                # This ensures subsequent workers can see this decision before execution completes
                if analysis_response and analysis_response.llm_decision and fingerprint:
                    decided_table = analysis_response.llm_decision.get("target_table")
                    if decided_table:
                        with fingerprint_lock:
                            if fingerprint in fingerprint_cache:
                                fingerprint_cache[fingerprint]["created_table"] = decided_table
                                logger.info(
                                    "FINGERPRINT REGISTRY: Early-recorded table decision '%s' for fingerprint (hint path)",
                                    decided_table
                                )
                
                # Record the table that was actually created/used after execution
                if summary.get("status") == "processed" and fingerprint:
                    actual_table = summary.get("actual_table_name") or summary.get("table_name")
                    if actual_table:
                        with fingerprint_lock:
                            if fingerprint in fingerprint_cache:
                                fingerprint_cache[fingerprint]["created_table"] = actual_table
                                logger.info(
                                    "FINGERPRINT REGISTRY: Confirmed table '%s' for fingerprint after execution (hint path)",
                                    actual_table
                                )
            else:
                # No table hint available or user forced a different table - run fresh analysis
                logger.info(
                    "FINGERPRINT MATCH but no table hint: Running fresh analysis for '%s'",
                    entry_name
                )
                analysis_response, summary = _run_fresh_analysis()
                
                # Store decision in cache for future files
                if fingerprint and analysis_response and analysis_response.llm_decision:
                    with fingerprint_lock:
                        existing_cache = fingerprint_cache.get(fingerprint) or {}
                        event = existing_cache.get("event") or cached_plan_event
                        fingerprint_cache[fingerprint] = {
                            "llm_decision": analysis_response.llm_decision,
                            "event": event,
                        }
                        if event:
                            event.set()
                
                # EARLY REGISTRY RECORDING: Record table decision immediately after LLM analysis
                if analysis_response and analysis_response.llm_decision and fingerprint:
                    decided_table = analysis_response.llm_decision.get("target_table")
                    if decided_table:
                        with fingerprint_lock:
                            if fingerprint in fingerprint_cache:
                                fingerprint_cache[fingerprint]["created_table"] = decided_table
                                logger.info(
                                    "FINGERPRINT REGISTRY: Early-recorded table decision '%s' for fingerprint (no hint path)",
                                    decided_table
                                )
                
                # Record the table that was actually created after execution
                if summary.get("status") == "processed" and fingerprint:
                    actual_table = summary.get("actual_table_name") or summary.get("table_name")
                    if actual_table:
                        with fingerprint_lock:
                            if fingerprint in fingerprint_cache:
                                fingerprint_cache[fingerprint]["created_table"] = actual_table
                                logger.info(
                                    "FINGERPRINT REGISTRY: Confirmed table '%s' for fingerprint after execution (no hint path)",
                                    actual_table
                                )
        elif cached_plan_event and fingerprint and not is_first_worker_for_plan:
            # Another worker is generating a plan; wait briefly to reuse it.
            # Calculate timeout based on file complexity (column count from fingerprint)
            # Wait timeout should be LONGER than LLM analysis timeout to avoid race conditions
            wait_timeout = min(settings.llm_analysis_timeout + 30, 240)  # LLM timeout + 30s buffer, cap at 240s
            if fingerprint:
                # Fingerprint format: "csv:{col_count}:{normalized_columns}"
                parts = fingerprint.split(":")
                if len(parts) >= 2 and parts[1].isdigit():
                    col_count = int(parts[1])
                    # For very complex files, add even more time
                    if col_count > 50:
                        wait_timeout = min(wait_timeout + 60, 300)
                    logger.info(
                        "FINGERPRINT WAIT: Waiting up to %d seconds for cached decision (file has %d columns)",
                        wait_timeout,
                        col_count
                    )
            
            cached_plan_event.wait(timeout=wait_timeout)
            with fingerprint_lock:
                cached_after_wait = fingerprint_cache.get(fingerprint)
            if cached_after_wait and cached_after_wait.get("llm_decision"):
                decision_to_use = dict(cached_after_wait["llm_decision"])
                if decision_to_use.get("strategy") == "NEW_TABLE":
                    decision_to_use["strategy"] = "ADAPT_DATA"
                    logger.info("AUTO-IMPORT: Switching cached strategy from NEW_TABLE to ADAPT_DATA for archive reuse (waited)")

                applied_decision = _apply_forced_table_decision(
                    decision_to_use,
                    forced_table_name,
                    forced_table_mode,
                )
                summary = _execute_cached_archive_decision(
                    entry_bytes=entry_bytes,
                    entry_name=entry_name,
                    llm_decision=applied_decision,
                    source_path=upload_result["file_path"],
                )
            else:
                # No plan materialized—fall back to fresh analysis so we still produce a result.
                # But FIRST check if another worker registered a table while we were waiting
                waited_for_cached_plan_without_decision = True
                
                # REGISTRY CHECK: Before running fresh analysis, check if another worker registered a table
                created_table_from_registry = None
                if fingerprint and not forced_table_name:
                    with fingerprint_lock:
                        existing_cache = fingerprint_cache.get(fingerprint) or {}
                        created_table_from_registry = existing_cache.get("created_table")
                
                if created_table_from_registry:
                    # Another file with same fingerprint created a table while we waited - force merge
                    logger.info(
                        "FINGERPRINT REGISTRY: Found existing table '%s' after cache timeout. "
                        "Forcing ADAPT_DATA for file '%s' without fresh analysis.",
                        created_table_from_registry,
                        entry_name
                    )
                    
                    # Build a decision using the existing table (use cached decision as template if available)
                    decision_to_use = dict(cached_after_wait.get("llm_decision", {})) if cached_after_wait else {}
                    decision_to_use["target_table"] = created_table_from_registry
                    decision_to_use["strategy"] = "ADAPT_DATA"
                    
                    applied_decision = _apply_forced_table_decision(
                        decision_to_use,
                        forced_table_name,
                        forced_table_mode,
                    )
                    
                    summary = _execute_cached_archive_decision(
                        entry_bytes=entry_bytes,
                        entry_name=entry_name,
                        llm_decision=applied_decision,
                        source_path=upload_result["file_path"],
                    )
                    
                    _log_archive_debug({
                        "event": "fingerprint_registry_enforcement_after_wait",
                        "archive_path": archive_path,
                        "entry_name": entry_name,
                        "fingerprint": fingerprint,
                        "enforced_table": created_table_from_registry,
                        "reason": "found_table_after_cache_timeout"
                    })
                else:
                    # No table in registry - proceed with fresh analysis
                    analysis_response, summary = _run_fresh_analysis()
                    
                    if fingerprint and analysis_response and analysis_response.llm_decision:
                        with fingerprint_lock:
                            event = cached_plan_event or (cached_after_wait or {}).get("event")
                            fingerprint_cache[fingerprint] = {
                                "llm_decision": analysis_response.llm_decision,
                                "event": event,
                            }
                            if event:
                                event.set()
                    
                    # Record the table that was actually created in the registry
                    if summary.get("status") == "processed" and fingerprint:
                        actual_table = summary.get("actual_table_name") or summary.get("table_name")
                        if actual_table:
                            with fingerprint_lock:
                                if fingerprint in fingerprint_cache:
                                    fingerprint_cache[fingerprint]["created_table"] = actual_table
                                    logger.info(
                                        "FINGERPRINT REGISTRY: Recorded table '%s' for fingerprint (waited path)",
                                        actual_table
                                    )
        else:
            # FINGERPRINT-TO-TABLE REGISTRY: Check if this fingerprint already has a created table
            # This handles the case where we do fresh analysis but another worker already created the table
            created_table_from_registry = None
            if fingerprint and not forced_table_name:
                with fingerprint_lock:
                    existing_cache = fingerprint_cache.get(fingerprint) or {}
                    created_table_from_registry = existing_cache.get("created_table")
            
            if created_table_from_registry:
                # Another file with same fingerprint already created a table - force merge
                logger.info(
                    "FINGERPRINT REGISTRY: Found existing table '%s' for fingerprint. "
                    "Forcing ADAPT_DATA for file '%s' without fresh analysis.",
                    created_table_from_registry,
                    entry_name
                )
                
                # Build a decision using the existing table
                decision_to_use = dict(cached_after_wait["llm_decision"]) if cached_after_wait and cached_after_wait.get("llm_decision") else {}
                decision_to_use["target_table"] = created_table_from_registry
                decision_to_use["strategy"] = "ADAPT_DATA"
                
                applied_decision = _apply_forced_table_decision(
                    decision_to_use,
                    forced_table_name,
                    forced_table_mode,
                )
                
                summary = _execute_cached_archive_decision(
                    entry_bytes=entry_bytes,
                    entry_name=entry_name,
                    llm_decision=applied_decision,
                )
                
                _log_archive_debug({
                    "event": "fingerprint_registry_enforcement_fresh_path",
                    "archive_path": archive_path,
                    "entry_name": entry_name,
                    "fingerprint": fingerprint,
                    "enforced_table": created_table_from_registry,
                    "reason": "fingerprint_already_has_table_from_registry"
                })
            else:
                # No table in registry - proceed with fresh analysis
                analysis_response, summary = _run_fresh_analysis()

                if fingerprint and analysis_response and analysis_response.llm_decision:
                    with fingerprint_lock:
                        existing_cache = fingerprint_cache.get(fingerprint) or {}
                        event = existing_cache.get("event") or cached_plan_event
                        fingerprint_cache[fingerprint] = {
                            "llm_decision": analysis_response.llm_decision,
                            "event": event,
                        }
                        if event:
                            event.set()
                
                # Record the table that was actually created in the registry
                if summary.get("status") == "processed" and fingerprint:
                    actual_table = summary.get("actual_table_name") or summary.get("table_name")
                    if actual_table:
                        with fingerprint_lock:
                            if fingerprint in fingerprint_cache:
                                fingerprint_cache[fingerprint]["created_table"] = actual_table
                                logger.info(
                                    "FINGERPRINT REGISTRY: Recorded table '%s' for fingerprint (fresh path)",
                                    actual_table
                                )

    except HTTPException as exc:
        msg = getattr(exc, "detail", None)
        if not msg:
            msg = str(exc)
        if not msg:
            msg = f"HTTPException {exc.status_code} (no detail provided)"
        summary = {
            "status": "failed",
            "message": msg,
            "auto_retry_used": False,
        }
        # Include LLM response if analysis got far enough to produce one
        if analysis_response and analysis_response.llm_response:
            summary["llm_response"] = analysis_response.llm_response
        if cached_plan_event:
            cached_plan_event.set()
    except Exception as exc:
        logger.exception("Auto-process failed for %s: %s", archive_path, exc)
        msg = str(exc)
        if not msg:
            msg = f"{type(exc).__name__}: (no error message)"
        summary = {
            "status": "failed",
            "message": msg,
            "auto_retry_used": False,
        }
        # Include LLM response if analysis got far enough to produce one
        if analysis_response and analysis_response.llm_response:
            summary["llm_response"] = analysis_response.llm_response
        if cached_plan_event:
            cached_plan_event.set()

    # Ensure we always surface a failure reason, even if a cached plan never arrived.
    summary = summary or {}
    if not summary.get("message"):
        fallback = "Automatic processing failed"
        if waited_for_cached_plan_without_decision:
            fallback += " after cache timeout waiting for parallel file's LLM decision"
        else:
            fallback += " - no error details captured (check server logs for exceptions)"
        summary["message"] = fallback
        # Even if we have no error message, include LLM response if available
        if analysis_response and analysis_response.llm_response:
            summary["llm_response"] = analysis_response.llm_response

    if cached_plan_event and fingerprint:
        with fingerprint_lock:
            existing_cache = fingerprint_cache.get(fingerprint) or {}
            event = existing_cache.get("event") or cached_plan_event
            fingerprint_cache[fingerprint] = {**existing_cache, "event": event}
            event.set()

    return ArchiveAutoProcessFileResult(
        archive_path=archive_path,
        stored_file_name=stored_file_name,
        uploaded_file_id=uploaded_file_id,
        sheet_name=sheet_name,
        status=summary.get("status", "failed"),
        table_name=summary.get("table_name"),
        records_processed=summary.get("records_processed"),
        duplicates_skipped=summary.get("duplicates_skipped"),
        validation_errors=summary.get("validation_errors"),
        mapping_errors=summary.get("mapping_errors"),
        import_id=summary.get("import_id"),
        auto_retry_used=summary.get("auto_retry_used", False),
        message=summary.get("message"),
        llm_response=summary.get("llm_response"),
    )


def _process_archive_entry(
    *,
    archive_path: str,
    entry_name: str,
    stored_file_name: str,
    archive_folder: str,
    zip_file: zipfile.ZipFile,
    zip_lock: threading.Lock,
    fingerprint_cache: Dict[str, Dict[str, Any]],
    fingerprint_lock: threading.Lock,
    analysis_mode: AnalysisMode,
    conflict_resolution: ConflictResolutionMode,
    auto_execute_confidence_threshold: float,
    max_iterations: int,
    forced_table_name: Optional[str],
    forced_table_mode: Optional[str],
    llm_instruction: Optional[str],
    parent_file_id: Optional[str] = None,
) -> ArchiveAutoProcessFileResult:
    """Process a single archive entry in a thread."""
    # Create a new database session for this thread
    SessionLocal = get_session_local()
    db_session = SessionLocal()
    
    try:
        # 1. Safe file reading (serialized)
        with zip_lock:
            try:
                entry_bytes = zip_file.read(archive_path)
            except KeyError:
                return ArchiveAutoProcessFileResult(
                    archive_path=archive_path,
                    stored_file_name=stored_file_name,
                    status="failed",
                    message="Unable to read archive member",
                )

        return _process_entry_bytes(
            entry_bytes=entry_bytes,
            archive_path=archive_path,
            entry_name=entry_name,
            stored_file_name=stored_file_name,
            archive_folder=archive_folder,
            fingerprint_cache=fingerprint_cache,
            fingerprint_lock=fingerprint_lock,
            analysis_mode=analysis_mode,
            conflict_resolution=conflict_resolution,
            auto_execute_confidence_threshold=auto_execute_confidence_threshold,
            max_iterations=max_iterations,
            forced_table_name=forced_table_name,
            forced_table_mode=forced_table_mode,
            llm_instruction=llm_instruction,
            db_session=db_session,
            parent_file_id=parent_file_id,
        )

    finally:
        db_session.close()


def _process_workbook_sheet_entry(
    *,
    sheet_name: str,
    entry_name: str,
    stored_file_name: str,
    archive_folder: str,
    entry_bytes: bytes,
    fingerprint_cache: Dict[str, Dict[str, Any]],
    fingerprint_lock: threading.Lock,
    analysis_mode: AnalysisMode,
    conflict_resolution: ConflictResolutionMode,
    auto_execute_confidence_threshold: float,
    max_iterations: int,
    forced_table_name: Optional[str],
    forced_table_mode: Optional[str],
    llm_instruction: Optional[str],
    parent_file_id: Optional[str] = None,
) -> ArchiveAutoProcessFileResult:
    """Process a single Excel sheet as an independent import."""
    SessionLocal = get_session_local()
    db_session = SessionLocal()
    try:
        return _process_entry_bytes(
            entry_bytes=entry_bytes,
            archive_path=sheet_name,
            entry_name=entry_name,
            stored_file_name=stored_file_name,
            archive_folder=archive_folder,
            fingerprint_cache=fingerprint_cache,
            fingerprint_lock=fingerprint_lock,
            analysis_mode=analysis_mode,
            conflict_resolution=conflict_resolution,
            auto_execute_confidence_threshold=auto_execute_confidence_threshold,
            max_iterations=max_iterations,
            forced_table_name=forced_table_name,
            forced_table_mode=forced_table_mode,
            llm_instruction=llm_instruction,
            db_session=db_session,
            sheet_name=sheet_name,
            parent_file_id=parent_file_id,
        )
    finally:
        db_session.close()


def _extract_supported_archive_entries(
    archive_bytes: bytes, allowed_paths: Optional[set[str]] = None
) -> tuple[list[zipfile.ZipInfo], list[ArchiveAutoProcessFileResult]]:
    """
    Return supported archive members and pre-populated skipped results.
    If allowed_paths is provided, only matching members are returned.
    """
    zip_file = zipfile.ZipFile(io.BytesIO(archive_bytes))
    try:
        skipped_results: list[ArchiveAutoProcessFileResult] = []
        supported_entries: list[zipfile.ZipInfo] = []
        normalized_allowed = set(allowed_paths) if allowed_paths is not None else None
        seen_paths: set[str] = set()

        for info in zip_file.infolist():
            if info.is_dir():
                continue
            archive_path = info.filename
            if not archive_path or archive_path.endswith("/"):
                continue
            
            # Skip macOS system files and hidden files
            if "__MACOSX" in archive_path or os.path.basename(archive_path).startswith("._"):
                continue

            if normalized_allowed is not None and archive_path not in normalized_allowed:
                continue

            lower_name = archive_path.lower()
            if not lower_name.endswith(ARCHIVE_SUPPORTED_SUFFIXES):
                skipped_results.append(
                    ArchiveAutoProcessFileResult(
                        archive_path=archive_path,
                        status="skipped",
                        message="Unsupported file type",
                    )
                )
                continue

            supported_entries.append(info)
            seen_paths.add(archive_path)

        if normalized_allowed is not None:
            missing_paths = normalized_allowed - seen_paths
            for missing in missing_paths:
                skipped_results.append(
                    ArchiveAutoProcessFileResult(
                        archive_path=missing,
                        status="failed",
                        message="Archive entry not found",
                    )
                )

        return supported_entries, skipped_results
    finally:
        zip_file.close()


def _run_archive_auto_process_job(
    *,
    file_id: str,
    archive_name: str,
    archive_bytes: bytes,
    supported_archive_paths: List[str],
    skipped_results: List[ArchiveAutoProcessFileResult],
    analysis_mode: AnalysisMode,
    conflict_resolution: ConflictResolutionMode,
    auto_execute_confidence_threshold: float,
    max_iterations: int,
    job_id: str,
    forced_table_name: Optional[str] = None,
    forced_table_mode: Optional[str] = None,
    llm_instruction: Optional[str] = None,
    prefilled_results: Optional[List[ArchiveAutoProcessFileResult]] = None,
) -> None:
    """Execute archive auto-processing off the main event loop."""
    import threading
    
    prefilled_results = prefilled_results or []
    prefilled_lookup = {result.archive_path: result for result in prefilled_results}
    initial_processed = sum(1 for result in prefilled_results if result.status == "processed")
    initial_failed = sum(1 for result in prefilled_results if result.status == "failed")
    initial_skipped = sum(1 for result in prefilled_results if result.status == "skipped")

    processed_files = 0
    failed_files = 0
    skipped_files = len(skipped_results)
    fingerprint_cache: Dict[str, Dict[str, Any]] = {}
    fingerprint_lock = threading.Lock()
    zip_lock = threading.Lock()
    
    results: List[ArchiveAutoProcessFileResult] = list(prefilled_results) + list(skipped_results)
    remaining_archive_paths = [path for path in supported_archive_paths if path not in prefilled_lookup]
    processing_paths = list(remaining_archive_paths)
    completed_archive_entries: List[Dict[str, str]] = [
        {"archive_path": res.archive_path, "status": res.status}
        for res in prefilled_results + skipped_results
    ]
    total_work_items = len(remaining_archive_paths) + initial_processed + initial_failed

    archive_stem = os.path.splitext(os.path.basename(archive_name))[0] or "archive"
    archive_folder = f"uploads/archive_{file_id}"

    _log_archive_debug(
        {
            "event": "job_started",
            "job_id": job_id,
            "file_id": file_id,
            "archive_name": archive_name,
            "supported_entries": list(processing_paths),
            "prefilled_processed": initial_processed,
            "prefilled_failed": initial_failed,
            "prefilled_skipped": initial_skipped,
            "forced_table_name": forced_table_name,
            "forced_table_mode": forced_table_mode,
        }
    )

    try:
        try:
            # ZipFile is not thread-safe for concurrent reads/seeks, so we must lock access
            zip_file = zipfile.ZipFile(io.BytesIO(archive_bytes))
        except zipfile.BadZipFile as exc:
            fail_active_job(file_id, job_id, f"Archive is corrupted: {exc}")
            return

        try:
            # Determine max workers - default to 4, but cap at CPU count
            max_workers = min(4, os.cpu_count() or 2)
            logger.info(f"Processing archive with {max_workers} parallel workers")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for index, archive_path in enumerate(processing_paths, start=1):
                    entry_name = os.path.basename(archive_path) or f"entry_{index:03d}"
                    stored_file_name = _build_archive_entry_name(
                        archive_stem, entry_name, index
                    )
                    
                    future = executor.submit(
                        _process_archive_entry,
                        archive_path=archive_path,
                        entry_name=entry_name,
                        stored_file_name=stored_file_name,
                        archive_folder=archive_folder,
                        zip_file=zip_file,
                        zip_lock=zip_lock,
                        fingerprint_cache=fingerprint_cache,
                        fingerprint_lock=fingerprint_lock,
                        analysis_mode=analysis_mode,
                        conflict_resolution=conflict_resolution,
                        auto_execute_confidence_threshold=auto_execute_confidence_threshold,
                        max_iterations=max_iterations,
                        forced_table_name=forced_table_name,
                        forced_table_mode=forced_table_mode,
                        llm_instruction=llm_instruction,
                        parent_file_id=file_id,
                    )
                    futures[future] = archive_path

                # Process results as they complete
                for future in as_completed(futures):
                    archive_path = futures[future]
                    try:
                        file_result = future.result()
                        results.append(file_result)

                        error_message = getattr(file_result, "message", None)
                        if not error_message and getattr(file_result, "status", None) == "failed":
                            error_message = (
                                f"Failed with no error message returned "
                                f"(status={getattr(file_result, 'status', None)}, path={archive_path})"
                            )

                        if getattr(file_result, "status", None) == "failed":
                            _log_mapping_failure(
                                {
                                    "event": "archive_entry_failed",
                                    "job_id": job_id,
                                    "file_id": file_id,
                                    "archive_name": archive_name,
                                    "archive_path": getattr(file_result, "archive_path", archive_path),
                                    "stored_file_name": getattr(file_result, "stored_file_name", None),
                                    "uploaded_file_id": getattr(file_result, "uploaded_file_id", None),
                                    "sheet_name": getattr(file_result, "sheet_name", None),
                                    "error": error_message,
                                    "analysis_mode": analysis_mode.value,
                                    "conflict_resolution": conflict_resolution.value,
                                    "forced_table_name": forced_table_name,
                                    "forced_table_mode": forced_table_mode,
                                    "raw_result": getattr(file_result, "model_dump", lambda: {})(),
                                }
                            )
                        
                        uploaded_file_id = getattr(file_result, "uploaded_file_id", None)
                        if file_result.status == "processed" and uploaded_file_id:
                            try:
                                update_file_status(
                                    uploaded_file_id,
                                    "mapped",
                                    mapped_table_name=getattr(file_result, "table_name", None),
                                    mapped_rows=getattr(file_result, "records_processed", None),
                                    duplicates_found=getattr(file_result, "duplicates_skipped", None),
                                    data_validation_errors=getattr(file_result, "validation_errors", None),
                                    mapping_errors=getattr(file_result, "mapping_errors", None),
                                )
                            except Exception as status_exc:  # pragma: no cover - defensive
                                logger.warning(
                                    "Archive entry %s mapped but status update failed for %s: %s",
                                    archive_path,
                                    uploaded_file_id,
                                    status_exc,
                                )
                        elif file_result.status == "failed" and uploaded_file_id:
                            try:
                                update_file_status(
                                    uploaded_file_id,
                                    "failed",
                                    error_message=getattr(file_result, "message", None),
                                )
                            except Exception as status_exc:  # pragma: no cover - defensive
                                logger.warning(
                                    "Archive entry %s failed but status update failed for %s: %s",
                                    archive_path,
                                    uploaded_file_id,
                                    status_exc,
                                )

                        if file_result.status == "processed":
                            processed_files += 1
                        elif file_result.status == "failed":
                            failed_files += 1

                        _log_archive_debug(
                            {
                                "event": "entry_finished",
                                "job_id": job_id,
                                "file_id": file_id,
                                "archive_name": archive_name,
                                "archive_path": getattr(file_result, "archive_path", archive_path),
                                "stored_file_name": getattr(file_result, "stored_file_name", None),
                                "uploaded_file_id": getattr(file_result, "uploaded_file_id", None),
                                "status": getattr(file_result, "status", None),
                                "table_name": getattr(file_result, "table_name", None),
                                "records_processed": getattr(file_result, "records_processed", None),
                                "duplicates_skipped": getattr(file_result, "duplicates_skipped", None),
                                "import_id": getattr(file_result, "import_id", None),
                                "auto_retry_used": getattr(file_result, "auto_retry_used", None),
                                "message": getattr(file_result, "message", None),
                            }
                        )
                            
                        completed_archive_entries.append(
                            {
                                "archive_path": archive_path,
                                "status": file_result.status,
                            }
                        )
                        if archive_path in remaining_archive_paths:
                            remaining_archive_paths.remove(archive_path)
                            
                        # Update job progress
                        total_handled = initial_processed + initial_failed + processed_files + failed_files
                        progress_denominator = total_work_items or 1
                        progress = int((total_handled / progress_denominator) * 100)
                        updated_processed = initial_processed + processed_files
                        updated_failed = initial_failed + failed_files
                        updated_skipped = initial_skipped + skipped_files
                        
                        update_import_job(
                            job_id,
                            stage="execution",
                            progress=progress,
                            metadata={
                                "current_file": archive_path,
                                "processed": updated_processed,
                                "failed": updated_failed,
                                "skipped": updated_skipped,
                                "total": total_work_items,
                                "completed_files": list(completed_archive_entries),
                                "remaining_files": list(remaining_archive_paths),
                                "files_in_archive": total_work_items + updated_skipped,
                                "source": "auto-process-archive",
                                "forced_table_name": forced_table_name,
                                "forced_table_mode": forced_table_mode,
                            },
                        )
                            
                    except Exception as exc:
                        logger.exception(f"Error processing archive entry {archive_path}: {exc}")
                        # Handle catastrophic worker failure
                        failed_files += 1
                        if archive_path in remaining_archive_paths:
                            remaining_archive_paths.remove(archive_path)
                        _log_archive_debug(
                            {
                                "event": "entry_error",
                                "job_id": job_id,
                                "file_id": file_id,
                                "archive_name": archive_name,
                                "archive_path": archive_path,
                                "error": str(exc),
                            }
                        )
                        _log_mapping_failure(
                            {
                                "event": "archive_entry_exception",
                                "job_id": job_id,
                                "file_id": file_id,
                                "archive_name": archive_name,
                                "archive_path": archive_path,
                                "error": str(exc),
                                "analysis_mode": analysis_mode.value,
                                "conflict_resolution": conflict_resolution.value,
                                "forced_table_name": forced_table_name,
                                "forced_table_mode": forced_table_mode,
                            }
                        )
        finally:
            zip_file.close()
            
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Archive auto-process background task failed: %s", exc)
        fail_active_job(file_id, job_id, f"Archive processing failed: {exc}")
        _log_archive_debug(
            {
                "event": "job_failed",
                "job_id": job_id,
                "file_id": file_id,
                "archive_name": archive_name,
                "error": str(exc),
            }
        )
        return

    processed_total = initial_processed + processed_files
    failed_total = initial_failed + failed_files
    skipped_total = initial_skipped + skipped_files
    total_supported = total_work_items
    total_files = total_supported + skipped_total
    success = failed_total == 0
    error_message = None if success else "One or more files failed auto-processing"

    result_metadata = {
        "files_total": total_supported,
        "processed_files": processed_total,
        "failed_files": failed_total,
        "skipped_files": skipped_total,
        "results": [result.model_dump() for result in results],
    }

    complete_import_job(
        job_id,
        success=success,
        error_message=error_message,
        result_metadata=result_metadata,
    )
    _log_archive_debug(
        {
            "event": "job_completed",
            "job_id": job_id,
            "file_id": file_id,
            "archive_name": archive_name,
            "processed_files": processed_total,
            "failed_files": failed_total,
            "skipped_files": skipped_total,
            "total_supported": total_supported,
            "total_files": total_files,
            "success": success,
            "error_message": error_message,
        }
    )


def _run_workbook_auto_process_job(
    *,
    file_id: str,
    workbook_name: str,
    sheet_entries: List[Tuple[str, bytes]],
    skipped_results: List[ArchiveAutoProcessFileResult],
    analysis_mode: AnalysisMode,
    conflict_resolution: ConflictResolutionMode,
    auto_execute_confidence_threshold: float,
    max_iterations: int,
    job_id: str,
    forced_table_name: Optional[str] = None,
    forced_table_mode: Optional[str] = None,
    llm_instruction: Optional[str] = None,
) -> None:
    """Execute auto-processing for each sheet in a workbook."""
    processed_files = 0
    failed_files = 0
    skipped_files = len(skipped_results)
    fingerprint_cache: Dict[str, Dict[str, Any]] = {}
    fingerprint_lock = threading.Lock()

    results: List[ArchiveAutoProcessFileResult] = list(skipped_results)
    remaining_sheet_names = [name for name, _ in sheet_entries]
    processing_sheet_names = list(remaining_sheet_names)
    completed_entries: List[Dict[str, str]] = [
        {"archive_path": res.archive_path, "status": res.status}
        for res in skipped_results
    ]

    workbook_stem = os.path.splitext(os.path.basename(workbook_name))[0] or "workbook"
    archive_folder = f"uploads/workbook_{file_id}"
    total_work_items = len(processing_sheet_names)

    _log_archive_debug(
        {
            "event": "workbook_job_started",
            "job_id": job_id,
            "file_id": file_id,
            "workbook_name": workbook_name,
            "sheets": list(processing_sheet_names),
            "forced_table_name": forced_table_name,
            "forced_table_mode": forced_table_mode,
        }
    )

    try:
        max_workers = min(4, os.cpu_count() or 2)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for index, (sheet_name, sheet_bytes) in enumerate(sheet_entries, start=1):
                entry_label = f"{sheet_name}.csv"
                stored_file_name = _build_archive_entry_name(
                    workbook_stem, entry_label, index
                )
                future = executor.submit(
                    _process_workbook_sheet_entry,
                    sheet_name=sheet_name,
                    entry_name=entry_label,
                    stored_file_name=stored_file_name,
                    archive_folder=archive_folder,
                    entry_bytes=sheet_bytes,
                    fingerprint_cache=fingerprint_cache,
                    fingerprint_lock=fingerprint_lock,
                    analysis_mode=analysis_mode,
                    conflict_resolution=conflict_resolution,
                    auto_execute_confidence_threshold=auto_execute_confidence_threshold,
                    max_iterations=max_iterations,
                    forced_table_name=forced_table_name,
                    forced_table_mode=forced_table_mode,
                    llm_instruction=llm_instruction,
                    parent_file_id=file_id,
                )
                futures[future] = sheet_name

            for future in as_completed(futures):
                sheet_name = futures[future]
                try:
                    file_result = future.result()
                    results.append(file_result)

                    error_message = getattr(file_result, "message", None)
                    if not error_message and getattr(file_result, "status", None) == "failed":
                        error_message = (
                            f"Failed with no error message returned "
                            f"(status={getattr(file_result, 'status', None)}, sheet={sheet_name})"
                        )

                    if getattr(file_result, "status", None) == "failed":
                        _log_mapping_failure(
                            {
                                "event": "workbook_entry_failed",
                                "job_id": job_id,
                                "file_id": file_id,
                                "workbook_name": workbook_name,
                                "sheet_name": getattr(file_result, "sheet_name", sheet_name),
                                "archive_path": getattr(file_result, "archive_path", sheet_name),
                                "stored_file_name": getattr(file_result, "stored_file_name", None),
                                "uploaded_file_id": getattr(file_result, "uploaded_file_id", None),
                                "error": error_message,
                                "analysis_mode": analysis_mode.value,
                                "conflict_resolution": conflict_resolution.value,
                                "forced_table_name": forced_table_name,
                                "forced_table_mode": forced_table_mode,
                                "raw_result": getattr(file_result, "model_dump", lambda: {})(),
                            }
                        )

                    uploaded_file_id = getattr(file_result, "uploaded_file_id", None)
                    if file_result.status == "processed" and uploaded_file_id:
                        try:
                            update_file_status(
                                uploaded_file_id,
                                "mapped",
                                mapped_table_name=getattr(file_result, "table_name", None),
                                mapped_rows=getattr(file_result, "records_processed", None),
                                duplicates_found=getattr(file_result, "duplicates_skipped", None),
                                data_validation_errors=getattr(file_result, "validation_errors", None),
                                mapping_errors=getattr(file_result, "mapping_errors", None),
                            )
                        except Exception as status_exc:  # pragma: no cover
                            logger.warning(
                                "Workbook sheet %s mapped but status update failed for %s: %s",
                                sheet_name,
                                uploaded_file_id,
                                status_exc,
                            )
                    elif file_result.status == "failed" and uploaded_file_id:
                        try:
                            update_file_status(
                                uploaded_file_id,
                                "failed",
                                error_message=getattr(file_result, "message", None),
                            )
                        except Exception as status_exc:  # pragma: no cover
                            logger.warning(
                                "Workbook sheet %s failed but status update failed for %s: %s",
                                sheet_name,
                                uploaded_file_id,
                                status_exc,
                            )

                    if file_result.status == "processed":
                        processed_files += 1
                    elif file_result.status == "failed":
                        failed_files += 1

                    _log_archive_debug(
                        {
                            "event": "workbook_entry_finished",
                            "job_id": job_id,
                            "file_id": file_id,
                            "workbook_name": workbook_name,
                            "sheet_name": getattr(file_result, "sheet_name", sheet_name),
                            "archive_path": getattr(file_result, "archive_path", sheet_name),
                            "stored_file_name": getattr(file_result, "stored_file_name", None),
                            "uploaded_file_id": getattr(file_result, "uploaded_file_id", None),
                            "status": getattr(file_result, "status", None),
                            "table_name": getattr(file_result, "table_name", None),
                            "records_processed": getattr(file_result, "records_processed", None),
                            "duplicates_skipped": getattr(file_result, "duplicates_skipped", None),
                            "import_id": getattr(file_result, "import_id", None),
                            "auto_retry_used": getattr(file_result, "auto_retry_used", None),
                            "message": getattr(file_result, "message", None),
                        }
                    )

                    completed_entries.append(
                        {
                            "archive_path": sheet_name,
                            "status": file_result.status,
                        }
                    )
                    if sheet_name in remaining_sheet_names:
                        remaining_sheet_names.remove(sheet_name)

                    total_handled = processed_files + failed_files
                    progress_denominator = total_work_items or 1
                    progress = int((total_handled / progress_denominator) * 100)
                    update_import_job(
                        job_id,
                        stage="execution",
                        progress=progress,
                        metadata={
                            "current_file": sheet_name,
                            "processed": processed_files,
                            "failed": failed_files,
                            "skipped": skipped_files,
                            "total": total_work_items,
                            "completed_files": list(completed_entries),
                            "remaining_files": list(remaining_sheet_names),
                            "files_in_archive": total_work_items + skipped_files,
                            "source": "auto-process-workbook",
                            "forced_table_name": forced_table_name,
                            "forced_table_mode": forced_table_mode,
                        },
                    )

                except Exception as exc:
                    logger.exception("Error processing workbook sheet %s: %s", sheet_name, exc)
                    failed_files += 1
                    if sheet_name in remaining_sheet_names:
                        remaining_sheet_names.remove(sheet_name)
                    _log_archive_debug(
                        {
                            "event": "workbook_entry_error",
                            "job_id": job_id,
                            "file_id": file_id,
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "error": str(exc),
                        }
                    )
                    _log_mapping_failure(
                        {
                            "event": "workbook_entry_exception",
                            "job_id": job_id,
                            "file_id": file_id,
                            "workbook_name": workbook_name,
                            "sheet_name": sheet_name,
                            "error": str(exc),
                            "analysis_mode": analysis_mode.value,
                            "conflict_resolution": conflict_resolution.value,
                            "forced_table_name": forced_table_name,
                            "forced_table_mode": forced_table_mode,
                        }
                    )

    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Workbook auto-process background task failed: %s", exc)
        fail_active_job(file_id, job_id, f"Workbook processing failed: {exc}")
        _log_archive_debug(
            {
                "event": "workbook_job_failed",
                "job_id": job_id,
                "file_id": file_id,
                "workbook_name": workbook_name,
                "error": str(exc),
            }
        )
        return

    success = failed_files == 0
    error_message = None if success else "One or more sheets failed auto-processing"
    result_metadata = {
        "files_total": total_work_items,
        "processed_files": processed_files,
        "failed_files": failed_files,
        "skipped_files": skipped_files,
        "results": [result.model_dump() for result in results],
    }

    complete_import_job(
        job_id,
        success=success,
        error_message=error_message,
        result_metadata=result_metadata,
    )
    _log_archive_debug(
        {
            "event": "workbook_job_completed",
            "job_id": job_id,
            "file_id": file_id,
            "workbook_name": workbook_name,
            "processed_files": processed_files,
            "failed_files": failed_files,
            "skipped_files": skipped_files,
            "total_supported": total_work_items,
            "success": success,
            "error_message": error_message,
        }
    )

async def _auto_retry_failed_auto_import(
    *,
    file_id: Optional[str],
    previous_error_message: str,
    max_iterations: int,
    db: Session,
    llm_instruction: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Attempt to recover from an auto-import failure by reusing the interactive Try Again flow.
    Returns metadata about the retry attempt, including any execution response.
    """
    result: Dict[str, Any] = {
        "success": False,
        "analysis_response": None,
        "execution_response": None,
        "error": None,
    }

    if not file_id:
        result["error"] = "Auto-retry requires a persisted upload (missing file_id)."
        return result

    try:
        interactive_request = AnalyzeFileInteractiveRequest(
            file_id=file_id,
            max_iterations=max_iterations,
            previous_error_message=previous_error_message,
            llm_instruction=llm_instruction,
        )
        interactive_response = await analyze_file_interactive_endpoint(
            request=interactive_request,
            db=db
        )
        result["analysis_response"] = interactive_response

        if not interactive_response.success:
            result["error"] = interactive_response.error or "Interactive analysis was unsuccessful."
            return result

        if not interactive_response.can_execute or not interactive_response.llm_decision:
            prompt_needed_msg = (
                "Interactive assistant requires user input before executing the retry plan."
            )
            if previous_error_message:
                prompt_needed_msg = (
                    f"{prompt_needed_msg} Last error: {previous_error_message}"
                )
            result["error"] = (
                f"{prompt_needed_msg} Open the Interactive tab for this file to review and approve the plan."
            )
            # Cleanup since we cannot move forward automatically
            interactive_sessions.pop(interactive_response.thread_id, None)
            return result

        execute_response = await execute_interactive_import_endpoint(
            request=ExecuteInteractiveImportRequest(
                file_id=file_id,
                thread_id=interactive_response.thread_id
            ),
            db=db
        )
        result["execution_response"] = execute_response
        result["success"] = execute_response.success
        if not execute_response.success:
            result["error"] = execute_response.message
        return result

    except HTTPException as exc:
        result["error"] = getattr(exc, "detail", str(exc))
        return result
    except Exception as exc:  # pragma: no cover - defensive
        result["error"] = str(exc)
        return result

def _get_analyze_file_for_import():
    """Return the analyze_file_for_import callable."""
    return _analyze_file_for_import


def _get_download_file_from_storage():
    """Return the download_file callable."""
    return _download_file_from_storage


def _get_execute_llm_import_decision():
    """Return the execute_llm_import_decision callable."""
    return auto_import.execute_llm_import_decision


@dataclass
class InteractiveSessionState:
    """In-memory state for an interactive mapping session."""
    file_id: str
    thread_id: str
    file_metadata: Dict[str, Any]
    sample: List[Dict[str, Any]]
    conversation: List[Dict[str, str]] = field(default_factory=list)
    initial_prompt_sent: bool = False
    llm_decision: Optional[Dict[str, Any]] = None
    max_iterations: int = 5
    last_error: Optional[str] = None
    status: str = "pending"
    job_id: Optional[str] = None
    sheet_name: Optional[str] = None
    llm_instruction: Optional[str] = None
    llm_instruction_id: Optional[str] = None
    skip_file_duplicate_check: bool = False

    def metadata_copy(self) -> Dict[str, Any]:
        """Return a safe copy of file metadata for the agent to mutate."""
        return dict(self.file_metadata)


def _store_interactive_session(session: InteractiveSessionState) -> None:
    """Persist interactive session state in the in-memory cache."""
    interactive_sessions[session.thread_id] = session


def _get_interactive_session(thread_id: str) -> InteractiveSessionState:
    """Retrieve an interactive session or raise if missing."""
    session = interactive_sessions.get(thread_id)
    if not isinstance(session, InteractiveSessionState):
        raise HTTPException(status_code=404, detail="Interactive session not found or expired")
    return session


def _build_interactive_initial_prompt(session: InteractiveSessionState) -> str:
    """Generate the initial message sent to the LLM in interactive mode."""
    file_name = session.file_metadata.get("name", "unknown")
    total_rows = session.file_metadata.get("total_rows", "unknown")
    sample_size = len(session.sample)
    sheet_name = session.file_metadata.get("sheet_name")
    prompt = (
        f"You are collaborating on an interactive import for file '{file_name}'.\n"
        f"Total rows: {total_rows}. Sample size: {sample_size}.\n"
    )
    if sheet_name:
        prompt += f"Sheet: {sheet_name}.\n"
    prompt += "\n"
    prompt += (
        "Analyze the data, recommend an import strategy, and explain your reasoning. "
        "Provide a numbered list of follow-up actions the user can take next "
        "(e.g., confirm mapping, rename columns, choose a different target table, "
        "create a new table, adjust duplicate handling). "
        "Wait for explicit confirmation before finalizing the mapping."
    )
    forced_table = session.file_metadata.get("forced_target_table")
    if forced_table:
        mode = session.file_metadata.get("forced_target_table_mode")
        mode_text = "existing table" if mode == "existing" else "new table" if mode == "new" else "specified table"
        prompt += (
            f"\nThe user requested mapping into the {mode_text} '{forced_table}'. "
            "Prioritize this target and avoid recommending a different table."
        )
    if session.llm_instruction:
        prompt += (
            "\n\nUser instruction to apply across this import:\n"
            f"{session.llm_instruction}"
        )
    if session.last_error:
        prompt += (
            "\n\nThe previous execution attempt failed with this error:\n"
            f"{session.last_error}\n"
            "Diagnose the failure, propose fixes, and wait for approval before recording a new decision."
        )
    return prompt


def _run_interactive_session_step(
    session: InteractiveSessionState,
    *,
    user_message: Optional[str],
    conversation_role: str = "user"
) -> AnalyzeFileInteractiveResponse:
    """Execute a single interactive LLM turn and update session state."""
    session.max_iterations = max(1, session.max_iterations)
    messages: List[Dict[str, str]] = []

    if not session.initial_prompt_sent:
        initial_prompt = _build_interactive_initial_prompt(session)
        messages.append({"role": "user", "content": initial_prompt})
        session.conversation.append({"role": "user", "content": initial_prompt})
        session.initial_prompt_sent = True

    if user_message is not None:
        normalized = user_message.strip()
        if not normalized:
            raise HTTPException(status_code=400, detail="user_message cannot be empty")
        messages.append({"role": "user", "content": normalized})
        session.conversation.append({"role": conversation_role, "content": normalized})
    elif session.initial_prompt_sent and not messages:
        raise HTTPException(status_code=400, detail="user_message required for ongoing interactive session")

    analysis_result = _invoke_analyzer(
        _get_analyze_file_for_import(),
        file_sample=session.sample,
        file_metadata=session.metadata_copy(),
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.ASK_USER,
        user_id=None,
        llm_instruction=session.llm_instruction,
        max_iterations=session.max_iterations,
        thread_id=session.thread_id,
        messages=messages,
        interactive_mode=True
    )

    if not analysis_result.get("success"):
        error_message = analysis_result.get("error", "LLM analysis failed")
        raise HTTPException(status_code=502, detail=error_message)

    llm_response = analysis_result["response"]
    session.conversation.append({"role": "assistant", "content": llm_response})
    session.llm_decision = analysis_result.get("llm_decision")
    
    if session.llm_decision and session.skip_file_duplicate_check:
        session.llm_decision["skip_file_duplicate_check"] = True

    session.status = "ready_to_execute" if session.llm_decision else "awaiting_user"
    session.last_error = None if session.llm_decision else session.last_error

    return AnalyzeFileInteractiveResponse(
        success=True,
        thread_id=session.thread_id,
        llm_message=llm_response,
        needs_user_input=session.llm_decision is None,
        question=None,
        options=None,
        can_execute=bool(session.llm_decision),
        llm_decision=session.llm_decision,
        iterations_used=analysis_result["iterations_used"],
        max_iterations=session.max_iterations,
        llm_instruction_id=session.llm_instruction_id,
    )


def _handle_interactive_execution_failure(
    session: InteractiveSessionState,
    error_message: str
) -> MapDataResponse:
    """Feed execution failure context back into the conversation."""
    session.last_error = error_message
    session.llm_decision = None
    failure_prompt = (
        "EXECUTION_FAILED\n"
        f"Error details: {error_message}\n"
        "Please analyze why this import failed and propose concrete fixes. "
        "Wait for confirmation before finalizing a new mapping."
    )

    followup = _run_interactive_session_step(
        session,
        user_message=failure_prompt,
        conversation_role="assistant"
    )

    if session.job_id:
        update_import_job(
            session.job_id,
            status="waiting_user",
            stage="analysis",
            error_message=error_message
        )

    response = MapDataResponse(
        success=False,
        message=f"Import execution failed: {error_message}",
        records_processed=0,
        table_name="",
        llm_followup=followup.llm_message,
        needs_user_input=followup.needs_user_input,
        can_execute=followup.can_execute,
        llm_decision=followup.llm_decision,
        thread_id=session.thread_id
    )
    if session.job_id:
        response.job_id = session.job_id
    return response


@router.post("/analyze-file", response_model=AnalyzeFileResponse)
async def analyze_file_endpoint(
    file: Optional[UploadFile] = File(None),
    file_id: Optional[str] = Form(None),
    sample_size: Optional[int] = Form(None),
    analysis_mode: AnalysisMode = Form(AnalysisMode.MANUAL),
    conflict_resolution: ConflictResolutionMode = Form(ConflictResolutionMode.ASK_USER),
    auto_execute_confidence_threshold: float = Form(0.9),
    max_iterations: int = Form(5),
    target_table_name: Optional[str] = Form(None),
    target_table_mode: Optional[str] = Form(None),
    llm_instruction: Optional[str] = Form(None),
    llm_instruction_id: Optional[str] = Form(None),
    save_llm_instruction: bool = Form(False),
    llm_instruction_title: Optional[str] = Form(None),
    require_explicit_multi_value: bool = Form(False),
    skip_file_duplicate_check: bool = Form(False),
    db: Session = Depends(get_db)
):
    """
    Analyze uploaded file and recommend import strategy using AI.
    
    This endpoint uses Claude Sonnet to intelligently analyze the file structure,
    compare it with existing database tables, and recommend the best import strategy.
    
    Parameters:
    - file: The file to analyze (CSV, Excel, JSON, or XML) - optional if file_id provided
    - file_id: UUID of previously uploaded file - optional if file provided
    - sample_size: Number of rows to sample (auto-calculated if not provided)
    - analysis_mode: MANUAL (user approval), AUTO_HIGH_CONFIDENCE, or AUTO_ALWAYS
    - conflict_resolution: ASK_USER, LLM_DECIDE, or PREFER_FLEXIBLE
    - auto_execute_confidence_threshold: Minimum confidence for auto-execution (0.0-1.0)
    - max_iterations: Maximum LLM iterations (1-10)
    """
    try:
        job_id: Optional[str] = None
        forced_table_name = _normalize_forced_table_name(target_table_name)
        forced_table_mode: Optional[str] = None
        normalized_instruction, saved_instruction_id = _resolve_llm_instruction(
            llm_instruction=llm_instruction,
            llm_instruction_id=llm_instruction_id,
            save_llm_instruction=save_llm_instruction,
            llm_instruction_title=llm_instruction_title,
        )
        if target_table_mode:
            normalized_mode = target_table_mode.strip().lower()
            if normalized_mode not in {"existing", "new"}:
                raise HTTPException(
                    status_code=400,
                    detail="target_table_mode must be either 'existing' or 'new'",
                )
            forced_table_mode = normalized_mode
        if forced_table_name and forced_table_mode is None:
            # Default to existing to avoid unintended new table creation when a name is forced
            forced_table_mode = "existing"
        require_explicit_multi_value = bool(require_explicit_multi_value)
        # Validate input: must provide either file or file_id
        if not file and not file_id:
            raise HTTPException(status_code=400, detail="Must provide either 'file' or 'file_id'")
        
        if file and file_id:
            raise HTTPException(status_code=400, detail="Cannot provide both 'file' and 'file_id'")
        
        # Get file content and name
        if file_id:
            # Fetch from uploaded_files and download from B2
            file_record = get_uploaded_file_by_id(file_id)
            if not file_record:
                raise HTTPException(status_code=404, detail=f"File {file_id} not found")

            if analysis_mode == AnalysisMode.AUTO_ALWAYS:
                try:
                    job = create_import_job(
                        file_id=file_id,
                        trigger_source="auto_process",
                        analysis_mode=analysis_mode.value,
                        conflict_mode=conflict_resolution.value,
                        metadata={"source": "analyze-file", "mode": analysis_mode.value}
                    )
                    job_id = job["id"]
                except Exception as job_exc:
                    logger.warning("Unable to create import job for file %s: %s", file_id, job_exc)

            # Update status to 'mapping'
            update_file_status(
                file_id,
                "mapping",
                expected_active_job_id=job_id if job_id else None
            )

            preloaded = _preloaded_file_contents.pop(file_id, None)
            if preloaded is not None:
                file_content = preloaded
            else:
                try:
                    file_content = _get_download_file_from_storage()(file_record["b2_file_path"])
                except StorageConnectionError as storage_exc:
                    # Handle network connectivity errors specifically
                    error_msg = str(storage_exc)
                    logger.error("Storage network error for file %s: %s", file_id, error_msg)

                    # Update file status to failed
                    update_file_status(file_id, "failed", error_message=error_msg)

                    # Fail the job if it was created
                    if job_id:
                        update_import_job(job_id, status="failed", error_message=error_msg)

                    # Return 503 Service Unavailable for network errors
                    raise HTTPException(status_code=503, detail=error_msg)
            file_name = file_record["file_name"]
        else:
            # Read uploaded file
            file_content = await file.read()
            file_name = file.filename
        
        # Detect and process file
        file_type = detect_file_type(file_name)
        
        # For CSV files, extract raw rows for LLM analysis WITHOUT parsing
        raw_csv_rows = None
        records = []
        
        if file_type == 'csv':
            raw_csv_rows = extract_raw_csv_rows(file_content, num_rows=100)

            # Load only a lightweight sample to avoid pulling whole multi-GB files into memory
            records = load_csv_sample(file_content, sample_rows=1000)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Smart sampling
        sample, total_rows = sample_file_data(records, sample_size, max_sample_size=50)
        
        # Prepare metadata
        file_metadata = {
            "name": file_name,
            "total_rows": total_rows,
            "file_type": file_type
        }
        if forced_table_name:
            file_metadata["forced_target_table"] = forced_table_name
        if forced_table_mode:
            file_metadata["forced_target_table_mode"] = forced_table_mode
        
        # Add raw CSV rows to metadata for LLM analysis
        if raw_csv_rows:
            file_metadata["raw_csv_rows"] = raw_csv_rows
        
        # Run AI analysis
        analysis_result = _invoke_analyzer(
            _get_analyze_file_for_import(),
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=analysis_mode,
            conflict_mode=conflict_resolution,
            user_id=None,  # Could be extracted from auth
            llm_instruction=normalized_instruction,
            max_iterations=max_iterations
        )
        
        if not analysis_result.get("success", False):
            error_message = analysis_result.get("error", "LLM analysis failed")
            raise HTTPException(status_code=502, detail=error_message)
        
        # Check if LLM made a decision
        if not analysis_result.get("success", False):
            error_message = analysis_result.get("error", "LLM analysis failed")
            raise HTTPException(status_code=502, detail=error_message)

        llm_decision = analysis_result.get("llm_decision")
        print(f"DEBUG: analyze_file_endpoint - llm_decision (before force)={llm_decision}")
        if forced_table_name:
            llm_decision = _apply_forced_table_decision(
                llm_decision,
                forced_table_name,
                forced_table_mode,
            )
        print(f"DEBUG: analyze_file_endpoint - llm_decision (after force)={llm_decision}")
        if llm_decision is not None:
            llm_decision = dict(llm_decision)
            if normalized_instruction:
                llm_decision.setdefault("llm_instruction", normalized_instruction)
            llm_decision["require_explicit_multi_value"] = require_explicit_multi_value
            llm_decision["skip_file_duplicate_check"] = skip_file_duplicate_check
        
        # Parse LLM response to extract structured data
        response = AnalyzeFileResponse(
            success=True,
            llm_response=analysis_result["response"],
            iterations_used=analysis_result["iterations_used"],
            max_iterations=max_iterations,
            can_auto_execute=False,  # Will be set below based on analysis_mode
            llm_decision=llm_decision,
            needs_user_input=llm_decision is None,
            llm_instruction_id=saved_instruction_id,
        )
        if job_id:
            response.job_id = job_id
        
        # Determine can_auto_execute based on analysis_mode
        if analysis_mode == AnalysisMode.AUTO_ALWAYS:
            response.can_auto_execute = llm_decision is not None
            if not llm_decision:
                # AUTO_ALWAYS requires a decision. If missing, it's a failure.
                error_msg = "Automatic processing failed: AI could not determine a confident import strategy without user input."
                response.success = False
                response.error = error_msg
                
                # Cleanup job state so it doesn't hang in 'running'
                if job_id:
                    update_import_job(
                        job_id, 
                        status="failed", 
                        error_message=error_msg,
                        stage="analysis"
                    )
                # Ensure file status reflects failure
                if file_id:
                    update_file_status(
                        file_id, 
                        "failed", 
                        error_message=error_msg,
                        expected_active_job_id=job_id
                    )

        elif analysis_mode == AnalysisMode.AUTO_HIGH_CONFIDENCE:
            # Would need to parse confidence from LLM response
            response.can_auto_execute = False  # Conservative default
        else:  # MANUAL
            response.can_auto_execute = False
        
        # Store analysis result for later execution
        analysis_id = str(uuid.uuid4())
        analysis_storage[analysis_id] = response
        
        # AUTO-EXECUTION LOGIC (separate from determining can_auto_execute)
        if analysis_mode == AnalysisMode.AUTO_ALWAYS and llm_decision:
            # Execute the import automatically
            try:
                print(f"DEBUG: analyze_file_endpoint - Calling execute_llm_import_decision with {llm_decision}")
                execution_result = _get_execute_llm_import_decision()(
                    file_content=file_content,
                    file_name=file_name,
                    all_records=records,  # Use all records, not just sample
                    llm_decision=llm_decision,
                    source_path=file_record.get("b2_file_path") if file_id else None
                )
                
                response.auto_execution_result = AutoExecutionResult(**execution_result)
                response.auto_execution_error = None

                if execution_result["success"]:
                    # Update file status to 'mapped' if file_id was provided
                    if file_id:
                        if job_id:
                            complete_import_job(
                                job_id,
                                success=True,
                                result_metadata={
                                    "table_name": execution_result["table_name"],
                                    "records_processed": execution_result["records_processed"]
                                },
                                mapped_table_name=execution_result["table_name"],
                                mapped_rows=execution_result["records_processed"],
                                data_validation_errors=execution_result.get("validation_errors", 0),
                                duplicates_found=execution_result.get("duplicates_skipped"),
                                mapping_errors=len(execution_result.get("mapping_errors", []))
                            )
                            job_id = None
                        else:
                            update_file_status(
                                file_id,
                                "mapped",
                                mapped_table_name=execution_result["table_name"],
                                mapped_rows=execution_result["records_processed"],
                                duplicates_found=execution_result.get("duplicates_skipped"),
                                data_validation_errors=execution_result.get("validation_errors", 0),
                                mapping_errors=len(execution_result.get("mapping_errors", [])),
                            )
                    
                    # Update response with execution results
                    response.can_auto_execute = True
                    # Add execution info to response (note: this extends the schema)
                    response.llm_response += f"\n\n✅ AUTO-EXECUTION COMPLETED:\n"
                    response.llm_response += f"- Strategy: {execution_result['strategy_executed']}\n"
                    response.llm_response += f"- Table: {execution_result['table_name']}\n"
                    response.llm_response += f"- Records Processed: {execution_result['records_processed']}\n"
                    response.needs_user_input = False
                else:
                    # Update file status to 'failed' if file_id was provided
                    error_msg = execution_result.get('error', 'Unknown error')
                    response.auto_execution_error = error_msg
                    auto_retry_details = None
                    if settings.enable_auto_retry_failed_imports:
                        auto_retry_details = await _auto_retry_failed_auto_import(
                            file_id=file_id,
                            previous_error_message=error_msg,
                            max_iterations=max_iterations,
                            db=db,
                            llm_instruction=normalized_instruction,
                        )
                    if auto_retry_details:
                        response.auto_retry_attempted = True
                    
                    if auto_retry_details and auto_retry_details.get("success"):
                        retry_result: MapDataResponse = auto_retry_details["execution_response"]
                        response.auto_retry_error = None
                        response.auto_retry_execution_result = retry_result
                        response.llm_response += f"\n\n❌ AUTO-EXECUTION FAILED:\n"
                        response.llm_response += f"- Error: {error_msg}\n"
                        response.llm_response += (
                            "\n🔁 AUTO-RETRY EXECUTED VIA INTERACTIVE ASSISTANT:\n"
                        )
                        response.llm_response += f"- Table: {retry_result.table_name}\n"
                        response.llm_response += f"- Records Processed: {retry_result.records_processed}\n"
                        response.llm_response += f"- Duplicates Skipped: {retry_result.duplicates_skipped}\n"
                        response.can_auto_execute = True
                        response.needs_user_input = False
                        if file_id:
                            if job_id:
                                complete_import_job(
                                    job_id,
                                    success=True,
                                    result_metadata={
                                        "table_name": retry_result.table_name,
                                        "records_processed": retry_result.records_processed,
                                        "duplicates_skipped": retry_result.duplicates_skipped,
                                        "import_id": retry_result.import_id,
                                    },
                                    mapped_table_name=retry_result.table_name,
                                    mapped_rows=retry_result.records_processed,
                                    data_validation_errors=retry_result.validation_errors or 0,
                                    duplicates_found=retry_result.duplicates_skipped,
                                    mapping_errors=len(retry_result.mapping_errors or [])
                                )
                                job_id = None
                            else:
                                update_file_status(
                                    file_id,
                                    "mapped",
                                    mapped_table_name=retry_result.table_name,
                                    mapped_rows=retry_result.records_processed,
                                    duplicates_found=retry_result.duplicates_skipped,
                                    data_validation_errors=retry_result.validation_errors or 0,
                                    mapping_errors=len(retry_result.mapping_errors or [])
                                )
                    else:
                        if auto_retry_details:
                            response.auto_retry_error = auto_retry_details.get("error")
                        fallback_error = (
                            auto_retry_details.get("error")
                            if auto_retry_details and auto_retry_details.get("error")
                            else error_msg
                        )
                        response.auto_execution_error = fallback_error
                        if file_id:
                            if job_id:
                                update_import_job(
                                    job_id,
                                    status="waiting_user",
                                    stage="analysis",
                                    error_message=fallback_error
                                )
                                update_file_status(
                                    file_id,
                                    "failed",
                                    error_message=fallback_error,
                                    expected_active_job_id=job_id
                                )
                        else:
                            update_file_status(file_id, "failed", error_message=fallback_error)

                        _log_mapping_failure(
                            {
                                "event": "auto_process_failed",
                                "source": "auto_process_single",
                                "file_id": file_id,
                                "file_name": file_name,
                                "job_id": job_id,
                                "error": fallback_error,
                                "analysis_mode": analysis_mode.value,
                                "conflict_resolution": conflict_resolution.value,
                                "strategy": (llm_decision or {}).get("strategy") if llm_decision else None,
                                "target_table": (llm_decision or {}).get("target_table") if llm_decision else None,
                                "auto_retry_attempted": bool(auto_retry_details),
                            }
                        )

                        response.llm_response += f"\n\n❌ AUTO-EXECUTION FAILED:\n"
                        response.llm_response += f"- Error: {error_msg}\n"
                        if auto_retry_details and auto_retry_details.get("error"):
                            response.llm_response += (
                                f"- Auto-retry attempt failed: {auto_retry_details['error']}\n"
                            )
                        response.can_auto_execute = False
                        response.needs_user_input = True

            except Exception as e:
                # Update file status to 'failed' if file_id was provided
                error_msg = str(e)
                response.auto_execution_error = error_msg
                auto_retry_details = None
                if settings.enable_auto_retry_failed_imports:
                    auto_retry_details = await _auto_retry_failed_auto_import(
                        file_id=file_id,
                        previous_error_message=error_msg,
                        max_iterations=max_iterations,
                        db=db,
                        llm_instruction=normalized_instruction,
                    )
                if auto_retry_details:
                    response.auto_retry_attempted = True

                if auto_retry_details and auto_retry_details.get("success"):
                    retry_result: MapDataResponse = auto_retry_details["execution_response"]
                    response.auto_retry_error = None
                    response.auto_retry_execution_result = retry_result
                    response.llm_response += f"\n\n❌ AUTO-EXECUTION ERROR: {error_msg}\n"
                    response.llm_response += (
                        "\n🔁 AUTO-RETRY EXECUTED VIA INTERACTIVE ASSISTANT:\n"
                    )
                    response.llm_response += f"- Table: {retry_result.table_name}\n"
                    response.llm_response += f"- Records Processed: {retry_result.records_processed}\n"
                    response.llm_response += f"- Duplicates Skipped: {retry_result.duplicates_skipped}\n"
                    response.can_auto_execute = True
                    if file_id:
                        if job_id:
                            complete_import_job(
                                job_id,
                                success=True,
                                result_metadata={
                                    "table_name": retry_result.table_name,
                                    "records_processed": retry_result.records_processed,
                                    "duplicates_skipped": retry_result.duplicates_skipped,
                                    "import_id": retry_result.import_id,
                                },
                                mapped_table_name=retry_result.table_name,
                                mapped_rows=retry_result.records_processed,
                                data_validation_errors=retry_result.validation_errors or 0,
                                duplicates_found=retry_result.duplicates_skipped,
                                mapping_errors=len(retry_result.mapping_errors or [])
                            )
                            job_id = None
                        else:
                            update_file_status(
                                file_id,
                                "mapped",
                                mapped_table_name=retry_result.table_name,
                                mapped_rows=retry_result.records_processed,
                                duplicates_found=retry_result.duplicates_skipped,
                                data_validation_errors=retry_result.validation_errors or 0,
                                mapping_errors=len(retry_result.mapping_errors or [])
                            )
                else:
                    if auto_retry_details:
                        response.auto_retry_error = auto_retry_details.get("error")
                    final_error = (
                        auto_retry_details.get("error")
                        if auto_retry_details and auto_retry_details.get("error")
                        else error_msg
                    )
                    response.auto_execution_error = final_error
                    if file_id:
                        if job_id:
                            complete_import_job(
                                job_id,
                                success=False,
                                error_message=final_error
                            )
                            job_id = None
                        else:
                            update_file_status(file_id, "failed", error_message=final_error)

                    _log_mapping_failure(
                        {
                            "event": "auto_process_failed",
                            "source": "auto_process_single",
                            "file_id": file_id,
                            "file_name": file_name,
                            "job_id": job_id,
                            "error": final_error,
                            "analysis_mode": analysis_mode.value,
                            "conflict_resolution": conflict_resolution.value,
                            "strategy": (llm_decision or {}).get("strategy") if llm_decision else None,
                            "target_table": (llm_decision or {}).get("target_table") if llm_decision else None,
                            "auto_retry_attempted": bool(auto_retry_details),
                        }
                    )

                    response.llm_response += f"\n\n❌ AUTO-EXECUTION ERROR: {error_msg}\n"
                    if auto_retry_details and auto_retry_details.get("error"):
                        response.llm_response += (
                            f"- Auto-retry attempt failed: {auto_retry_details['error']}\n"
                        )
                    response.can_auto_execute = False
                    response.needs_user_input = True

        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.get("/workbooks/{file_id}/sheets", response_model=WorkbookSheetsResponse)
async def list_workbook_sheets_endpoint(file_id: str):
    """Return sheet names for an uploaded Excel workbook."""
    file_record = get_uploaded_file_by_id(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    file_name = file_record["file_name"]
    if not file_name.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File is not an Excel workbook")

    file_content = _get_download_file_from_storage()(file_record["b2_file_path"])
    try:
        sheets = list_excel_sheets(file_content)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return WorkbookSheetsResponse(success=True, sheets=sheets)


@router.post("/auto-process-archive", response_model=ArchiveAutoProcessResponse)
async def auto_process_archive_endpoint(
    file_id: str = Form(...),
    analysis_mode: AnalysisMode = Form(AnalysisMode.AUTO_ALWAYS),
    conflict_resolution: ConflictResolutionMode = Form(ConflictResolutionMode.LLM_DECIDE),
    auto_execute_confidence_threshold: float = Form(0.9),
    max_iterations: int = Form(5),
    target_table_name: Optional[str] = Form(None),
    target_table_mode: Optional[str] = Form(None),
    llm_instruction: Optional[str] = Form(None),
    llm_instruction_id: Optional[str] = Form(None),
    save_llm_instruction: bool = Form(False),
    llm_instruction_title: Optional[str] = Form(None),
):
    """
    Queue background processing for every supported file contained within a ZIP archive.

    The endpoint returns immediately with a job id so the request lifecycle
    does not control the processing. Results are persisted to the import job
    metadata and can be fetched via /import-jobs/{job_id}.
    """
    if analysis_mode != AnalysisMode.AUTO_ALWAYS:
        raise HTTPException(
            status_code=400,
            detail="Archive processing currently supports auto process mode only.",
        )
    forced_table_name = _normalize_forced_table_name(target_table_name)
    forced_table_mode: Optional[str] = None
    if target_table_mode:
        normalized_mode = target_table_mode.strip().lower()
        if normalized_mode not in {"existing", "new"}:
            raise HTTPException(
                status_code=400,
                detail="target_table_mode must be either 'existing' or 'new'",
            )
        forced_table_mode = normalized_mode
    if forced_table_name and forced_table_mode is None:
        forced_table_mode = "existing"

    print(f"DEBUG: analyze_file_endpoint - target_table_name={target_table_name!r}, target_table_mode={target_table_mode!r}")
    print(f"DEBUG: analyze_file_endpoint - forced_table_name={forced_table_name!r}, forced_table_mode={forced_table_mode!r}")

    normalized_instruction, saved_instruction_id = _resolve_llm_instruction(
        llm_instruction=llm_instruction,
        llm_instruction_id=llm_instruction_id,
        save_llm_instruction=save_llm_instruction,
        llm_instruction_title=llm_instruction_title,
    )

    file_record = get_uploaded_file_by_id(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    archive_name = file_record["file_name"]
    if not archive_name.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="File is not a ZIP archive")

    try:
        archive_bytes = _get_download_file_from_storage()(file_record["b2_file_path"])
    except StorageConnectionError as storage_exc:
        # Handle network connectivity errors specifically
        error_msg = str(storage_exc)
        logger.error("Storage network error downloading archive %s: %s", file_id, error_msg)

        # Update file status to failed
        update_file_status(file_id, "failed", error_message=error_msg)

        # Return 503 Service Unavailable for network errors
        raise HTTPException(status_code=503, detail=error_msg)

    try:
        supported_entries, skipped_results = _extract_supported_archive_entries(archive_bytes)
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail="Archive is corrupted") from exc

    if not supported_entries:
        raise HTTPException(
            status_code=400,
            detail="Archive does not contain CSV or Excel files.",
        )

    remaining_archive_paths = [info.filename for info in supported_entries]

    job = create_import_job(
        file_id=file_id,
        trigger_source="archive_auto_process",
        analysis_mode=analysis_mode.value,
        conflict_mode=conflict_resolution.value,
        metadata={
            "source": "auto-process-archive",
            "files_in_archive": len(supported_entries),
            "remaining_files": list(remaining_archive_paths),
            "completed_files": [],
            "forced_table_name": forced_table_name,
            "forced_table_mode": forced_table_mode,
        },
    )
    job_id = job["id"]
    update_file_status(file_id, "mapping", expected_active_job_id=job_id)

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        lambda: _run_archive_auto_process_job(
            file_id=file_id,
            archive_name=archive_name,
            archive_bytes=archive_bytes,
            supported_archive_paths=[info.filename for info in supported_entries],
            skipped_results=skipped_results,
            analysis_mode=analysis_mode,
            conflict_resolution=conflict_resolution,
            auto_execute_confidence_threshold=auto_execute_confidence_threshold,
            max_iterations=max_iterations,
            job_id=job_id,
            forced_table_name=forced_table_name,
            forced_table_mode=forced_table_mode,
            llm_instruction=normalized_instruction,
        ),
    )

    return ArchiveAutoProcessResponse(
        success=True,
        total_files=len(supported_entries) + len(skipped_results),
        processed_files=0,
        failed_files=0,
        skipped_files=len(skipped_results),
        results=skipped_results,
        job_id=job_id,
    )


@router.post("/auto-process-archive/resume", response_model=ArchiveAutoProcessResponse)
async def resume_auto_process_archive_endpoint(
    file_id: str = Form(...),
    from_job_id: Optional[str] = Form(None),
    resume_failed_entries_only: bool = Form(True),
    analysis_mode: AnalysisMode = Form(AnalysisMode.AUTO_ALWAYS),
    conflict_resolution: ConflictResolutionMode = Form(ConflictResolutionMode.LLM_DECIDE),
    auto_execute_confidence_threshold: float = Form(0.9),
    max_iterations: int = Form(5),
    target_table_name: Optional[str] = Form(None),
    target_table_mode: Optional[str] = Form(None),
    llm_instruction: Optional[str] = Form(None),
    llm_instruction_id: Optional[str] = Form(None),
    save_llm_instruction: bool = Form(False),
    llm_instruction_title: Optional[str] = Form(None),
):
    """
    Resume archive auto-processing using a previous job for state.

    By default, only failed or unprocessed entries from the referenced job
    are retried. Set resume_failed_entries_only=False to reprocess the entire
    archive from scratch.
    """
    if analysis_mode != AnalysisMode.AUTO_ALWAYS:
        raise HTTPException(
            status_code=400,
            detail="Archive processing currently supports auto process mode only.",
        )
    forced_table_name = _normalize_forced_table_name(target_table_name)
    forced_table_mode: Optional[str] = None
    if target_table_mode:
        normalized_mode = target_table_mode.strip().lower()
        if normalized_mode not in {"existing", "new"}:
            raise HTTPException(
                status_code=400,
                detail="target_table_mode must be either 'existing' or 'new'",
            )
        forced_table_mode = normalized_mode

    normalized_instruction, saved_instruction_id = _resolve_llm_instruction(
        llm_instruction=llm_instruction,
        llm_instruction_id=llm_instruction_id,
        save_llm_instruction=save_llm_instruction,
        llm_instruction_title=llm_instruction_title,
    )

    file_record = get_uploaded_file_by_id(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    archive_name = file_record["file_name"]
    if not archive_name.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="File is not a ZIP archive")

    prefilled_results: List[ArchiveAutoProcessFileResult] = []
    allowed_paths: Optional[set[str]] = None

    if from_job_id:
        previous_job = get_import_job(from_job_id)
        if not previous_job:
            raise HTTPException(status_code=404, detail="Import job not found")
        if previous_job["file_id"] != file_id:
            raise HTTPException(
                status_code=400, detail="Job does not belong to the provided file"
            )
        if previous_job.get("trigger_source") != "archive_auto_process":
            raise HTTPException(
                status_code=400, detail="Job is not an archive auto-process run"
            )

        previous_metadata = previous_job.get("metadata") or {}
        if not forced_table_name:
            forced_table_name = previous_metadata.get("forced_table_name")
        if not forced_table_mode:
            forced_table_mode = previous_metadata.get("forced_table_mode")
        allowed_paths = set((previous_job.get("metadata") or {}).get("remaining_files") or [])
        previous_results = (previous_job.get("result_metadata") or {}).get("results") or []

        if resume_failed_entries_only:
            for raw_result in previous_results:
                result = ArchiveAutoProcessFileResult(**raw_result)
                if result.status == "failed":
                    allowed_paths.add(result.archive_path)
                else:
                    prefilled_results.append(result)
            if not allowed_paths:
                raise HTTPException(
                    status_code=400,
                    detail="No failed or pending archive entries to resume.",
                )
        else:
            allowed_paths = None  # Reprocess everything

    if forced_table_name and forced_table_mode is None:
        forced_table_mode = "existing"
    if forced_table_name:
        forced_table_name = _normalize_forced_table_name(forced_table_name)

    archive_bytes = _get_download_file_from_storage()(file_record["b2_file_path"])
    try:
        supported_entries, extracted_skipped_results = _extract_supported_archive_entries(
            archive_bytes, allowed_paths=allowed_paths
        )
    except zipfile.BadZipFile as exc:
        raise HTTPException(status_code=400, detail="Archive is corrupted") from exc

    failed_carryovers = [result for result in extracted_skipped_results if result.status == "failed"]
    skipped_results = [result for result in extracted_skipped_results if result.status == "skipped"]
    prefilled_results.extend(failed_carryovers)

    if not supported_entries and not prefilled_results:
        raise HTTPException(
            status_code=400,
            detail="Archive does not contain CSV or Excel files.",
        )

    remaining_archive_paths = [info.filename for info in supported_entries]
    completed_seed = [
        {"archive_path": res.archive_path, "status": res.status}
        for res in prefilled_results + skipped_results
    ]

    job_metadata = {
        "source": "auto-process-archive",
        "files_in_archive": len(remaining_archive_paths) + len(completed_seed),
        "remaining_files": list(remaining_archive_paths),
        "completed_files": completed_seed,
        "forced_table_name": forced_table_name,
        "forced_table_mode": forced_table_mode,
    }
    if from_job_id:
        job_metadata["resume_of_job_id"] = from_job_id
        job_metadata["resume_failed_entries_only"] = resume_failed_entries_only

    job = create_import_job(
        file_id=file_id,
        trigger_source="archive_auto_process",
        analysis_mode=analysis_mode.value,
        conflict_mode=conflict_resolution.value,
        metadata=job_metadata,
    )
    job_id = job["id"]
    update_file_status(file_id, "mapping", expected_active_job_id=job_id)

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        lambda: _run_archive_auto_process_job(
            file_id=file_id,
            archive_name=archive_name,
            archive_bytes=archive_bytes,
            supported_archive_paths=remaining_archive_paths,
            skipped_results=skipped_results,
            analysis_mode=analysis_mode,
            conflict_resolution=conflict_resolution,
            auto_execute_confidence_threshold=auto_execute_confidence_threshold,
            max_iterations=max_iterations,
            job_id=job_id,
            prefilled_results=prefilled_results,
            forced_table_name=forced_table_name,
            forced_table_mode=forced_table_mode,
            llm_instruction=normalized_instruction,
        ),
    )

    processed_prefilled = sum(1 for res in prefilled_results if res.status == "processed")
    failed_prefilled = sum(1 for res in prefilled_results if res.status == "failed")
    skipped_prefilled = len(skipped_results) + sum(
        1 for res in prefilled_results if res.status == "skipped"
    )

    return ArchiveAutoProcessResponse(
        success=True,
        total_files=len(remaining_archive_paths) + len(skipped_results) + len(prefilled_results),
        processed_files=processed_prefilled,
        failed_files=failed_prefilled,
        skipped_files=skipped_prefilled,
        results=[*prefilled_results, *skipped_results],
        job_id=job_id,
    )


def _parse_sheet_names_param(raw_sheet_names: Optional[str]) -> Optional[List[str]]:
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


@router.post("/auto-process-workbook", response_model=ArchiveAutoProcessResponse)
async def auto_process_workbook_endpoint(
    file_id: str = Form(...),
    sheet_names: Optional[str] = Form(None),
    analysis_mode: AnalysisMode = Form(AnalysisMode.AUTO_ALWAYS),
    conflict_resolution: ConflictResolutionMode = Form(ConflictResolutionMode.LLM_DECIDE),
    auto_execute_confidence_threshold: float = Form(0.9),
    max_iterations: int = Form(5),
    target_table_name: Optional[str] = Form(None),
    target_table_mode: Optional[str] = Form(None),
    llm_instruction: Optional[str] = Form(None),
):
    """
    Auto-process each sheet in an Excel workbook as independent imports.

    Behaves similarly to archive auto-processing: each sheet is uploaded as a
    separate file (named with the workbook + sheet) and processed in parallel.
    """
    if analysis_mode != AnalysisMode.AUTO_ALWAYS:
        raise HTTPException(
            status_code=400,
            detail="Workbook processing currently supports auto process mode only.",
        )
    forced_table_name = _normalize_forced_table_name(target_table_name)
    forced_table_mode: Optional[str] = None
    if target_table_mode:
        normalized_mode = target_table_mode.strip().lower()
        if normalized_mode not in {"existing", "new"}:
            raise HTTPException(
                status_code=400,
                detail="target_table_mode must be either 'existing' or 'new'",
            )
        forced_table_mode = normalized_mode
    if forced_table_name and forced_table_mode is None:
        forced_table_mode = "existing"
    normalized_instruction = (llm_instruction or "").strip() or None

    file_record = get_uploaded_file_by_id(file_id)
    if not file_record:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    workbook_name = file_record["file_name"]
    if not workbook_name.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File is not an Excel workbook")

    try:
        workbook_bytes = _get_download_file_from_storage()(file_record["b2_file_path"])
    except StorageConnectionError as storage_exc:
        # Handle network connectivity errors specifically
        error_msg = str(storage_exc)
        logger.error("Storage network error downloading workbook %s: %s", file_id, error_msg)

        # Update file status to failed
        update_file_status(file_id, "failed", error_message=error_msg)

        # Return 503 Service Unavailable for network errors
        raise HTTPException(status_code=503, detail=error_msg)

    try:
        available_sheets = list_excel_sheets(workbook_bytes)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    requested_sheets = _parse_sheet_names_param(sheet_names)
    sheet_selection = (
        [name for name in requested_sheets or [] if name]
        if requested_sheets
        else available_sheets
    )

    available_lookup = {name.lower(): name for name in available_sheets}
    skipped_results: List[ArchiveAutoProcessFileResult] = []
    normalized_selection: List[str] = []
    for name in sheet_selection:
        key = name.lower()
        if key not in available_lookup:
            skipped_results.append(
                ArchiveAutoProcessFileResult(
                    archive_path=name,
                    sheet_name=name,
                    status="failed",
                    message="Sheet not found in workbook",
                )
            )
            continue
        resolved_name = available_lookup[key]
        if resolved_name not in normalized_selection:
            normalized_selection.append(resolved_name)

    if not normalized_selection and not skipped_results:
        raise HTTPException(status_code=400, detail="No sheets selected for processing")

    sheet_entries: List[tuple[str, bytes]] = []
    for sheet_name in normalized_selection:
        try:
            sheet_bytes = extract_excel_sheet_csv_bytes(workbook_bytes, sheet_name)
            sheet_entries.append((sheet_name, sheet_bytes))
        except Exception as exc:
            logger.warning("Failed to extract sheet %s from %s: %s", sheet_name, workbook_name, exc)
            skipped_results.append(
                ArchiveAutoProcessFileResult(
                    archive_path=sheet_name,
                    sheet_name=sheet_name,
                    status="failed",
                    message=str(exc),
                )
            )

    if not sheet_entries and not skipped_results:
        raise HTTPException(
            status_code=400,
            detail="Workbook does not contain any readable sheets.",
        )

    if not sheet_entries:
        skipped_failed = sum(1 for res in skipped_results if res.status == "failed")
        skipped_only = sum(1 for res in skipped_results if res.status == "skipped")
        return ArchiveAutoProcessResponse(
            success=skipped_failed == 0,
            total_files=len(skipped_results),
            processed_files=0,
            failed_files=skipped_failed,
            skipped_files=skipped_only,
            results=skipped_results,
            job_id=None,
        )

    remaining_sheets = [name for name, _ in sheet_entries]
    completed_seed = [{"archive_path": res.archive_path, "status": res.status} for res in skipped_results]

    job = create_import_job(
        file_id=file_id,
        trigger_source="workbook_auto_process",
        analysis_mode=analysis_mode.value,
        conflict_mode=conflict_resolution.value,
        metadata={
            "source": "auto-process-workbook",
            "files_in_archive": len(remaining_sheets) + len(skipped_results),
            "remaining_files": list(remaining_sheets),
            "completed_files": completed_seed,
            "forced_table_name": forced_table_name,
            "forced_table_mode": forced_table_mode,
            "sheet_names": normalized_selection,
        },
    )
    job_id = job["id"]
    update_file_status(file_id, "mapping", expected_active_job_id=job_id)

    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        lambda: _run_workbook_auto_process_job(
            file_id=file_id,
            workbook_name=workbook_name,
            sheet_entries=sheet_entries,
            skipped_results=skipped_results,
            analysis_mode=analysis_mode,
            conflict_resolution=conflict_resolution,
            auto_execute_confidence_threshold=auto_execute_confidence_threshold,
            max_iterations=max_iterations,
            job_id=job_id,
            forced_table_name=forced_table_name,
            forced_table_mode=forced_table_mode,
            llm_instruction=normalized_instruction,
        ),
    )

    return ArchiveAutoProcessResponse(
        success=True,
        total_files=len(sheet_entries) + len(skipped_results),
        processed_files=0,
        failed_files=0,
        skipped_files=len(skipped_results),
        results=skipped_results,
        job_id=job_id,
    )


@router.post("/analyze-b2-file", response_model=AnalyzeFileResponse)
async def analyze_storage_file_endpoint(
    request: AnalyzeB2FileRequest,
    db: Session = Depends(get_db)
):
    """
    Analyze a file from B2 storage and recommend import strategy using AI.
    
    This endpoint downloads a file from Backblaze B2, then uses Claude Sonnet
    to analyze its structure and recommend the best import strategy.
    """
    try:
        # Download file from B2
        file_content = _get_download_file_from_storage()(request.file_name)
        
        # Detect and process file
        file_type = detect_file_type(request.file_name)
        
        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        normalized_instruction, saved_instruction_id = _resolve_llm_instruction(
            llm_instruction=request.llm_instruction,
            llm_instruction_id=request.llm_instruction_id,
            save_llm_instruction=request.save_llm_instruction,
            llm_instruction_title=request.llm_instruction_title,
        )
        require_explicit_multi_value = bool(request.require_explicit_multi_value)
        
        # Smart sampling
        sample, total_rows = sample_file_data(records, request.sample_size, max_sample_size=50)
        
        # Prepare metadata
        file_metadata = {
            "name": request.file_name,
            "total_rows": total_rows,
            "file_type": file_type
        }
        
        # Run AI analysis
        analysis_result = _invoke_analyzer(
            _get_analyze_file_for_import(),
            file_sample=sample,
            file_metadata=file_metadata,
            analysis_mode=request.analysis_mode,
            conflict_mode=request.conflict_resolution,
            user_id=None,
            llm_instruction=normalized_instruction,
            max_iterations=request.max_iterations
        )
        
        if not analysis_result.get("success", False):
            error_message = analysis_result.get("error", "LLM analysis failed")
            raise HTTPException(status_code=502, detail=error_message)
        
        # Parse LLM response
        response = AnalyzeFileResponse(
            success=True,
            llm_response=analysis_result["response"],
            iterations_used=analysis_result["iterations_used"],
            max_iterations=request.max_iterations,
            can_auto_execute=False  # Will be set below based on analysis_mode
            ,
            llm_instruction_id=saved_instruction_id or request.llm_instruction_id
        )
        llm_decision = analysis_result.get("llm_decision")
        if llm_decision is not None:
            llm_decision = dict(llm_decision)
            if normalized_instruction:
                llm_decision.setdefault("llm_instruction", normalized_instruction)
            llm_decision["require_explicit_multi_value"] = require_explicit_multi_value
            response.llm_decision = llm_decision
        
        # Determine can_auto_execute based on analysis_mode
        if request.analysis_mode == AnalysisMode.AUTO_ALWAYS:
            response.can_auto_execute = True
        elif request.analysis_mode == AnalysisMode.AUTO_HIGH_CONFIDENCE:
            # Would need to parse confidence from LLM response
            response.can_auto_execute = False  # Conservative default
        else:  # MANUAL
            response.can_auto_execute = False
        
        # Store analysis result
        analysis_id = str(uuid.uuid4())
        analysis_storage[analysis_id] = response
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")


@router.post("/execute-recommended-import", response_model=MapDataResponse)
async def execute_recommended_import_endpoint(
    request: ExecuteRecommendedImportRequest,
    db: Session = Depends(get_db)
):
    """
    Execute a previously analyzed import recommendation.
    
    This endpoint takes an analysis ID from a previous /analyze-file or /analyze-b2-file
    call and executes the recommended import strategy. Users can optionally modify
    the suggested mapping before execution.
    """
    try:
        # Retrieve stored analysis
        if request.analysis_id not in analysis_storage:
            raise HTTPException(status_code=404, detail="Analysis not found. Please run /analyze-file first.")
        
        analysis = analysis_storage[request.analysis_id]
        
        if not analysis.success:
            raise HTTPException(status_code=400, detail="Cannot execute failed analysis")
        
        # Use user's confirmed mapping or the suggested one
        if request.confirmed_mapping:
            mapping = request.confirmed_mapping
        elif analysis.suggested_mapping:
            mapping = analysis.suggested_mapping
        else:
            raise HTTPException(
                status_code=400, 
                detail="No mapping available. Please provide confirmed_mapping in request."
            )
        
        # Check for conflicts
        if analysis.conflicts and not request.force_execute:
            raise HTTPException(
                status_code=409,
                detail=f"Analysis has {len(analysis.conflicts)} unresolved conflicts. "
                       "Set force_execute=true to proceed anyway."
            )
        
        # Execute import using existing pipeline
        # Note: This is a simplified version - in production, you'd need to:
        # 1. Retrieve the original file content
        # 2. Process it according to the mapping
        # 3. Insert into database
        
        raise HTTPException(
            status_code=501,
            detail="Import execution not yet implemented. This endpoint will execute "
                   "the recommended import strategy in the next phase."
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@router.post("/analyze-file-interactive")
async def analyze_file_interactive_endpoint(
    request: AnalyzeFileInteractiveRequest,
    db: Session = Depends(get_db)
):
    try:
        job_id: Optional[str] = None
        # Existing interactive session - continue conversation
        if request.thread_id:
            session = _get_interactive_session(request.thread_id)
            if session.file_id != request.file_id:
                raise HTTPException(status_code=400, detail="Thread does not belong to the requested file")
            if request.sheet_name and session.sheet_name and request.sheet_name != session.sheet_name:
                raise HTTPException(status_code=400, detail="This thread is tied to a different sheet")
            session.max_iterations = request.max_iterations
            if (
                request.llm_instruction is not None
                or request.llm_instruction_id
                or request.save_llm_instruction
            ):
                updated_instruction, saved_instruction_id = _resolve_llm_instruction(
                    llm_instruction=request.llm_instruction,
                    llm_instruction_id=request.llm_instruction_id,
                    save_llm_instruction=request.save_llm_instruction,
                    llm_instruction_title=request.llm_instruction_title,
                )
                session.llm_instruction = updated_instruction
                session.llm_instruction_id = (
                    saved_instruction_id or request.llm_instruction_id or session.llm_instruction_id
                )
            response = _run_interactive_session_step(
                session,
                user_message=request.user_message,
                conversation_role="user"
            )
            if session.job_id:
                update_import_job(
                    session.job_id,
                    status="ready_to_execute" if response.can_execute else "waiting_user",
                    stage="planning" if response.can_execute else "analysis",
                    error_message=session.last_error
                )
                response.job_id = session.job_id
            response.llm_instruction_id = session.llm_instruction_id
            return response

        if request.user_message:
            raise HTTPException(status_code=400, detail="Start the interactive analysis before sending messages")

        file_record = get_uploaded_file_by_id(request.file_id)
        if not file_record:
            raise HTTPException(status_code=404, detail=f"File {request.file_id} not found")

        job_id = file_record.get("active_job_id")
        if job_id:
            update_import_job(job_id, status="running", stage="analysis")
        else:
            try:
                job = create_import_job(
                    file_id=request.file_id,
                    trigger_source="interactive_manual",
                    analysis_mode="interactive",
                    conflict_mode=ConflictResolutionMode.ASK_USER.value,
                    metadata={"source": "analyze-file-interactive"}
                )
                job_id = job["id"]
            except Exception as job_exc:
                logger.warning("Unable to create interactive import job for %s: %s", request.file_id, job_exc)

        try:
            file_content = _get_download_file_from_storage()(file_record["b2_file_path"])
        except StorageConnectionError as storage_exc:
            # Handle network connectivity errors specifically
            error_msg = str(storage_exc)
            logger.error("Storage network error for interactive file %s: %s", request.file_id, error_msg)

            # Update file status to failed
            update_file_status(request.file_id, "failed", error_message=error_msg)

            # Fail the job if it was created
            if job_id:
                update_import_job(job_id, status="failed", error_message=error_msg)

            # Return 503 Service Unavailable for network errors
            raise HTTPException(status_code=503, detail=error_msg)

        file_type = detect_file_type(file_record["file_name"])
        target_sheet_name: Optional[str] = None
        if file_type == "excel":
            try:
                available_sheets = list_excel_sheets(file_content)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc))
            if request.sheet_name:
                lookup = {name.lower(): name for name in available_sheets}
                normalized = request.sheet_name.strip().lower()
                if normalized not in lookup:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Sheet '{request.sheet_name}' not found in workbook",
                    )
                target_sheet_name = lookup[normalized]
            elif available_sheets:
                target_sheet_name = available_sheets[0]

        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content, sheet_name=target_sheet_name)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        sample, total_rows = sample_file_data(records, None, max_sample_size=50)
        resolved_instruction, saved_instruction_id = _resolve_llm_instruction(
            llm_instruction=request.llm_instruction,
            llm_instruction_id=request.llm_instruction_id,
            save_llm_instruction=request.save_llm_instruction,
            llm_instruction_title=request.llm_instruction_title,
        )

        display_name = file_record["file_name"]
        if target_sheet_name:
            display_name = (
                f"{os.path.splitext(file_record['file_name'])[0]}__{target_sheet_name}.csv"
            )
        file_metadata = {
            "name": display_name,
            "total_rows": total_rows,
            "file_type": file_type
        }
        if target_sheet_name:
            file_metadata["sheet_name"] = target_sheet_name

        if request.target_table_name:
            file_metadata["forced_target_table"] = _normalize_forced_table_name(request.target_table_name)
        
        if request.target_table_mode:
            normalized_mode = request.target_table_mode.strip().lower()
            if normalized_mode in {"existing", "new"}:
                file_metadata["forced_target_table_mode"] = normalized_mode
            elif request.target_table_name:
                 # Default to existing if name provided but mode is missing/invalid
                 file_metadata["forced_target_table_mode"] = "existing"
        
        if request.target_table_name and "forced_target_table_mode" not in file_metadata:
             file_metadata["forced_target_table_mode"] = "existing"

        thread_id = str(uuid.uuid4())
        session = InteractiveSessionState(
            file_id=request.file_id,
            thread_id=thread_id,
            file_metadata=file_metadata,
            sample=sample,
            max_iterations=request.max_iterations,
            job_id=job_id,
            sheet_name=target_sheet_name,
            llm_instruction=resolved_instruction,
            llm_instruction_id=saved_instruction_id or request.llm_instruction_id,
            skip_file_duplicate_check=request.skip_file_duplicate_check,
        )

        if request.previous_error_message:
            cleaned_error = request.previous_error_message.strip()
            if cleaned_error:
                session.last_error = cleaned_error

        _store_interactive_session(session)

        response = _run_interactive_session_step(
            session,
            user_message=None
        )
        if job_id:
            update_import_job(
                job_id,
                status="ready_to_execute" if response.can_execute else "waiting_user",
                stage="planning" if response.can_execute else "analysis",
                error_message=session.last_error
            )
            response.job_id = job_id
        response.llm_instruction_id = session.llm_instruction_id
        return response

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Interactive analysis failed: {str(e)}")


@router.post("/execute-interactive-import")
async def execute_interactive_import_endpoint(
    request: ExecuteInteractiveImportRequest,
    db: Session = Depends(get_db)
):
    session: Optional[InteractiveSessionState] = None
    try:
        session = _get_interactive_session(request.thread_id)
        if session.file_id != request.file_id:
            raise HTTPException(status_code=400, detail="Thread does not belong to the requested file")

        if not session.llm_decision:
            raise HTTPException(
                status_code=400,
                detail="No confirmed import decision found. Ask the assistant to finalize the plan before executing."
            )

        file_record = get_uploaded_file_by_id(request.file_id)
        if not file_record:
            raise HTTPException(status_code=404, detail=f"File {request.file_id} not found")

        job_id = session.job_id or file_record.get("active_job_id")
        if job_id:
            update_import_job(job_id, status="running", stage="execution")

        file_content = _get_download_file_from_storage()(file_record["b2_file_path"])
        file_type = detect_file_type(file_record["file_name"])
        file_name_for_import = file_record["file_name"]

        if file_type == "excel" and session.sheet_name:
            sheet_file_name = f"{os.path.splitext(file_record['file_name'])[0]}__{session.sheet_name}.csv"
            sheet_file_name = sheet_file_name.replace("/", "_").replace("\\", "_")
            file_name_for_import = sheet_file_name
            file_content = extract_excel_sheet_csv_bytes(file_content, session.sheet_name)
            file_type = "csv"

        if file_type == 'csv':
            records = process_csv(file_content)
        elif file_type == 'excel':
            records = process_excel(file_content, sheet_name=session.sheet_name)
        elif file_type == 'json':
            records = process_json(file_content)
        elif file_type == 'xml':
            records = process_xml(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")

        update_file_status(
            request.file_id,
            "mapping",
            expected_active_job_id=job_id if job_id else None
        )

        try:
            execution_result = _get_execute_llm_import_decision()(
                file_content=file_content,
                file_name=file_name_for_import,
                all_records=records,
                llm_decision=session.llm_decision,
                source_path=file_record.get("b2_file_path")
            )
        except Exception as exc:
            error_text = str(exc)
            if job_id:
                update_file_status(
                    request.file_id,
                    "mapping",
                    error_message=error_text,
                    expected_active_job_id=job_id
                )
            else:
                update_file_status(request.file_id, "failed", error_message=error_text)
            return _handle_interactive_execution_failure(session, error_text)

        if execution_result["success"]:
            if job_id:
                complete_import_job(
                    job_id,
                    success=True,
                    result_metadata={
                        "table_name": execution_result["table_name"],
                        "records_processed": execution_result["records_processed"]
                    },
                    mapped_table_name=execution_result["table_name"],
                    mapped_rows=execution_result["records_processed"],
                    data_validation_errors=execution_result.get("validation_errors", 0),
                    duplicates_found=execution_result.get("duplicates_skipped"),
                    mapping_errors=len(execution_result.get("mapping_errors", []))
                )
                job_id = None
            else:
                update_file_status(
                    request.file_id,
                    "mapped",
                    mapped_table_name=execution_result["table_name"],
                    mapped_rows=execution_result["records_processed"],
                    duplicates_found=execution_result.get("duplicates_skipped"),
                    data_validation_errors=execution_result.get("validation_errors", 0),
                    mapping_errors=len(execution_result.get("mapping_errors", [])),
                )

            session.status = "completed"
            interactive_sessions.pop(request.thread_id, None)

            duplicates_skipped = execution_result.get("duplicates_skipped", 0) or 0
            duplicate_rows = execution_result.get("duplicate_rows")
            duplicate_rows_count = execution_result.get("duplicate_rows_count")
            import_id_value = execution_result.get("import_id")

            if import_id_value and ((duplicate_rows_count or 0) == 0 or not duplicate_rows):
                try:
                    history_records = get_import_history(import_id=import_id_value, limit=1)
                    if history_records:
                        history_record = history_records[0]
                        history_duplicates = history_record.get("duplicates_found") or 0
                        if history_duplicates:
                            duplicates_skipped = history_duplicates
                            duplicate_rows_count = history_duplicates
                            duplicate_rows = list_duplicate_rows(
                                import_id_value,
                                limit=history_duplicates,
                                include_existing_row=True
                            )
                except Exception as audit_err:
                    logger.warning(
                        "Unable to backfill duplicate metadata for import %s: %s",
                        import_id_value,
                        audit_err,
                    )

            success_response = MapDataResponse(
                success=True,
                message="Import executed successfully",
                records_processed=execution_result["records_processed"],
                duplicates_skipped=duplicates_skipped,
                duplicate_rows=duplicate_rows,
                duplicate_rows_count=duplicate_rows_count,
                import_id=import_id_value,
                llm_followup=execution_result.get("llm_followup"),
                table_name=execution_result["table_name"]
            )
            if job_id:
                success_response.job_id = job_id
            return success_response

        error_msg = execution_result.get('error', 'Unknown error')
        if job_id:
            update_file_status(
                request.file_id,
                "mapping",
                error_message=error_msg,
                expected_active_job_id=job_id
            )
        else:
            update_file_status(request.file_id, "failed", error_message=error_msg)
        return _handle_interactive_execution_failure(session, error_msg)

    except HTTPException:
        raise
    except Exception as e:
        if session is not None:
            return _handle_interactive_execution_failure(session, str(e))
        raise HTTPException(status_code=500, detail=f"Import execution failed: {str(e)}")
