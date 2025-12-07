"""
Auto-execution and retry logic for file imports.

This module handles:
- Automatic execution of LLM-recommended import strategies
- Auto-retry mechanism using interactive assistant for failed imports
- Execution result summarization
"""
import logging
from typing import Optional, Any, Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.api.schemas.shared import (
    AnalyzeFileResponse,
    AnalyzeFileInteractiveRequest,
    ExecuteInteractiveImportRequest,
    MapDataResponse,
    AutoExecutionResult,
)
from app.api.dependencies import interactive_sessions
from app.core.config import settings

logger = logging.getLogger(__name__)


def summarize_archive_execution(response: AnalyzeFileResponse) -> Dict[str, Any]:
    """
    Summarize execution results for archive/workbook processing.
    
    Returns a dict with status, table_name, records_processed, etc.
    """
    auto_result = response.auto_execution_result
    retry_result = response.auto_retry_execution_result

    if retry_result and retry_result.success:
        return {
            "status": "processed",
            "table_name": retry_result.table_name,
            "records_processed": retry_result.records_processed,
            "duplicates_skipped": retry_result.duplicates_skipped,
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
    }


async def auto_retry_failed_auto_import(
    *,
    file_id: Optional[str],
    previous_error_message: str,
    max_iterations: int,
    db: Session,
    llm_instruction: Optional[str] = None,
    analyze_file_interactive_fn,
    execute_interactive_import_fn,
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
        interactive_response = await analyze_file_interactive_fn(
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

        execute_response = await execute_interactive_import_fn(
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


def get_analyze_file_for_import():
    """Return the analyze_file_for_import callable."""
    from app.domain.queries.analyzer import analyze_file_for_import
    return analyze_file_for_import


def get_download_file_from_storage():
    """Return the download_file callable."""
    from app.integrations.storage import download_file
    return download_file


def get_execute_llm_import_decision():
    """Return the execute_llm_import_decision callable."""
    from app.integrations.auto_import import execute_llm_import_decision
    return execute_llm_import_decision
