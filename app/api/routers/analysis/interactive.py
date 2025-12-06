"""
Interactive session management for file analysis.

This module handles the conversational, back-and-forth mapping workflow
where users can collaborate with the LLM to refine import strategies.
"""
import logging
import uuid
from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.api.schemas.shared import (
    AnalysisMode,
    ConflictResolutionMode,
    AnalyzeFileInteractiveResponse,
    MapDataResponse,
)
from app.api.dependencies import interactive_sessions
from app.domain.imports.jobs import update_import_job

logger = logging.getLogger(__name__)


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

    def metadata_copy(self) -> Dict[str, Any]:
        """Return a safe copy of file metadata for the agent to mutate."""
        return dict(self.file_metadata)


def store_interactive_session(session: InteractiveSessionState) -> None:
    """Persist interactive session state in the in-memory cache."""
    interactive_sessions[session.thread_id] = session


def get_interactive_session(thread_id: str) -> InteractiveSessionState:
    """Retrieve an interactive session or raise if missing."""
    session = interactive_sessions.get(thread_id)
    if not isinstance(session, InteractiveSessionState):
        raise HTTPException(status_code=404, detail="Interactive session not found or expired")
    return session


def build_interactive_initial_prompt(session: InteractiveSessionState) -> str:
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


def _is_user_confirmation(message: str) -> bool:
    """Check if user message is an explicit confirmation to proceed with import."""
    if not message:
        return False
    normalized = message.lower().strip()
    confirmation_phrases = ["confirm import", "confirm", "approve", "proceed", "execute", "yes", "go ahead"]
    return any(phrase in normalized for phrase in confirmation_phrases)


def run_interactive_session_step(
    session: InteractiveSessionState,
    *,
    user_message: Optional[str],
    conversation_role: str = "user",
    analyze_fn,
) -> AnalyzeFileInteractiveResponse:
    """Execute a single interactive LLM turn and update session state."""
    session.max_iterations = max(1, session.max_iterations)
    messages: List[Dict[str, str]] = []

    if not session.initial_prompt_sent:
        initial_prompt = build_interactive_initial_prompt(session)
        messages.append({"role": "user", "content": initial_prompt})
        session.conversation.append({"role": "user", "content": initial_prompt})
        session.initial_prompt_sent = True

    # Check if user is explicitly confirming the mapping
    is_confirmation = user_message and _is_user_confirmation(user_message)

    if user_message is not None:
        normalized = user_message.strip()
        if not normalized:
            raise HTTPException(status_code=400, detail="user_message cannot be empty")
        messages.append({"role": "user", "content": normalized})
        session.conversation.append({"role": conversation_role, "content": normalized})
    elif session.initial_prompt_sent and not messages:
        raise HTTPException(status_code=400, detail="user_message required for ongoing interactive session")

    # Import here to avoid circular dependency
    from app.api.routers.analysis.utils import invoke_analyzer
    
    analysis_result = invoke_analyzer(
        analyze_fn,
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
    
    # If user explicitly confirmed but LLM didn't call make_import_decision,
    # enable execution anyway - user has final say
    if is_confirmation and session.llm_decision is None:
        # User wants to proceed, so mark as ready to execute
        # The actual decision will be validated during execution
        session.status = "ready_to_execute"
        can_execute = True
    else:
        session.status = "ready_to_execute" if session.llm_decision else "awaiting_user"
        can_execute = bool(session.llm_decision)
    
    session.last_error = None if session.llm_decision else session.last_error

    return AnalyzeFileInteractiveResponse(
        success=True,
        thread_id=session.thread_id,
        llm_message=llm_response,
        needs_user_input=not can_execute,
        question=None,
        options=None,
        can_execute=can_execute,
        llm_decision=session.llm_decision,
        iterations_used=analysis_result["iterations_used"],
        max_iterations=session.max_iterations,
        llm_instruction_id=session.llm_instruction_id,
    )


def handle_interactive_execution_failure(
    session: InteractiveSessionState,
    error_message: str,
    analyze_fn,
) -> MapDataResponse:
    """Feed execution failure context back into the conversation."""
    session.last_error = error_message
    session.llm_decision = None
    failure_prompt = (
        "EXECUTION_FAILED\n"
        f"Error details: {error_message}\n"
        "Please analyze why this import failed and propose concrete fixes. "
        "Wait for user approval before finalizing a new mapping."
    )

    followup = run_interactive_session_step(
        session,
        user_message=failure_prompt,
        conversation_role="assistant",
        analyze_fn=analyze_fn,
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
