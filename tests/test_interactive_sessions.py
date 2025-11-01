import uuid
from typing import List, Dict, Any

import pytest

from app.api.routers import analysis as analysis_module
from app.api.routers.analysis import (
    InteractiveSessionState,
    _run_interactive_session_step,
    _handle_interactive_execution_failure,
)
from app.api.schemas.shared import AnalyzeFileInteractiveResponse


@pytest.fixture(scope="session", autouse=True)
def initialize_test_database():
    """Override the global autouse fixture so these unit tests avoid touching Postgres."""
    yield


def _make_session(sample: List[Dict[str, Any]] = None) -> InteractiveSessionState:
    """Utility to build a minimal interactive session for tests."""
    return InteractiveSessionState(
        file_id="file-123",
        thread_id=str(uuid.uuid4()),
        file_metadata={"name": "demo.csv", "total_rows": 5, "file_type": "csv"},
        sample=sample or [{"col_a": "value"}],
        max_iterations=3,
    )


def test_run_interactive_session_initial_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: Dict[str, Any] = {}

    def fake_analyze(
        *,
        file_sample,
        file_metadata,
        analysis_mode,
        conflict_mode,
        user_id,
        max_iterations,
        thread_id,
        messages,
        interactive_mode
    ):
        recorded["messages"] = messages
        recorded["interactive_mode"] = interactive_mode
        recorded["file_sample"] = file_sample
        return {
            "success": True,
            "response": "Here is an initial plan with follow-up steps.",
            "iterations_used": 1,
            "llm_decision": None,
        }

    monkeypatch.setattr(analysis_module, "_get_analyze_file_for_import", lambda: fake_analyze)

    session = _make_session()
    response = _run_interactive_session_step(session, user_message=None)

    assert response.success is True
    assert response.needs_user_input is True
    assert response.can_execute is False
    assert session.initial_prompt_sent is True
    assert session.status == "awaiting_user"
    assert session.llm_decision is None
    assert len(session.conversation) == 2  # initial prompt + assistant reply
    assert session.conversation[0]["role"] == "user"
    assert "interactive import" in session.conversation[0]["content"]
    assert recorded["interactive_mode"] is True
    assert recorded["messages"][0]["role"] == "user"
    assert "interactive import" in recorded["messages"][0]["content"]
    assert recorded["file_sample"] == session.sample


def test_run_interactive_session_confirms_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}

    def fake_analyze(
        *,
        file_sample,
        file_metadata,
        analysis_mode,
        conflict_mode,
        user_id,
        max_iterations,
        thread_id,
        messages,
        interactive_mode
    ):
        captured["messages"] = messages
        return {
            "success": True,
            "response": "Plan confirmed and ready to execute.",
            "iterations_used": 2,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "demo_table",
                "column_mapping": {"col_a": "name"},
            },
        }

    monkeypatch.setattr(analysis_module, "_get_analyze_file_for_import", lambda: fake_analyze)

    session = _make_session()
    session.initial_prompt_sent = True
    session.conversation.append({"role": "user", "content": "Initial prompt already sent."})

    response = _run_interactive_session_step(session, user_message="CONFIRM IMPORT")

    assert response.success is True
    assert response.can_execute is True
    assert response.needs_user_input is False
    assert session.llm_decision is not None
    assert session.llm_decision["target_table"] == "demo_table"
    assert session.status == "ready_to_execute"
    assert captured["messages"][0]["content"] == "CONFIRM IMPORT"


def test_handle_interactive_execution_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    followup = AnalyzeFileInteractiveResponse(
        success=True,
        thread_id="thread-xyz",
        llm_message="Detected duplicate key violation. Suggest changing unique columns.",
        needs_user_input=True,
        question=None,
        options=None,
        can_execute=False,
        llm_decision=None,
        iterations_used=1,
        max_iterations=5,
    )

    def fake_run_step(session, user_message=None, conversation_role="user"):
        # The failure prompt should be delivered from the backend.
        assert user_message and user_message.startswith("EXECUTION_FAILED")
        # Maintain conversation continuity so the session can resume.
        session.conversation.append({"role": "assistant", "content": followup.llm_message})
        session.llm_decision = None
        session.status = "awaiting_user"
        return followup

    monkeypatch.setattr(analysis_module, "_run_interactive_session_step", fake_run_step)

    session = _make_session()
    session.llm_decision = {
        "strategy": "NEW_TABLE",
        "target_table": "demo_table",
        "column_mapping": {"col_a": "name"},
    }

    result = _handle_interactive_execution_failure(session, "duplicate key value violates constraint")

    assert result.success is False
    assert "duplicate key value violates constraint" in result.message
    assert result.llm_followup == followup.llm_message
    assert result.needs_user_input is True
    assert result.can_execute is False
    assert result.thread_id == session.thread_id
    assert session.last_error == "duplicate key value violates constraint"
    assert session.llm_decision is None
