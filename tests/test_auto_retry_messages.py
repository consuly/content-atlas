from types import SimpleNamespace

import pytest

from app.api.routers.analysis.execution import (
    auto_retry_failed_auto_import,
    summarize_archive_execution,
)
from app.api.schemas.shared import AnalyzeFileResponse


@pytest.mark.asyncio
async def test_auto_retry_failed_import_requires_user_input_message(monkeypatch):
    """
    When the interactive flow returns without an executable plan, the auto-retry
    helper should surface a clear, actionable error that preserves the last error.
    """

    async def fake_analyze_file_interactive_endpoint(*_args, **_kwargs):
        return SimpleNamespace(
            success=True,
            can_execute=False,
            llm_decision=None,
            thread_id="thread-123",
        )

    async def fake_execute_interactive_import_endpoint(*_args, **_kwargs):
        raise AssertionError("Execute should not be called when can_execute is False")

    # Test the case where interactive analysis succeeds but cannot execute
    result = await auto_retry_failed_auto_import(
        file_id="file-123",
        previous_error_message="upstream parse failed",
        max_iterations=3,
        db=None,  # db is unused in this failure path
        analyze_file_interactive_fn=fake_analyze_file_interactive_endpoint,
        execute_interactive_import_fn=fake_execute_interactive_import_endpoint,
    )

    assert result["success"] is False
    assert "Open the Interactive tab" in result["error"]
    assert "Last error: upstream parse failed" in result["error"]


def test_summarize_archive_execution_includes_interactive_hint():
    """
    Archive summaries should include an explicit hint when auto-retry pauses
    awaiting user input.
    """
    response = AnalyzeFileResponse(
        success=False,
        llm_response="",
        iterations_used=1,
        max_iterations=1,
        auto_execution_error="Base failure",
        auto_retry_attempted=True,
        auto_retry_error="Interactive assistant requires user input before executing the retry plan.",
    )

    summary = summarize_archive_execution(response)
    assert summary["status"] == "failed"
    assert "Auto-retry paused" in summary["message"]
    assert "Try Again" in summary["message"]


def test_summarize_archive_execution_does_not_over_hint_for_other_errors():
    """
    Non-interactive retry failures should not show the interactive hint.
    """
    response = AnalyzeFileResponse(
        success=False,
        llm_response="",
        iterations_used=1,
        max_iterations=1,
        auto_execution_error="Base failure",
        auto_retry_attempted=True,
        auto_retry_error="Timeout retrying execution",
    )

    summary = summarize_archive_execution(response)
    assert "Auto-retry paused" not in summary["message"]
    assert "Try Again: Timeout retrying execution" in summary["message"]


@pytest.mark.asyncio
async def test_auto_retry_execute_path_surfaces_execution_error(monkeypatch):
    """
    If the interactive execute endpoint fails, the error should be returned to callers.
    """

    async def fake_interactive(*_args, **_kwargs):
        return SimpleNamespace(
            success=True,
            can_execute=True,
            llm_decision={"strategy": "NEW_TABLE"},
            thread_id="thread-123",
        )

    async def fake_execute(*_args, **_kwargs):
        return SimpleNamespace(success=False, message="execution failed hard")

    result = await auto_retry_failed_auto_import(
        file_id="file-123",
        previous_error_message="base fail",
        max_iterations=3,
        db=None,
        analyze_file_interactive_fn=fake_interactive,
        execute_interactive_import_fn=fake_execute,
    )

    assert result["success"] is False
    assert result["error"] == "execution failed hard"
    assert result["execution_response"].message == "execution failed hard"


@pytest.mark.asyncio
async def test_auto_retry_execute_path_success(monkeypatch):
    """
    Successful execute path should propagate success and include execution payload.
    """

    async def fake_interactive(*_args, **_kwargs):
        return SimpleNamespace(
            success=True,
            can_execute=True,
            llm_decision={"strategy": "NEW_TABLE"},
            thread_id="thread-456",
        )

    async def fake_execute(*_args, **_kwargs):
        return SimpleNamespace(
            success=True,
            message=None,
            table_name="table_xyz",
            records_processed=5,
        )

    result = await auto_retry_failed_auto_import(
        file_id="file-456",
        previous_error_message="",
        max_iterations=3,
        db=None,
        analyze_file_interactive_fn=fake_interactive,
        execute_interactive_import_fn=fake_execute,
    )

    assert result["success"] is True
    assert result["error"] is None
    assert result["execution_response"].table_name == "table_xyz"
