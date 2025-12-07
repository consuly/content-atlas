"""
Analysis router package.

This package provides AI-powered file analysis endpoints for intelligent import recommendations.
The router has been refactored into focused modules for better maintainability.
"""
# Main router and endpoints
from app.api.routers.analysis.routes import (
    router,
    analyze_file_endpoint,
    _preloaded_file_contents,
)

# Execution and retry logic
from app.api.routers.analysis.execution import (
    auto_retry_failed_auto_import,
    summarize_archive_execution,
    get_analyze_file_for_import as _get_analyze_file_for_import,
    get_download_file_from_b2 as _get_download_file_from_b2,
    get_execute_llm_import_decision as _get_execute_llm_import_decision,
)

# Interactive session management
from app.api.routers.analysis.interactive import (
    InteractiveSessionState,
    store_interactive_session as _store_interactive_session,
    get_interactive_session as _get_interactive_session,
    build_interactive_initial_prompt as _build_interactive_initial_prompt,
    run_interactive_session_step as _run_interactive_session_step,
    handle_interactive_execution_failure as _handle_interactive_execution_failure,
)

# Utility functions
from app.api.routers.analysis.utils import (
    normalize_forced_table_name as _normalize_forced_table_name,
    resolve_llm_instruction as _resolve_llm_instruction,
    invoke_analyzer as _invoke_analyzer,
    apply_forced_table_decision as _apply_forced_table_decision,
    log_archive_debug as _log_archive_debug,
    log_mapping_failure as _log_mapping_failure,
    guess_content_type as _guess_content_type,
    build_archive_entry_name as _build_archive_entry_name,
    normalize_columns as _normalize_columns,
    build_structure_fingerprint as _build_structure_fingerprint,
    parse_sheet_names_param as _parse_sheet_names_param,
)

# Re-export domain functions for backward compatibility with tests
from app.domain.uploads.uploaded_files import (
    insert_uploaded_file,
    get_uploaded_file_by_id,
    update_file_status,
)
from app.domain.imports.jobs import (
    create_import_job,
    update_import_job,
    complete_import_job,
    get_import_job,
)
from app.db.session import get_session_local

__all__ = [
    # Router
    "router",
    "analyze_file_endpoint",
    "_preloaded_file_contents",
    # Execution
    "auto_retry_failed_auto_import",
    "summarize_archive_execution",
    "_get_analyze_file_for_import",
    "_get_download_file_from_b2",
    "_get_execute_llm_import_decision",
    # Interactive
    "InteractiveSessionState",
    "_store_interactive_session",
    "_get_interactive_session",
    "_build_interactive_initial_prompt",
    "_run_interactive_session_step",
    "_handle_interactive_execution_failure",
    # Utils
    "_normalize_forced_table_name",
    "_resolve_llm_instruction",
    "_invoke_analyzer",
    "_apply_forced_table_decision",
    "_log_archive_debug",
    "_log_mapping_failure",
    "_guess_content_type",
    "_build_archive_entry_name",
    "_normalize_columns",
    "_build_structure_fingerprint",
    "_parse_sheet_names_param",
    # Domain functions (for test compatibility)
    "insert_uploaded_file",
    "get_uploaded_file_by_id",
    "update_file_status",
    "create_import_job",
    "update_import_job",
    "complete_import_job",
    "get_import_job",
    "get_session_local",
]
