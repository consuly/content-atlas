"""
Utilities for ensuring system tables exist before destructive cleanup logic
in integration-style tests.
"""

from typing import Optional

from sqlalchemy.engine import Engine

from app.db.metadata import create_table_metadata_table
from app.domain.imports.history import create_import_history_table
from app.domain.imports.jobs import ensure_import_jobs_table
from app.domain.uploads.uploaded_files import create_uploaded_files_table
from app.db.models import create_file_imports_table_if_not_exists
from app.db.session import get_engine
from app.domain.workflows.models import create_workflow_tables


def ensure_system_tables_ready(engine: Optional[Engine] = None) -> Engine:
    """
    Create key system tables if they were dropped during a previous test run.

    Returns:
        Engine: The SQLAlchemy engine that was verified/used.
    """
    engine = engine or get_engine()

    # Each helper is idempotent, so calling on every cleanup keeps tests stable
    create_table_metadata_table()
    create_import_history_table()
    create_uploaded_files_table()
    ensure_import_jobs_table()
    create_file_imports_table_if_not_exists(engine)
    create_workflow_tables()

    return engine
