"""
Pytest configuration and fixtures for Content Atlas tests.

This module provides shared fixtures and setup for all tests, including
database initialization to ensure system tables exist before tests run.
"""

import os

# Force database bootstrap unless the user explicitly exports SKIP_DB_INIT=1.
os.environ.setdefault("SKIP_DB_INIT", "0")

import pytest
from sqlalchemy.exc import OperationalError
from app.db.session import get_engine
from app.db.metadata import create_table_metadata_table
from app.domain.imports.history import create_import_history_table
from app.domain.uploads.uploaded_files import create_uploaded_files_table
from app.db.models import create_file_imports_table_if_not_exists
from app.domain.imports.jobs import ensure_import_jobs_table
from app.domain.queries.history import create_query_history_tables
from app.db.llm_instructions import create_llm_instruction_table


@pytest.fixture(scope="session", autouse=True)
def initialize_test_database():
    """
    Initialize system tables before running tests.
    
    This fixture runs automatically once per test session and ensures that
    all required system tables exist in the database before any tests execute.
    
    System tables created:
    - table_metadata: Tracks metadata about user data tables
    - import_history: Tracks all import operations and their details
    - mapping_errors: Tracks errors during data mapping
    - uploaded_files: Tracks uploaded files (if using file storage)
    - file_imports: Tracks file-level duplicate detection
    
    Scope: session (runs once for all tests)
    Autouse: True (runs automatically without explicit reference)
    """
    print("\n" + "="*80)
    print("PYTEST SETUP: Initializing system tables for test session")
    print("="*80)
    
    if os.getenv("SKIP_DB_INIT") == "1":
        print("  SKIP_DB_INIT=1 detected; skipping database bootstrap for tests")
        print("="*80 + "\n")
        yield
        return
    
    try:
        if os.getenv("SKIP_DB_INIT") == "1":
            print("  SKIP_DB_INIT=1 detected; skipping database bootstrap for fast unit test run")
            print("="*80 + "\n")
            yield
            return

        engine = get_engine()
        
        # Create all system tables
        print("  Creating table_metadata table...")
        create_table_metadata_table()
        
        print("  Creating import_history table...")
        create_import_history_table()
        
        print("  Creating uploaded_files table...")
        create_uploaded_files_table()

        print("  Creating import_jobs table...")
        ensure_import_jobs_table()

        print("  Creating query conversation tables...")
        create_query_history_tables()

        print("  Creating llm_instructions table...")
        create_llm_instruction_table()

        print("  Creating file_imports table...")
        create_file_imports_table_if_not_exists(engine)
        
        print("  ✓ All system tables initialized successfully")
        print("="*80 + "\n")
        
    except OperationalError as exc:
        print(f"  WARNING: Database unavailable, skipping system table init: {exc}")
        print("="*80 + "\n")
        yield
        return
    except Exception as e:
        print(f"  ERROR: Failed to initialize system tables: {e}")
        print("="*80 + "\n")
        raise
    
    # Yield control to tests
    yield
    
    # Teardown (if needed) would go here
    # For now, we leave tables in place for inspection after tests
