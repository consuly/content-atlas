"""
Pytest configuration and fixtures for Content Atlas tests.

This module provides shared fixtures and setup for all tests, including
database initialization to ensure system tables exist before tests run.
"""

import pytest
from app.database import get_engine
from app.table_metadata import create_table_metadata_table
from app.import_history import create_import_history_table
from app.uploaded_files import create_uploaded_files_table
from app.models import create_file_imports_table_if_not_exists


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
    
    try:
        engine = get_engine()
        
        # Create all system tables
        print("  Creating table_metadata table...")
        create_table_metadata_table()
        
        print("  Creating import_history table...")
        create_import_history_table()
        
        print("  Creating uploaded_files table...")
        create_uploaded_files_table()
        
        print("  Creating file_imports table...")
        create_file_imports_table_if_not_exists(engine)
        
        print("  ✓ All system tables initialized successfully")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"  ERROR: Failed to initialize system tables: {e}")
        print("="*80 + "\n")
        raise
    
    # Yield control to tests
    yield
    
    # Teardown (if needed) would go here
    # For now, we leave tables in place for inspection after tests
