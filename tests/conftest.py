"""
Pytest configuration and fixtures for Content Atlas tests.

This module provides shared fixtures and setup for all tests, including
database initialization to ensure system tables exist before tests run.
"""

import os

# Force database bootstrap unless the user explicitly exports SKIP_DB_INIT=1.
os.environ.setdefault("SKIP_DB_INIT", "0")

import pytest
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from app.db.session import get_engine
from app.db.metadata import create_table_metadata_table
from app.domain.imports.history import create_import_history_table
from app.domain.uploads.uploaded_files import create_uploaded_files_table
from app.db.models import create_file_imports_table_if_not_exists, create_table_fingerprints_table_if_not_exists
from app.domain.imports.jobs import ensure_import_jobs_table
from app.domain.queries.history import create_query_history_tables
from app.db.llm_instructions import create_llm_instruction_table
from app.core.security import (
    init_auth_tables,
    create_user,
    delete_user,
    create_access_token,
)
from app.db.organization import init_organization_tables, create_organization


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

        print("  Creating table_fingerprints table...")
        create_table_fingerprints_table_if_not_exists(engine)

        print("  Creating auth and organization tables...")
        init_auth_tables()
        init_organization_tables()

        print("  ✓ All system tables initialized successfully")
        print("="*80 + "\n")
        
    except OperationalError as exc:
        print(f"  WARNING: Database unavailable, skipping system table init: {exc}")
        import traceback
        traceback.print_exc()
        print("="*80 + "\n")
        yield
        return
    except Exception as e:
        print(f"  ERROR: Failed to initialize system tables: {e}")
        import traceback
        traceback.print_exc()
        print("="*80 + "\n")
        raise
    
    # Yield control to tests
    yield
    
    # Teardown (if needed) would go here
    # For now, we leave tables in place for inspection after tests


@pytest.fixture(scope="function")
def test_engine(initialize_test_database):
    """
    Provide the database engine for tests.
    
    This fixture depends on initialize_test_database to ensure
    all system tables exist before any tests run.
    
    Scope: function (each test gets a fresh reference)
    """
    return get_engine()


@pytest.fixture(scope="function")
def auth_headers(initialize_test_database):
    """
    Create a test user with organization and return auth headers.
    
    This fixture provides authenticated request headers for tests that need
    to call endpoints requiring authentication (e.g., /tables, /map-data).
    
    Returns:
        dict: Authorization headers with Bearer token
    """
    engine = get_engine()
    session = Session(engine)
    user_id = None
    org_id = None
    
    try:
        # Initialize auth and organization tables
        init_auth_tables()
        init_organization_tables()
        
        # Create test organization
        org_name = f"Test Org {uuid.uuid4().hex[:8]}"
        org = create_organization(db=session, name=org_name)
        org_id = org.id
        
        # Create test user with organization
        email = f"test_{uuid.uuid4().hex}@example.com"
        user = create_user(
            db=session,
            email=email,
            password="TestPass123!",
            full_name="Test User",
            role="user"
        )
        user_id = user.id
        
        # Assign organization to user
        user.organization_id = org_id
        session.commit()
        session.refresh(user)
        
        # Generate JWT token
        token = create_access_token({"sub": user.email})
        
        headers = {"Authorization": f"Bearer {token}"}
        
        yield headers
        
    except Exception as e:
        # If database is not available, skip the test
        pytest.skip(f"Database not available for authentication: {e}")
    finally:
        # Cleanup: delete user and organization
        try:
            if user_id:
                delete_user(session, user_id)
            if org_id:
                from sqlalchemy import text
                session.execute(text("DELETE FROM organizations WHERE id = :org_id"), {"org_id": org_id})
                session.commit()
        except Exception as e:
            print(f"Warning: Cleanup failed in auth_headers fixture: {e}")
        finally:
            session.close()
