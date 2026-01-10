"""
Tests for temporary tables feature.

This test suite verifies:
- Marking tables as temporary
- Listing temporary tables
- Converting temporary to permanent
- Extending expiration
- Cleanup of expired tables
- Import protection for temporary tables
- API endpoints
"""
import pytest
from datetime import datetime, timedelta
from sqlalchemy import text
from app.db.session import get_engine
from app.db.temporary_tables import (
    mark_table_as_temporary,
    is_temporary_table,
    get_temporary_table_info,
    list_temporary_tables,
    unmark_temporary_table,
    extend_temporary_table_expiration,
    cleanup_expired_temporary_tables,
    allows_additional_imports,
    create_temporary_tables_tracking_table_if_not_exists,
)


@pytest.fixture(scope="function")
def test_organization(test_engine):
    """Create a test organization for temporary table operations."""
    from sqlalchemy import text
    import uuid
    
    org_id = None
    org_slug = f"test-org-{uuid.uuid4().hex[:8]}"
    
    with test_engine.begin() as conn:
        # Create test organization with slug
        result = conn.execute(text("""
            INSERT INTO organizations (name, slug, created_at)
            VALUES ('Test Org for Temporary Tables', :slug, NOW())
            RETURNING id
        """), {"slug": org_slug})
        org_id = result.scalar()
    
    yield org_id
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM organizations WHERE id = :org_id
        """), {"org_id": org_id})


@pytest.fixture(scope="function")
def test_table(test_engine):
    """Create a test table for temporary table operations."""
    table_name = "temp_table_for_testing"
    
    with test_engine.begin() as conn:
        # Drop if exists
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        
        # Create test table with all system columns used by import tracking
        conn.execute(text(f"""
            CREATE TABLE "{table_name}" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                value INTEGER,
                _import_id VARCHAR(255),
                _source_row_number INTEGER,
                _corrections_applied TEXT,
                _organization_id INTEGER
            )
        """))
        
        # Insert some test data
        conn.execute(text(f"""
            INSERT INTO "{table_name}" (name, value)
            VALUES ('test1', 100), ('test2', 200)
        """))
    
    yield table_name
    
    # Cleanup
    with test_engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
        conn.execute(text("""
            DELETE FROM temporary_tables WHERE table_name = :table_name
        """), {"table_name": table_name})


@pytest.fixture(scope="function", autouse=True)
def setup_temporary_tables_table(test_engine):
    """Ensure temporary_tables tracking table exists."""
    create_temporary_tables_tracking_table_if_not_exists(test_engine)


def test_mark_table_as_temporary(test_engine, test_table, test_organization):
    """Test marking a table as temporary."""
    result = mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        purpose="Testing temporary table",
        engine=test_engine
    )
    
    assert result is True
    assert is_temporary_table(test_table, engine=test_engine) is True


def test_mark_nonexistent_table_fails(test_engine, test_organization):
    """Test that marking a non-existent table fails."""
    result = mark_table_as_temporary(
        table_name="nonexistent_table_xyz",
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    assert result is False


def test_get_temporary_table_info(test_engine, test_table, test_organization):
    """Test retrieving information about a temporary table."""
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        purpose="Test purpose",
        engine=test_engine
    )
    
    # Get info
    info = get_temporary_table_info(test_table, engine=test_engine)
    
    assert info is not None
    assert info["table_name"] == test_table
    assert info["organization_id"] == test_organization
    assert info["purpose"] == "Test purpose"
    assert info["allow_additional_imports"] is False
    assert info["expires_at"] is not None


def test_list_temporary_tables(test_engine, test_table, test_organization):
    """Test listing temporary tables."""
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # List all temporary tables
    tables = list_temporary_tables(engine=test_engine)
    
    assert len(tables) >= 1
    assert any(t["table_name"] == test_table for t in tables)
    
    # List for specific organization
    org_tables = list_temporary_tables(organization_id=test_organization, engine=test_engine)
    
    assert len(org_tables) >= 1
    assert any(t["table_name"] == test_table for t in org_tables)


def test_list_temporary_tables_filters_expired(test_engine, test_table, test_organization):
    """Test that list_temporary_tables filters expired tables by default."""
    # Mark as temporary with negative expiration (already expired)
    with test_engine.begin() as conn:
        expires_at = datetime.now() - timedelta(days=1)
        conn.execute(text("""
            INSERT INTO temporary_tables 
                (table_name, expires_at, organization_id)
            VALUES (:table_name, :expires_at, :org_id)
        """), {
            "table_name": test_table,
            "expires_at": expires_at,
            "org_id": test_organization
        })
    
    # List without including expired
    tables = list_temporary_tables(include_expired=False, engine=test_engine)
    assert not any(t["table_name"] == test_table for t in tables)
    
    # List including expired
    all_tables = list_temporary_tables(include_expired=True, engine=test_engine)
    assert any(t["table_name"] == test_table for t in all_tables)


def test_unmark_temporary_table(test_engine, test_table, test_organization):
    """Test converting a temporary table to permanent."""
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    assert is_temporary_table(test_table, engine=test_engine) is True
    
    # Unmark
    result = unmark_temporary_table(test_table, engine=test_engine)
    
    assert result is True
    assert is_temporary_table(test_table, engine=test_engine) is False


def test_extend_temporary_table_expiration(test_engine, test_table, test_organization):
    """Test extending the expiration of a temporary table."""
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get original expiration
    info_before = get_temporary_table_info(test_table, engine=test_engine)
    original_expires = info_before["expires_at"]
    
    # Extend expiration
    result = extend_temporary_table_expiration(
        table_name=test_table,
        additional_days=14,
        engine=test_engine
    )
    
    assert result is True
    
    # Verify new expiration (should be ~14 days later, allowing for small timing differences)
    info_after = get_temporary_table_info(test_table, engine=test_engine)
    new_expires = info_after["expires_at"]
    
    # The difference should be approximately 14 days (allow 1 second tolerance)
    time_diff = (new_expires - original_expires).total_seconds()
    assert 14 * 86400 - 1 <= time_diff <= 14 * 86400 + 1


def test_allows_additional_imports(test_engine, test_table, test_organization):
    """Test checking if additional imports are allowed."""
    # Mark as temporary with imports not allowed
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        allow_additional_imports=False,
        engine=test_engine
    )
    
    assert allows_additional_imports(test_table, engine=test_engine) is False
    
    # Update to allow imports
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        allow_additional_imports=True,
        engine=test_engine
    )
    
    assert allows_additional_imports(test_table, engine=test_engine) is True


def test_cleanup_expired_temporary_tables(test_engine, test_table, test_organization):
    """Test cleanup of expired temporary tables."""
    # Mark as temporary with negative expiration (already expired)
    with test_engine.begin() as conn:
        expires_at = datetime.now() - timedelta(days=1)
        conn.execute(text("""
            INSERT INTO temporary_tables 
                (table_name, expires_at, organization_id)
            VALUES (:table_name, :expires_at, :org_id)
        """), {
            "table_name": test_table,
            "expires_at": expires_at,
            "org_id": test_organization
        })
    
    # Verify table exists
    with test_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": test_table})
        assert result.fetchone() is not None
    
    # Run cleanup
    cleanup_result = cleanup_expired_temporary_tables(engine=test_engine)
    
    assert cleanup_result["success"] is True
    assert cleanup_result["deleted_count"] >= 1
    assert test_table in cleanup_result["deleted_tables"]
    
    # Verify table was dropped
    with test_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": test_table})
        assert result.fetchone() is None


def test_cleanup_does_not_delete_unexpired_tables(test_engine, test_table, test_organization):
    """Test that cleanup only deletes expired tables."""
    # Mark as temporary with future expiration
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Run cleanup
    cleanup_result = cleanup_expired_temporary_tables(engine=test_engine)
    
    assert cleanup_result["success"] is True
    assert test_table not in cleanup_result["deleted_tables"]
    
    # Verify table still exists
    with test_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": test_table})
        assert result.fetchone() is not None


def test_import_protection_blocks_imports(test_engine, test_table, test_organization):
    """Test that imports are blocked to protected temporary tables."""
    from app.domain.imports.orchestrator import execute_data_import
    from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
    
    # Mark as temporary without allowing imports
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        allow_additional_imports=False,
        engine=test_engine
    )
    
    # Attempt to import data
    mapping_config = MappingConfig(
        table_name=test_table,
        db_schema={"name": "VARCHAR(255)", "value": "INTEGER"},
        mappings={"name": "name", "value": "value"},
        duplicate_check=DuplicateCheckConfig(enabled=False)
    )
    
    # Create minimal CSV content
    csv_content = b"name,value\ntest3,300\n"
    
    # Should raise ValueError about temporary table protection
    with pytest.raises(ValueError) as exc_info:
        execute_data_import(
            file_content=csv_content,
            file_name="test.csv",
            mapping_config=mapping_config,
            source_type="local_upload",
            organization_id=test_organization
        )
    
    assert "temporary table" in str(exc_info.value).lower()
    assert "does not allow additional imports" in str(exc_info.value).lower()


def test_import_protection_allows_when_enabled(test_engine, test_table, test_organization):
    """Test that imports are allowed when enabled for temporary tables."""
    from app.domain.imports.orchestrator import execute_data_import
    from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig
    
    # Mark as temporary WITH allowing imports
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        allow_additional_imports=True,
        engine=test_engine
    )
    
    # Attempt to import data
    mapping_config = MappingConfig(
        table_name=test_table,
        db_schema={"name": "VARCHAR(255)", "value": "INTEGER"},
        mappings={"name": "name", "value": "value"},
        duplicate_check=DuplicateCheckConfig(enabled=False)
    )
    
    # Create minimal CSV content
    csv_content = b"name,value\ntest3,300\n"
    
    # Should succeed
    result = execute_data_import(
        file_content=csv_content,
        file_name="test.csv",
        mapping_config=mapping_config,
        source_type="local_upload",
        organization_id=test_organization
    )
    
    assert result["success"] is True
    assert result["records_processed"] == 1


def test_get_table_names_filters_temporary_by_default(test_engine, test_table, test_organization):
    """Test that temporary tables are hidden from get_table_names by default."""
    from app.db.context import get_table_names
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get table names without including temporary
    tables = get_table_names(include_temporary=False)
    table_names = [t["name"] for t in tables]
    
    # Test table should NOT be in the list
    assert test_table not in table_names


def test_get_table_names_includes_explicitly_requested_temporary(test_engine, test_table, test_organization):
    """Test that temporary tables are included when explicitly requested."""
    from app.db.context import get_table_names
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get table names with explicit request for the temporary table
    tables = get_table_names(
        include_temporary=False,
        explicitly_requested_tables=[test_table]
    )
    table_names = [t["name"] for t in tables]
    
    # Test table SHOULD be in the list when explicitly requested
    assert test_table in table_names


def test_get_table_names_includes_all_when_flag_set(test_engine, test_table, test_organization):
    """Test that temporary tables are included when include_temporary=True."""
    from app.db.context import get_table_names
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get table names with include_temporary flag
    tables = get_table_names(include_temporary=True)
    table_names = [t["name"] for t in tables]
    
    # Test table SHOULD be in the list
    assert test_table in table_names


def test_get_database_schema_filters_temporary_by_default(test_engine, test_table, test_organization):
    """Test that temporary tables are hidden from full schema fetch by default."""
    from app.db.context import get_database_schema
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get schema without specifying tables (should exclude temporary)
    schema = get_database_schema(table_names=None, include_temporary=False)
    
    # Test table should NOT be in the schema
    assert test_table not in schema["tables"]


def test_get_database_schema_includes_temporary_when_named(test_engine, test_table, test_organization):
    """Test that temporary tables are accessible when explicitly named."""
    from app.db.context import get_database_schema
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        engine=test_engine
    )
    
    # Get schema with explicit table name
    schema = get_database_schema(table_names=[test_table])
    
    # Test table SHOULD be in the schema when explicitly requested
    assert test_table in schema["tables"]
    assert "columns" in schema["tables"][test_table]
    assert "row_count" in schema["tables"][test_table]


def test_llm_agent_tools_respect_temporary_filtering(test_engine, test_table, test_organization):
    """Test that LLM agent tools properly filter temporary tables."""
    from app.domain.queries.agent import list_tables_tool, get_table_schema_tool
    
    # Mark as temporary
    mark_table_as_temporary(
        table_name=test_table,
        organization_id=test_organization,
        expires_days=7,
        purpose="Test LLM filtering",
        engine=test_engine
    )
    
    # Test 1: list_tables_tool should hide temporary tables
    list_result = list_tables_tool.invoke({})
    assert test_table not in list_result  # Should not appear in general list
    
    # Test 2: get_table_schema_tool should include temporary tables when explicitly requested
    schema_result = get_table_schema_tool.invoke({"table_names": [test_table]})
    assert test_table in schema_result  # Should appear when explicitly requested
    assert "columns" in schema_result.lower()  # Should contain schema information


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
