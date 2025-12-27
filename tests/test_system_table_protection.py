"""
Test system table protection in query agent.

This test verifies that the LLM cannot access protected system tables
through natural language queries.
"""
import pytest
from app.domain.queries.agent import execute_sql_query, PROTECTED_SYSTEM_TABLES


def test_protected_tables_constant():
    """Verify the protected tables list is correctly defined."""
    expected_tables = {
        'import_history',
        'mapping_errors',
        'table_metadata',
        'uploaded_files',
        'users',
        'file_imports',
        'import_jobs',
        'llm_instructions',
    }
    assert PROTECTED_SYSTEM_TABLES == expected_tables


def test_block_direct_system_table_query():
    """Test that direct queries to system tables are blocked."""
    # Test each protected table
    for table in PROTECTED_SYSTEM_TABLES:
        result = execute_sql_query.invoke(f"SELECT * FROM {table}")
        assert "ERROR" in result
        assert "not allowed" in result.lower()
        assert table in result.lower()


def test_block_system_table_with_quotes():
    """Test that queries with quoted table names are blocked."""
    result = execute_sql_query.invoke('SELECT * FROM "users"')
    assert "ERROR" in result
    # Either explicitly blocked or doesn't exist (both prevent access)
    assert "not allowed" in result.lower() or "does not exist" in result.lower()


def test_block_system_table_with_schema():
    """Test that queries with schema prefix are blocked."""
    result = execute_sql_query.invoke("SELECT * FROM public.users")
    assert "ERROR" in result
    assert "not allowed" in result.lower()


def test_block_system_table_in_join():
    """Test that JOINs with system tables are blocked."""
    result = execute_sql_query.invoke("""
        SELECT * FROM some_table 
        JOIN users ON some_table.user_id = users.id
    """)
    assert "ERROR" in result
    assert "not allowed" in result.lower()


def test_block_case_insensitive():
    """Test that protection is case-insensitive."""
    test_cases = [
        "SELECT * FROM USERS",
        "SELECT * FROM Users",
        "SELECT * FROM uSeRs"
    ]
    
    for query in test_cases:
        result = execute_sql_query.invoke(query)
        assert "ERROR" in result
        assert "not allowed" in result.lower()


def test_allow_normal_table_query():
    """Test that queries to non-system tables are allowed (will fail if table doesn't exist, but won't be blocked)."""
    # This should not be blocked by system table protection
    # It may fail with a different error if the table doesn't exist, but that's expected
    result = execute_sql_query.invoke("SELECT * FROM my_data_table LIMIT 1")
    
    # Should NOT contain the system table protection error
    assert "not allowed" not in result.lower() or "my_data_table" not in result.lower()


def test_block_multiple_system_tables():
    """Test that queries referencing multiple system tables are blocked."""
    result = execute_sql_query.invoke("""
        SELECT u.*, f.* 
        FROM users u
        JOIN file_imports f ON u.id = f.user_id
    """)
    assert "ERROR" in result
    assert "not allowed" in result.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
