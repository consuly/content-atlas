"""
Tests for database schema retrieval functions.

This module tests the get_database_schema() function to ensure it correctly
retrieves schema information and properly filters out system tables.
"""

import pytest
from sqlalchemy import text
from app.db.context import get_database_schema, format_schema_for_prompt
from app.db.session import get_engine


def test_get_database_schema_basic():
    """Test that get_database_schema() executes without SQL errors."""
    # This test primarily ensures the SQL query is valid and doesn't throw
    # PostgreSQL errors like invalid escape sequences
    schema_info = get_database_schema()
    
    assert isinstance(schema_info, dict)
    assert "tables" in schema_info
    assert "relationships" in schema_info
    assert isinstance(schema_info["tables"], dict)
    assert isinstance(schema_info["relationships"], list)


def test_get_database_schema_excludes_system_tables():
    """Test that system tables are properly excluded from schema results."""
    schema_info = get_database_schema()
    
    # System tables that should be excluded
    excluded_tables = [
        'file_imports', 'table_metadata', 'import_history', 'uploaded_files',
        'users', 'mapping_errors', 'import_jobs', 'import_duplicates',
        'mapping_chunk_status', 'api_keys', 'query_messages', 'query_threads',
        'llm_instructions'
    ]
    
    for table_name in excluded_tables:
        assert table_name not in schema_info["tables"], \
            f"System table '{table_name}' should be excluded from schema"


def test_get_database_schema_excludes_test_tables():
    """Test that tables starting with 'test_' are properly excluded."""
    engine = get_engine()
    
    # Create a test table to verify exclusion
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_should_be_excluded (
                id SERIAL PRIMARY KEY,
                data TEXT
            )
        """))
        conn.commit()
    
    try:
        schema_info = get_database_schema()
        
        # Verify test_ tables are excluded
        for table_name in schema_info["tables"]:
            assert not table_name.startswith("test_"), \
                f"Table '{table_name}' starting with 'test_' should be excluded"
    
    finally:
        # Clean up test table
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_should_be_excluded"))
            conn.commit()


def test_get_database_schema_includes_user_tables():
    """Test that user-created tables are included in schema results."""
    engine = get_engine()
    
    # Create a user table
    test_table_name = "user_data_table_for_test"
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """))
        conn.commit()
    
    try:
        schema_info = get_database_schema()
        
        # Verify user table is included
        assert test_table_name in schema_info["tables"], \
            f"User table '{test_table_name}' should be included in schema"
        
        # Verify table has expected structure
        table_info = schema_info["tables"][test_table_name]
        assert "columns" in table_info
        assert "sample_data" in table_info
        assert "row_count" in table_info
        
        # Verify columns (excluding auto-generated 'id')
        column_names = [col["name"] for col in table_info["columns"]]
        assert "name" in column_names
        assert "email" in column_names
    
    finally:
        # Clean up test table
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_table_name}"))
            conn.commit()


def test_format_schema_for_prompt():
    """Test that schema formatting produces valid output."""
    schema_info = get_database_schema()
    formatted = format_schema_for_prompt(schema_info)
    
    assert isinstance(formatted, str)
    assert len(formatted) > 0
    assert "Database Schema Overview" in formatted
    assert "Total Tables:" in formatted


def test_get_database_schema_with_metadata():
    """Test that schema includes table metadata when available."""
    engine = get_engine()
    
    # Create a test table with some data
    test_table_name = "metadata_test_table"
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {test_table_name} (
                id SERIAL PRIMARY KEY,
                test_column VARCHAR(50)
            )
        """))
        conn.execute(text(f"""
            INSERT INTO {test_table_name} (test_column) VALUES ('sample')
        """))
        conn.commit()
    
    try:
        schema_info = get_database_schema()
        
        if test_table_name in schema_info["tables"]:
            table_info = schema_info["tables"][test_table_name]
            
            # Verify row count is accurate
            assert table_info["row_count"] >= 1
            
            # Verify sample data is included
            assert isinstance(table_info["sample_data"], list)
    
    finally:
        # Clean up
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {test_table_name}"))
            conn.commit()
