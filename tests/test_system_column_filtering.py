"""
Tests to verify that system columns are properly filtered from LLM queries.

System columns like _row_id, _import_id, etc. should never be exposed to
the LLM or included in natural language query results.
"""

import pytest
from app.db.context import get_database_schema, format_schema_for_prompt
from app.db.models import SYSTEM_COLUMNS
from app.db.session import get_engine
from sqlalchemy import text


@pytest.fixture
def test_table_with_system_columns():
    """Create a test table with both user and system columns."""
    engine = get_engine()
    
    # Use a non-test-prefixed name to avoid being filtered by get_database_schema
    table_name = "demo_system_filter"
    
    with engine.begin() as conn:
        # Drop table if it exists from previous test
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        # Create a simple test table with system columns
        conn.execute(text(f"""
            CREATE TABLE {table_name} (
                _row_id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                _import_id UUID,
                _imported_at TIMESTAMP DEFAULT NOW(),
                _source_row_number INTEGER,
                _corrections_applied JSONB
            )
        """))
        
        # Insert test data
        conn.execute(text(f"""
            INSERT INTO {table_name} (name, email, _import_id, _source_row_number)
            VALUES 
                ('John Doe', 'john@example.com', gen_random_uuid(), 1),
                ('Jane Smith', 'jane@example.com', gen_random_uuid(), 2)
        """))
    
    yield table_name
    
    # Cleanup
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_get_database_schema_excludes_system_columns(test_table_with_system_columns):
    """Test that get_database_schema filters out system columns."""
    table_name = test_table_with_system_columns
    schema_info = get_database_schema()
    
    # Find our test table
    assert table_name in schema_info["tables"], f"Test table '{table_name}' should be in schema"
    
    table_info = schema_info["tables"][table_name]
    column_names = [col["name"] for col in table_info["columns"]]
    
    # User columns should be present
    assert "name" in column_names, "User column 'name' should be present"
    assert "email" in column_names, "User column 'email' should be present"
    
    # System columns should be filtered out
    for system_col in SYSTEM_COLUMNS:
        assert system_col not in column_names, (
            f"System column '{system_col}' should NOT be in schema exposed to LLM. "
            f"Found columns: {column_names}"
        )


def test_format_schema_for_prompt_excludes_system_columns(test_table_with_system_columns):
    """Test that formatted schema prompt doesn't mention system columns."""
    schema_info = get_database_schema()
    formatted = format_schema_for_prompt(schema_info)
    
    # User columns should be mentioned
    assert "name" in formatted, "User column 'name' should be in formatted schema"
    assert "email" in formatted, "User column 'email' should be in formatted schema"
    
    # System columns should NOT be mentioned
    for system_col in SYSTEM_COLUMNS:
        assert system_col not in formatted, (
            f"System column '{system_col}' should NOT appear in formatted schema. "
            f"Found in prompt:\n{formatted}"
        )


def test_sample_data_excludes_system_columns(test_table_with_system_columns):
    """Test that sample data doesn't include system columns."""
    table_name = test_table_with_system_columns
    schema_info = get_database_schema()
    
    table_info = schema_info["tables"][table_name]
    sample_data = table_info["sample_data"]
    
    assert len(sample_data) > 0, "Should have sample data"
    
    # Check first sample record
    first_sample = sample_data[0]
    sample_keys = list(first_sample.keys())
    
    # User columns should be present
    assert "name" in sample_keys, "User column 'name' should be in sample data"
    assert "email" in sample_keys, "User column 'email' should be in sample data"
    
    # System columns should be filtered out
    for system_col in SYSTEM_COLUMNS:
        assert system_col not in sample_keys, (
            f"System column '{system_col}' should NOT be in sample data. "
            f"Found keys: {sample_keys}"
        )


def test_all_system_columns_are_filtered():
    """Test that all defined system columns are properly filtered."""
    # This test ensures we're filtering all columns defined in SYSTEM_COLUMNS
    expected_system_columns = {
        '_row_id',
        '_import_id',
        '_imported_at',
        '_source_row_number',
        '_corrections_applied'
    }
    
    assert SYSTEM_COLUMNS == expected_system_columns, (
        f"SYSTEM_COLUMNS definition has changed. "
        f"Expected: {expected_system_columns}, "
        f"Got: {SYSTEM_COLUMNS}"
    )


def test_system_columns_not_in_llm_context(test_table_with_system_columns):
    """
    Integration test: Verify that when LLM gets schema context,
    it doesn't see any system columns.
    """
    schema_info = get_database_schema()
    formatted = format_schema_for_prompt(schema_info)
    
    # Split into lines and check each line
    lines = formatted.split('\n')
    
    for line in lines:
        for system_col in SYSTEM_COLUMNS:
            assert system_col not in line, (
                f"System column '{system_col}' found in LLM context line: {line}"
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
