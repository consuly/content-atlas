"""
Test cases for multi-table UNION queries in the query agent.

Validates that the LLM correctly generates UNION/UNION ALL queries when users
request data from multiple tables combined (e.g., "100 from table A + 100 from table B").
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from app.domain.queries.agent import query_database_with_agent


@pytest.fixture
def mock_engine():
    """Mock database engine for testing without actual DB connection."""
    with patch('app.domain.queries.agent.get_engine') as mock_get_engine:
        mock_conn = MagicMock()
        mock_engine_instance = MagicMock()
        mock_engine_instance.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine_instance
        
        # Mock execute to return empty results by default
        mock_result = MagicMock()
        mock_result.keys.return_value = []
        mock_result.fetchmany.return_value = []
        mock_result.scalar.return_value = 0
        mock_conn.execute.return_value = mock_result
        
        yield mock_conn


@pytest.fixture
def mock_schema_with_two_tables():
    """Mock schema with two similar tables for UNION testing."""
    schema = {
        "tables": {
            "clients-list": {
                "columns": [
                    {"name": "contact_full_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "first_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "last_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "email", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "title", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "company_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "company_industry", "type": "VARCHAR", "nullable": True, "default": None},
                ],
                "sample_data": [
                    {
                        "contact_full_name": "John Doe",
                        "email": "john@example.com",
                        "title": "CEO",
                        "company_name": "Acme Corp",
                        "company_industry": "Marketing"
                    }
                ],
                "row_count": 5000,
                "metadata": {
                    "purpose_short": "Client contact database",
                    "data_domain": "CRM"
                }
            },
            "competitors-list": {
                "columns": [
                    {"name": "contact_full_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "first_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "last_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "email", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "title", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "company_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "company_industry", "type": "VARCHAR", "nullable": True, "default": None},
                ],
                "sample_data": [
                    {
                        "contact_full_name": "Jane Smith",
                        "email": "jane@competitor.com",
                        "title": "CTO",
                        "company_name": "Rival Inc",
                        "company_industry": "Software"
                    }
                ],
                "row_count": 3000,
                "metadata": {
                    "purpose_short": "Competitor contact database",
                    "data_domain": "Market Research"
                }
            }
        },
        "relationships": []
    }
    return schema


def test_multi_table_union_prompt_contains_union_guidance():
    """Verify that the system prompt includes UNION/UNION ALL guidance."""
    from app.domain.queries.agent import BASE_SYSTEM_PROMPT
    
    assert "UNION" in BASE_SYSTEM_PROMPT
    assert "UNION ALL" in BASE_SYSTEM_PROMPT
    assert "COMBINING DATA FROM MULTIPLE TABLES" in BASE_SYSTEM_PROMPT
    assert "merging contact lists" in BASE_SYSTEM_PROMPT.lower()


def test_union_query_pattern_recognition():
    """Test that queries requesting 'X from table A and Y from table B' are recognized."""
    test_prompts = [
        "Get 100 records from clients-list and 100 from competitors-list",
        "Merge 500 contacts from table-a and 500 contacts from table-b",
        "Combine 1000 rows from clients and 1000 rows from prospects",
        "Show me 50 from list-a plus 50 from list-b",
    ]
    
    from app.domain.queries.agent import _prompt_requires_sql
    
    for prompt in test_prompts:
        # These should all be recognized as SQL-requiring prompts
        assert _prompt_requires_sql(prompt) is True


@patch('app.domain.queries.agent.get_database_schema')
@patch('app.domain.queries.agent.get_table_names')
def test_union_query_validation_accepts_union_syntax(mock_get_table_names, mock_get_schema, mock_schema_with_two_tables):
    """Test that schema validation accepts UNION queries with proper syntax."""
    from app.domain.queries.agent import validate_sql_against_schema
    
    mock_get_schema.return_value = mock_schema_with_two_tables
    
    # Valid UNION query
    union_query = """
    (SELECT "contact_full_name", "email", "title", "company_name"
     FROM "clients-list"
     WHERE "email" IS NOT NULL
     LIMIT 100)
    UNION ALL
    (SELECT "contact_full_name", "email", "title", "company_name"
     FROM "competitors-list"
     WHERE "email" IS NOT NULL
     LIMIT 100);
    """
    
    is_valid, error = validate_sql_against_schema(union_query)
    assert is_valid is True
    assert error is None


@patch('app.domain.queries.agent.get_database_schema')
@patch('app.domain.queries.agent.get_table_names')
def test_union_query_with_column_aliasing(mock_get_table_names, mock_get_schema):
    """Test UNION query with column aliasing when column names differ."""
    from app.domain.queries.agent import validate_sql_against_schema
    
    # Schema with different column names
    schema = {
        "tables": {
            "table-a": {
                "columns": [
                    {"name": "contact_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "email_address", "type": "VARCHAR", "nullable": True, "default": None},
                ],
                "sample_data": [],
                "row_count": 100
            },
            "table-b": {
                "columns": [
                    {"name": "full_name", "type": "VARCHAR", "nullable": True, "default": None},
                    {"name": "email", "type": "VARCHAR", "nullable": True, "default": None},
                ],
                "sample_data": [],
                "row_count": 100
            }
        },
        "relationships": []
    }
    
    mock_get_schema.return_value = schema
    
    # UNION query with aliasing to standardize columns
    union_query = """
    (SELECT "contact_name" AS "name", "email_address" AS "email"
     FROM "table-a" LIMIT 50)
    UNION ALL
    (SELECT "full_name" AS "name", "email" AS "email"
     FROM "table-b" LIMIT 50);
    """
    
    is_valid, error = validate_sql_against_schema(union_query)
    assert is_valid is True
    assert error is None


@patch('app.domain.queries.agent.get_database_schema')
def test_union_vs_join_distinction(mock_get_schema, mock_schema_with_two_tables):
    """Verify that the prompt distinguishes between UNION (merging) and JOIN (relating)."""
    from app.domain.queries.agent import BASE_SYSTEM_PROMPT
    
    # Check that the prompt explains when to use each
    assert "JOIN" in BASE_SYSTEM_PROMPT and "related" in BASE_SYSTEM_PROMPT
    assert "UNION" in BASE_SYSTEM_PROMPT and "independent tables" in BASE_SYSTEM_PROMPT


def test_union_all_vs_union_guidance():
    """Verify that the prompt explains the difference between UNION and UNION ALL."""
    from app.domain.queries.agent import BASE_SYSTEM_PROMPT
    
    # Should explain that UNION ALL preserves duplicates, UNION removes them
    assert "UNION ALL" in BASE_SYSTEM_PROMPT
    assert "preserves" in BASE_SYSTEM_PROMPT.lower() or "duplicate" in BASE_SYSTEM_PROMPT.lower()


@patch('app.domain.queries.agent.get_database_schema')
@patch('app.domain.queries.agent.get_table_names')
def test_union_query_requires_matching_columns(mock_get_table_names, mock_get_schema):
    """Test that validation notes UNION requires same number of columns."""
    from app.domain.queries.agent import BASE_SYSTEM_PROMPT
    
    # The prompt should mention this requirement
    assert "same number of columns" in BASE_SYSTEM_PROMPT
    assert "same order" in BASE_SYSTEM_PROMPT or "Column" in BASE_SYSTEM_PROMPT


@pytest.mark.integration
@patch('app.domain.queries.agent.ChatAnthropic')
@patch('app.domain.queries.agent.get_database_schema')
@patch('app.domain.queries.agent.get_table_names')
def test_agent_generates_union_query_for_multi_table_request(
    mock_get_table_names, 
    mock_get_schema, 
    mock_llm,
    mock_schema_with_two_tables,
    mock_engine
):
    """
    Integration test: Verify agent generates a UNION query when asked to combine data.
    
    This is a smoke test to ensure the guidance is being used.
    Note: This test mocks the LLM response for deterministic testing.
    """
    mock_get_schema.return_value = mock_schema_with_two_tables
    mock_get_table_names.return_value = [
        {"name": "clients-list", "row_count": 5000, "purpose": "Client contacts", "domain": "CRM"},
        {"name": "competitors-list", "row_count": 3000, "purpose": "Competitor contacts", "domain": "Market Research"}
    ]
    
    # Mock LLM to generate a UNION query
    mock_agent_instance = MagicMock()
    
    # Simulate the agent generating a UNION query
    expected_sql = """(SELECT "contact_full_name", "email", "company_name" 
     FROM "clients-list" 
     WHERE "email" IS NOT NULL 
     LIMIT 100)
    UNION ALL
    (SELECT "contact_full_name", "email", "company_name" 
     FROM "competitors-list" 
     WHERE "email" IS NOT NULL 
     LIMIT 100)"""
    
    # Create mock message objects that match what _extract_agent_outputs expects
    mock_tool_call_msg = Mock()
    mock_tool_call_msg.tool_calls = [
        {
            "name": "execute_sql_query",
            "args": {"sql_query": expected_sql}
        }
    ]
    mock_tool_call_msg.content = "Executing query..."
    
    mock_tool_result_msg = Mock()
    mock_tool_result_msg.name = "execute_sql_query"
    mock_tool_result_msg.tool_calls = None
    mock_tool_result_msg.content = [{
        "type": "text",
        "text": "Query executed successfully.\nRows returned: 200\nExecution time: 0.15s\n\nCSV Data:\ncontact_full_name,email,company_name\nJohn Doe,john@example.com,Acme Corp"
    }]
    
    mock_final_msg = Mock()
    mock_final_msg.tool_calls = None
    mock_final_msg.content = [{
        "type": "text",
        "text": "I've successfully combined 100 records from clients-list and 100 from competitors-list using UNION ALL."
    }]
    
    mock_result = {
        "messages": [
            mock_tool_call_msg,
            mock_tool_result_msg,
            mock_final_msg
        ]
    }
    
    mock_agent_instance.invoke.return_value = mock_result
    
    with patch('app.domain.queries.agent.create_query_agent', return_value=mock_agent_instance):
        result = query_database_with_agent(
            "Get 100 contacts from clients-list and 100 from competitors-list, merge them together",
            thread_id="test-union"
        )
    
    # Verify the result contains a UNION query
    assert result["success"] is True
    assert result["executed_sql"] is not None
    assert "UNION" in result["executed_sql"].upper()
    assert "clients-list" in result["executed_sql"]
    assert "competitors-list" in result["executed_sql"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
