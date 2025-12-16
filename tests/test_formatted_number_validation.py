"""
Tests for formatted number validation in SQL queries.

This test suite verifies that the validate_sql_against_schema function
correctly detects and prevents casting formatted numbers (with commas, 
dollar signs, etc.) directly to numeric types without proper sanitization.
"""
import pytest
from unittest.mock import patch, MagicMock
from app.domain.queries.agent import validate_sql_against_schema


@pytest.fixture
def mock_schema_with_formatted_numbers():
    """Mock schema with text columns containing formatted numbers."""
    return {
        "tables": {
            "investors-list": {
                "columns": [
                    {"name": "firm_name", "type": "VARCHAR"},
                    {"name": "assets_under_management", "type": "VARCHAR"},
                    {"name": "total_investments_count", "type": "VARCHAR"},
                    {"name": "dry_powder_aum", "type": "VARCHAR"},
                    {"name": "revenue", "type": "VARCHAR"},
                ],
                "sample_values": {
                    "firm_name": ["ABC Capital", "XYZ Ventures", "Test Fund"],
                    "assets_under_management": ["10,806.40", "5,234.56", "1,000,000.00"],
                    "total_investments_count": ["25", "30", "42"],
                    "dry_powder_aum": ["$5,000.00", "$10,250.50", "$25,000.00"],
                    "revenue": ["100000", "250000", "500000"],
                }
            }
        }
    }


@pytest.fixture
def mock_schema_clean_numbers():
    """Mock schema with text columns containing clean numeric values."""
    return {
        "tables": {
            "clean-data": {
                "columns": [
                    {"name": "id", "type": "VARCHAR"},
                    {"name": "amount", "type": "VARCHAR"},
                ],
                "sample_values": {
                    "id": ["1", "2", "3"],
                    "amount": ["100.50", "200.75", "300.00"],
                }
            }
        }
    }


def test_detect_comma_formatted_cast(mock_schema_with_formatted_numbers):
    """Test detection of CAST on comma-separated numbers."""
    sql = '''
        SELECT "firm_name", CAST("assets_under_management" AS DECIMAL) as aum
        FROM "investors-list"
        ORDER BY aum DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "VALIDATION ERROR" in error
    assert "formatted number" in error.lower()
    assert "assets_under_management" in error
    assert "10,806.40" in error
    assert "REPLACE" in error


def test_detect_double_colon_cast(mock_schema_with_formatted_numbers):
    """Test detection of :: casting syntax on formatted numbers."""
    sql = '''
        SELECT "firm_name", "assets_under_management"::DECIMAL as aum
        FROM "investors-list"
        ORDER BY aum DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "VALIDATION ERROR" in error
    assert "assets_under_management" in error


def test_detect_currency_formatted_cast(mock_schema_with_formatted_numbers):
    """Test detection of CAST on currency values with dollar signs."""
    sql = '''
        SELECT "firm_name", CAST("dry_powder_aum" AS DECIMAL) as powder
        FROM "investors-list"
        ORDER BY powder DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "VALIDATION ERROR" in error
    assert "dry_powder_aum" in error
    assert "$5,000.00" in error or "$10,250.50" in error


def test_allow_cast_with_replace_comma(mock_schema_with_formatted_numbers):
    """Test that CAST with REPLACE is allowed (user already fixed it)."""
    sql = '''
        SELECT "firm_name", 
               CAST(REPLACE("assets_under_management", ',', '') AS DECIMAL) as aum
        FROM "investors-list"
        ORDER BY aum DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert is_valid
    assert error is None


def test_allow_cast_with_nested_replace(mock_schema_with_formatted_numbers):
    """Test that nested REPLACE calls (for currency) are allowed."""
    sql = '''
        SELECT "firm_name", 
               CAST(REPLACE(REPLACE("dry_powder_aum", '$', ''), ',', '') AS DECIMAL) as powder
        FROM "investors-list"
        ORDER BY powder DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert is_valid
    assert error is None


def test_allow_cast_clean_numbers(mock_schema_clean_numbers):
    """Test that CAST on clean numeric text values is allowed."""
    sql = '''
        SELECT "id", CAST("amount" AS DECIMAL) as amt
        FROM "clean-data"
        ORDER BY amt DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_clean_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert is_valid
    assert error is None


def test_detect_cast_in_coalesce(mock_schema_with_formatted_numbers):
    """Test detection of formatted numbers in COALESCE with CAST."""
    sql = '''
        SELECT "firm_name",
               COALESCE(CAST("assets_under_management" AS DECIMAL), 0) as aum
        FROM "investors-list"
        ORDER BY aum DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "VALIDATION ERROR" in error
    assert "assets_under_management" in error


def test_detect_cast_in_order_by(mock_schema_with_formatted_numbers):
    """Test detection of formatted numbers in ORDER BY clause."""
    sql = '''
        SELECT "firm_name", "assets_under_management"
        FROM "investors-list"
        ORDER BY CAST("assets_under_management" AS DECIMAL) DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "VALIDATION ERROR" in error


def test_allow_cast_integer_type(mock_schema_with_formatted_numbers):
    """Test that clean integer-like strings can be cast to INTEGER."""
    sql = '''
        SELECT "firm_name", CAST("total_investments_count" AS INTEGER) as count
        FROM "investors-list"
        ORDER BY count DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    # Should be valid - no commas in sample values for this column
    assert is_valid
    assert error is None


def test_multiple_casts_one_formatted(mock_schema_with_formatted_numbers):
    """Test query with multiple casts where only one has formatted numbers."""
    sql = '''
        SELECT "firm_name", 
               CAST("total_investments_count" AS INTEGER) as count,
               CAST("assets_under_management" AS DECIMAL) as aum
        FROM "investors-list"
        ORDER BY aum DESC
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    # Should fail due to assets_under_management having commas
    assert not is_valid
    assert "assets_under_management" in error


def test_error_message_includes_fix_suggestion(mock_schema_with_formatted_numbers):
    """Test that error message includes helpful fix suggestion."""
    sql = '''
        SELECT CAST("assets_under_management" AS DECIMAL) as aum
        FROM "investors-list"
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    assert "Fix:" in error
    assert 'CAST(REPLACE("assets_under_management", \',\', \'\') AS DECIMAL)' in error


def test_error_message_shows_sample_value(mock_schema_with_formatted_numbers):
    """Test that error message shows actual problematic sample value."""
    sql = '''
        SELECT CAST("assets_under_management" AS DECIMAL) as aum
        FROM "investors-list"
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    assert not is_valid
    # Should show one of the sample values with commas
    assert any(val in error for val in ["10,806.40", "5,234.56", "1,000,000.00"])


def test_validation_gracefully_handles_missing_samples():
    """Test that validation doesn't crash if sample values are missing."""
    schema_no_samples = {
        "tables": {
            "test-table": {
                "columns": [
                    {"name": "amount", "type": "VARCHAR"},
                ],
                "sample_values": {}  # No samples available
            }
        }
    }
    
    sql = '''
        SELECT CAST("amount" AS DECIMAL) as amt
        FROM "test-table"
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=schema_no_samples):
        is_valid, error = validate_sql_against_schema(sql)
    
    # Should pass validation if no samples to check
    assert is_valid


def test_real_world_query_from_logs(mock_schema_with_formatted_numbers):
    """Test the actual problematic query from the production logs."""
    sql = '''
        SELECT 
          "row_id",
          "firm_name",
          "firm_website",
          "firm_description",
          "contact_person_name",
          "contact_phone",
          "contact_title",
          "contact_email",
          "total_investments_count",
          "active_investments_count",
          "dry_powder_aum",
          "investment_notes",
          "investment_notes_2",
          "preferred_sectors",
          "preferred_investment_size",
          "preferred_transaction_sizes",
          "additional_preferences",
          "last_fund_closed_date",
          "assets_under_management",
          "investor_type",
          "headquarters_location"
        FROM "investors-list"
        WHERE 
          (
            "preferred_sectors" ILIKE '%Software%' 
            OR "preferred_sectors" ILIKE '%AI%' 
            OR "preferred_sectors" ILIKE '%Artificial Intelligence%'
            OR "preferred_sectors" ILIKE '%Content%'
            OR "preferred_sectors" ILIKE '%Media%'
            OR "preferred_sectors" ILIKE '%Technology%'
            OR "preferred_sectors" ILIKE '%Internet%'
            OR "preferred_sectors" ILIKE '%Digital%'
            OR "preferred_sectors" ILIKE '%SaaS%'
            OR "preferred_sectors" ILIKE '%Marketing%'
          )
          AND (
            "contact_person_name" IS NOT NULL 
            OR "contact_phone" IS NOT NULL 
            OR "contact_email" IS NOT NULL
          )
          AND "firm_name" IS NOT NULL
        ORDER BY 
          CASE 
            WHEN "preferred_sectors" ILIKE '%AI%' OR "preferred_sectors" ILIKE '%Artificial Intelligence%' THEN 1
            WHEN "preferred_sectors" ILIKE '%Software%' THEN 2
            WHEN "preferred_sectors" ILIKE '%Content%' OR "preferred_sectors" ILIKE '%Media%' THEN 3
            ELSE 4
          END,
          COALESCE(CAST("total_investments_count" AS INTEGER), 0) DESC,
          COALESCE(CAST("assets_under_management" AS DECIMAL), 0) DESC
        LIMIT 100;
    '''
    
    with patch('app.domain.queries.agent.get_database_schema', return_value=mock_schema_with_formatted_numbers):
        is_valid, error = validate_sql_against_schema(sql)
    
    # Should catch the assets_under_management cast issue
    assert not is_valid
    assert "assets_under_management" in error
    assert "10,806.40" in error
