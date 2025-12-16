"""
Tests for SQL query validation against database schema.

Tests cover:
1. SELECT DISTINCT + ORDER BY validation
2. Column existence validation
3. Table existence validation
"""
import pytest
from app.domain.queries.agent import validate_sql_against_schema
from app.db.session import get_engine
from sqlalchemy import text


@pytest.fixture
def setup_test_table():
    """Create a test table for validation testing."""
    engine = get_engine()
    with engine.connect() as conn:
        # Create test table with sample data
        conn.execute(text("""
            DROP TABLE IF EXISTS "test-validation-table"
        """))
        conn.execute(text("""
            CREATE TABLE "test-validation-table" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER,
                city VARCHAR(100),
                seniority VARCHAR(50),
                department VARCHAR(100)
            )
        """))
        conn.execute(text("""
            INSERT INTO "test-validation-table" (name, age, city, seniority, department)
            VALUES 
                ('Alice', 30, 'New York', 'Senior', 'Engineering'),
                ('Bob', 25, 'San Francisco', 'Junior', 'Marketing'),
                ('Charlie', 35, 'Boston', 'Manager', 'Sales')
        """))
        conn.commit()
    
    yield
    
    # Cleanup
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "test-validation-table"'))
        conn.commit()


class TestDistinctOrderByValidation:
    """Test SELECT DISTINCT + ORDER BY constraint validation."""
    
    def test_valid_distinct_with_order_by_in_select(self, setup_test_table):
        """Valid: ORDER BY column is in SELECT list."""
        sql = """
            SELECT DISTINCT "name", "age"
            FROM "test-validation-table"
            ORDER BY "age"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_invalid_distinct_with_order_by_not_in_select(self, setup_test_table):
        """Invalid: ORDER BY column not in SELECT list."""
        sql = """
            SELECT DISTINCT "name"
            FROM "test-validation-table"
            ORDER BY "age"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "age" in error
        assert "ORDER BY" in error
    
    def test_invalid_distinct_with_case_expression_order_by(self, setup_test_table):
        """Invalid: CASE expression in ORDER BY references column not in SELECT."""
        sql = """
            SELECT DISTINCT "name", "city"
            FROM "test-validation-table"
            ORDER BY CASE 
                WHEN "seniority" = 'Senior' THEN 1
                ELSE 2
            END
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "seniority" in error
        assert "ORDER BY" in error
    
    def test_valid_distinct_with_case_expression_and_column_in_select(self, setup_test_table):
        """Valid: CASE expression column is also in SELECT."""
        sql = """
            SELECT DISTINCT "name", "seniority"
            FROM "test-validation-table"
            ORDER BY CASE 
                WHEN "seniority" = 'Senior' THEN 1
                ELSE 2
            END
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_valid_select_without_distinct(self, setup_test_table):
        """Valid: Regular SELECT can ORDER BY any column."""
        sql = """
            SELECT "name"
            FROM "test-validation-table"
            ORDER BY "age"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None


class TestColumnExistenceValidation:
    """Test column name validation against schema."""
    
    def test_valid_column_references(self, setup_test_table):
        """Valid: All columns exist in the table."""
        sql = """
            SELECT "name", "age", "city"
            FROM "test-validation-table"
            WHERE "department" = 'Engineering'
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_invalid_column_reference(self, setup_test_table):
        """Invalid: Column does not exist in table."""
        sql = """
            SELECT "name", "nonexistent_column"
            FROM "test-validation-table"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "nonexistent_column" in error
        assert "does not exist" in error
    
    def test_invalid_column_in_where_clause(self, setup_test_table):
        """Invalid: Non-existent column in WHERE clause."""
        sql = """
            SELECT "name"
            FROM "test-validation-table"
            WHERE "fake_column" = 'value'
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "fake_column" in error
    
    def test_invalid_column_in_order_by(self, setup_test_table):
        """Invalid: Non-existent column in ORDER BY."""
        sql = """
            SELECT "name", "age"
            FROM "test-validation-table"
            ORDER BY "missing_column"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "missing_column" in error


class TestTableExistenceValidation:
    """Test table name validation."""
    
    def test_valid_table_reference(self, setup_test_table):
        """Valid: Table exists."""
        sql = """
            SELECT "name"
            FROM "test-validation-table"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_invalid_table_reference(self):
        """Invalid: Table does not exist."""
        sql = """
            SELECT "name"
            FROM "nonexistent_table"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "nonexistent_table" in error
        assert "does not exist" in error


class TestComplexQueryValidation:
    """Test validation of complex queries."""
    
    def test_valid_complex_query_with_aggregation(self, setup_test_table):
        """Valid: Complex query with GROUP BY and HAVING."""
        sql = """
            SELECT "department", COUNT(*) as count, AVG("age") as avg_age
            FROM "test-validation-table"
            GROUP BY "department"
            HAVING COUNT(*) > 0
            ORDER BY count DESC
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_invalid_complex_query_with_case_and_distinct(self, setup_test_table):
        """Invalid: Real-world scenario from bug report - DISTINCT with CASE ORDER BY."""
        sql = """
            SELECT DISTINCT
                "name",
                "city",
                "department"
            FROM "test-validation-table"
            WHERE "department" IN ('Engineering', 'Marketing')
            ORDER BY 
                CASE 
                    WHEN "seniority" = 'Senior' THEN 1
                    WHEN "seniority" = 'Manager' THEN 2
                    ELSE 3
                END ASC,
                "city" ASC
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "VALIDATION ERROR" in error
        assert "seniority" in error
        assert "ORDER BY" in error
        assert "SELECT" in error


class TestValidationErrorMessages:
    """Test that error messages are helpful and actionable."""
    
    def test_error_message_suggests_fix_for_distinct_order_by(self, setup_test_table):
        """Error message should suggest adding column to SELECT or removing DISTINCT."""
        sql = """
            SELECT DISTINCT "name"
            FROM "test-validation-table"
            ORDER BY "age"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "Fix:" in error
        assert ("add" in error.lower() or "remove DISTINCT" in error)
    
    def test_error_message_lists_available_columns(self, setup_test_table):
        """Error message should list available columns for reference."""
        sql = """
            SELECT "invalid_column"
            FROM "test-validation-table"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "Available columns:" in error
        # Should mention at least some real columns
        assert any(col in error for col in ["name", "age", "city"])
    
    def test_error_message_suggests_schema_tool(self, setup_test_table):
        """Error message should suggest using get_database_schema_tool."""
        sql = """
            SELECT "wrong_column"
            FROM "test-validation-table"
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is False
        assert "get_database_schema_tool" in error


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_validation_handles_sql_keywords_in_order_by(self, setup_test_table):
        """Validation should not flag SQL keywords as missing columns."""
        sql = """
            SELECT "name", "age"
            FROM "test-validation-table"
            ORDER BY "age" DESC NULLS LAST
        """
        is_valid, error = validate_sql_against_schema(sql)
        assert is_valid is True
        assert error is None
    
    def test_validation_with_table_alias(self, setup_test_table):
        """Validation should handle table aliases gracefully."""
        sql = """
            SELECT t."name", t."age"
            FROM "test-validation-table" t
            ORDER BY t."age"
        """
        # This might pass validation or fail gracefully
        # The important thing is it doesn't crash
        is_valid, error = validate_sql_against_schema(sql)
        assert isinstance(is_valid, bool)
    
    def test_validation_fails_open_on_internal_error(self):
        """If validation encounters an internal error, fail open (allow query)."""
        # Malformed SQL that might cause parsing issues
        sql = "SELECT COMPLETELY INVALID SQL THAT BREAKS PARSER"
        is_valid, error = validate_sql_against_schema(sql)
        # Should fail open rather than crash
        assert is_valid is True  # Fail open
        assert error is None
