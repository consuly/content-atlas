"""
Tests for the export endpoint that allows large CSV downloads.
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from io import StringIO
import csv

from app.main import app
from app.db.session import get_engine, get_db
from app.core.api_key_auth import create_api_key, init_api_key_tables


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


@pytest.fixture
def api_key(client):
    """Create a test API key for authentication."""
    # Ensure API key table exists
    init_api_key_tables()
    
    # Create an API key for testing
    db = next(get_db())
    try:
        api_key_record, plain_key = create_api_key(
            db=db,
            app_name="test-export-key",
            description="Test key for export endpoint tests"
        )
        return plain_key
    finally:
        db.close()


@pytest.fixture
def test_table():
    """Create a test table with sample data."""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Drop if exists
        conn.execute(text('DROP TABLE IF EXISTS "test-export-data"'))
        conn.commit()
        
        # Create test table
        conn.execute(text('''
            CREATE TABLE "test-export-data" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                company VARCHAR(100),
                revenue INTEGER
            )
        '''))
        conn.commit()
        
        # Insert test data (100 rows)
        for i in range(100):
            conn.execute(text('''
                INSERT INTO "test-export-data" (name, email, company, revenue)
                VALUES (:name, :email, :company, :revenue)
            '''), {
                "name": f"Person {i}",
                "email": f"person{i}@example.com",
                "company": f"Company {i % 10}",
                "revenue": (i + 1) * 1000
            })
        conn.commit()
    
    yield "test-export-data"
    
    # Cleanup
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "test-export-data"'))
        conn.commit()


def test_export_query_success(client, api_key, test_table):
    """Test successful export of query results."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" LIMIT 50',
            "filename": "test_export.csv"
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/csv; charset=utf-8"
    assert "test_export.csv" in response.headers["content-disposition"]
    assert response.headers["x-row-count"] == "50"
    
    # Parse CSV content
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    
    assert len(rows) == 50
    assert "name" in rows[0]
    assert "email" in rows[0]
    assert "Person 0" in rows[0]["name"]


def test_export_query_default_filename(client, api_key, test_table):
    """Test export with default filename."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT name, email FROM "{test_table}" LIMIT 10'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    assert "export.csv" in response.headers["content-disposition"]


def test_export_query_adds_csv_extension(client, api_key, test_table):
    """Test that .csv extension is added if missing."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" LIMIT 10',
            "filename": "my_export"
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    assert "my_export.csv" in response.headers["content-disposition"]


def test_export_query_with_where_clause(client, api_key, test_table):
    """Test export with WHERE clause filtering."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" WHERE revenue > 50000'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    
    # Should have 50 rows (IDs 50-99 have revenue > 50000)
    assert len(rows) == 50
    for row in rows:
        assert int(row["revenue"]) > 50000


def test_export_query_with_union(client, api_key, test_table):
    """Test export with UNION query."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'''
                (SELECT name, email FROM "{test_table}" WHERE revenue < 10000 LIMIT 5)
                UNION ALL
                (SELECT name, email FROM "{test_table}" WHERE revenue > 90000 LIMIT 5)
            '''
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    
    # Should have 10 rows total (5 + 5)
    assert len(rows) == 10


def test_export_query_requires_authentication(client, test_table):
    """Test that export requires API key authentication."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" LIMIT 10'
        }
    )
    
    assert response.status_code == 401  # Unauthorized without API key


def test_export_query_rejects_non_select(client, api_key, test_table):
    """Test that non-SELECT queries are rejected."""
    # Try DELETE
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'DELETE FROM "{test_table}" WHERE id = 1'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 400
    assert "Only SELECT queries are allowed" in response.json()["detail"]
    
    # Try UPDATE
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'UPDATE "{test_table}" SET name = \'test\''
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 400
    # Both DELETE and UPDATE are caught by the same validation (not starting with SELECT)
    assert "Only SELECT queries are allowed" in response.json()["detail"]


def test_export_query_blocks_system_tables(client, api_key):
    """Test that system tables are blocked."""
    system_tables = ["users", "api_keys", "import_history", "file_imports"]
    
    for table in system_tables:
        response = client.post(
            "/api/export/query",
            json={
                "sql_query": f'SELECT * FROM {table} LIMIT 10'
            },
            headers={"X-API-Key": api_key}
        )
        
        assert response.status_code == 400
        assert "system table" in response.json()["detail"].lower()


def test_export_query_blocks_sql_injection(client, api_key, test_table):
    """Test that SQL injection attempts are blocked."""
    # Try comment injection
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" -- DROP TABLE users'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 400
    assert "forbidden operations" in response.json()["detail"].lower()
    
    # Try multi-statement
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}"; DROP TABLE users;'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 400


def test_export_query_no_results(client, api_key, test_table):
    """Test export when query returns no results."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" WHERE revenue > 999999999'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 404
    assert "no results" in response.json()["detail"].lower()


def test_export_query_with_aggregation(client, api_key, test_table):
    """Test export with aggregation functions."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'''
                SELECT company, 
                       COUNT(*) as employee_count,
                       AVG(revenue) as avg_revenue,
                       MAX(revenue) as max_revenue
                FROM "{test_table}"
                GROUP BY company
                ORDER BY employee_count DESC
            '''
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    
    # Should have 10 unique companies
    assert len(rows) == 10
    assert "employee_count" in rows[0]
    assert "avg_revenue" in rows[0]


def test_export_query_respects_row_limit(client, api_key, test_table):
    """Test that export respects the configured row limit."""
    # Request more rows than exist in test data
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" LIMIT 200'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    
    # Should only return 100 rows (all available data)
    assert len(rows) == 100


def test_export_query_with_joins(client, api_key):
    """Test export with JOIN operations."""
    engine = get_engine()
    
    # Create two related tables
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "test-companies"'))
        conn.execute(text('DROP TABLE IF EXISTS "test-employees"'))
        conn.commit()
        
        conn.execute(text('''
            CREATE TABLE "test-companies" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            )
        '''))
        
        conn.execute(text('''
            CREATE TABLE "test-employees" (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                company_id INTEGER
            )
        '''))
        conn.commit()
        
        # Insert test data
        for i in range(5):
            conn.execute(text(
                'INSERT INTO "test-companies" (name) VALUES (:name)'
            ), {"name": f"Company {i}"})
        
        for i in range(20):
            conn.execute(text(
                'INSERT INTO "test-employees" (name, company_id) VALUES (:name, :company_id)'
            ), {"name": f"Employee {i}", "company_id": (i % 5) + 1})
        conn.commit()
    
    try:
        # Test JOIN query
        response = client.post(
            "/api/export/query",
            json={
                "sql_query": '''
                    SELECT e.name as employee_name, 
                           c.name as company_name
                    FROM "test-employees" e
                    JOIN "test-companies" c ON e.company_id = c.id
                    ORDER BY c.name, e.name
                '''
            },
            headers={"X-API-Key": api_key}
        )
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == 20
        assert "employee_name" in rows[0]
        assert "company_name" in rows[0]
        
    finally:
        # Cleanup
        with engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "test-employees"'))
            conn.execute(text('DROP TABLE IF EXISTS "test-companies"'))
            conn.commit()


def test_export_health_endpoint(client):
    """Test the export health check endpoint."""
    response = client.get("/api/export/health")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["status"] == "healthy"
    assert data["service"] == "export"
    assert "max_rows" in data
    assert "timeout_seconds" in data
    assert data["max_rows"] == 100000  # Default value
    assert data["timeout_seconds"] == 120  # Default value


def test_export_query_execution_time_header(client, api_key, test_table):
    """Test that execution time is included in response headers."""
    response = client.post(
        "/api/export/query",
        json={
            "sql_query": f'SELECT * FROM "{test_table}" LIMIT 10'
        },
        headers={"X-API-Key": api_key}
    )
    
    assert response.status_code == 200
    assert "x-execution-time" in response.headers
    
    # Execution time should be a valid float with 's' suffix
    exec_time = response.headers["x-execution-time"]
    assert exec_time.endswith("s")
    float(exec_time[:-1])  # Should not raise ValueError


def test_export_query_with_special_characters(client, api_key):
    """Test export with column names containing special characters."""
    engine = get_engine()
    
    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS "test-special-cols"'))
        conn.commit()
        
        conn.execute(text('''
            CREATE TABLE "test-special-cols" (
                "Full Name" VARCHAR(100),
                "Email Address" VARCHAR(100),
                "Company-Name" VARCHAR(100)
            )
        '''))
        conn.commit()
        
        conn.execute(text('''
            INSERT INTO "test-special-cols" VALUES
            ('John Doe', 'john@example.com', 'ACME Corp'),
            ('Jane Smith', 'jane@example.com', 'Tech-Solutions')
        '''))
        conn.commit()
    
    try:
        response = client.post(
            "/api/export/query",
            json={
                "sql_query": 'SELECT "Full Name", "Email Address", "Company-Name" FROM "test-special-cols"'
            },
            headers={"X-API-Key": api_key}
        )
        
        assert response.status_code == 200
        
        csv_content = response.text
        reader = csv.DictReader(StringIO(csv_content))
        rows = list(reader)
        
        assert len(rows) == 2
        assert "Full Name" in rows[0]
        assert rows[0]["Full Name"] == "John Doe"
        
    finally:
        with engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "test-special-cols"'))
            conn.commit()
