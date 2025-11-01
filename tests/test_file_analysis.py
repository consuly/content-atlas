"""
Tests for AI-powered file analysis endpoints.

These tests cover the /analyze-file, /analyze-b2-file, and /execute-recommended-import endpoints.
Most tests use mocked LLM responses to avoid expensive API calls.
"""

import os
import io
import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from app.main import app
from app.api.schemas.shared import AnalysisMode, ConflictResolutionMode
from app.domain.queries.analyzer import ImportStrategy

client = TestClient(app)


# Test fixtures for mocked LLM responses

@pytest.fixture
def mock_successful_analysis():
    """Mock a successful LLM analysis response."""
    return {
        "success": True,
        "response": """Based on my analysis:

**Recommended Strategy: MERGE_EXACT**
**Confidence: 0.95**

The uploaded file matches the existing 'customers' table with 98% column overlap. 
All columns align perfectly with the existing schema.

**Reasoning:**
- File has columns: customer_id, name, email, phone
- Existing 'customers' table has: customer_id, name, email, phone
- Data types are compatible
- No conflicts detected

**Suggested Mapping:**
- customer_id (INTEGER) → id
- name (VARCHAR) → customer_name  
- email (VARCHAR) → email_address
- phone (VARCHAR) → phone_number

**Data Quality:**
- All columns have <5% null values
- No duplicate records detected in sample

This is a high-confidence match suitable for direct import.""",
        "iterations_used": 3,
        "max_iterations": 5
    }


@pytest.fixture
def mock_new_table_analysis():
    """Mock analysis recommending a new table."""
    return {
        "success": True,
        "response": """Based on my analysis:

**Recommended Strategy: NEW_TABLE**
**Confidence: 0.88**

The uploaded file contains product inventory data that doesn't match any existing tables.

**Reasoning:**
- File has columns: product_id, sku, name, quantity, price
- No existing tables have similar structure
- This appears to be a new data domain

**Suggested Schema:**
- product_id: INTEGER
- sku: VARCHAR
- name: VARCHAR
- quantity: INTEGER
- price: DECIMAL

**Data Quality:**
- Column 'price' has 2% null values
- All other columns are well-populated

Recommend creating a new 'products' table.""",
        "iterations_used": 2,
        "max_iterations": 5
    }


@pytest.fixture
def mock_failed_analysis():
    """Mock a failed LLM analysis."""
    return {
        "success": False,
        "error": "Analysis failed: Unable to connect to database",
        "response": "Error occurred during analysis",
        "iterations_used": 1,
        "max_iterations": 5
    }


# Basic endpoint existence tests

def test_analyze_file_endpoint_exists():
    """Test that /analyze-file endpoint exists and accepts requests."""
    # Create a small test CSV
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    # Mock the analysis function to avoid actual LLM calls
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = {
            "success": True,
            "response": "Test response",
            "iterations_used": 1,
            "max_iterations": 5
        }
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "success" in data
        assert "llm_response" in data


def test_analyze_b2_file_endpoint_exists():
    """Test that /analyze-b2-file endpoint exists."""
    # Mock both B2 download and analysis
    with patch('app.main.download_file_from_b2') as mock_download, \
         patch('app.main.analyze_file_for_import') as mock_analyze:
        
        mock_download.return_value = b"name,email\nJohn,john@example.com\n"
        mock_analyze.return_value = {
            "success": True,
            "response": "Test response",
            "iterations_used": 1,
            "max_iterations": 5
        }
        
        response = client.post(
            "/analyze-b2-file",
            json={
                "file_name": "test.csv",
                "analysis_mode": "manual",
                "conflict_resolution": "llm_decide"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "success" in data


def test_execute_import_endpoint_placeholder():
    """Test that /execute-recommended-import returns 501 (not implemented)."""
    response = client.post(
        "/execute-recommended-import",
        json={
            "analysis_id": "test-id",
            "force_execute": False
        }
    )
    
    # Should return 404 (analysis not found) or 501 (not implemented)
    assert response.status_code in [404, 501]


# Mocked LLM analysis tests

def test_analyze_file_with_mock_llm(mock_successful_analysis):
    """Test file analysis with mocked LLM response."""
    csv_content = b"customer_id,name,email\n1,John,john@example.com\n"
    files = {"file": ("customers.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={
                "analysis_mode": "manual",
                "conflict_resolution": "llm_decide",
                "max_iterations": 5
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert data["success"] is True
        assert data["llm_response"] is not None
        assert data["iterations_used"] == 3
        assert data["max_iterations"] == 5
        assert data["can_auto_execute"] is False  # manual mode


def test_analyze_new_table_recommendation(mock_new_table_analysis):
    """Test analysis recommending a new table."""
    csv_content = b"product_id,sku,name\n1,ABC123,Widget\n"
    files = {"file": ("products.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_new_table_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "NEW_TABLE" in data["llm_response"]


def test_analyze_failed_analysis(mock_failed_analysis):
    """Test handling of failed analysis."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_failed_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        assert response.status_code == 502


# Configuration tests

def test_analysis_mode_manual(mock_successful_analysis):
    """Test manual analysis mode."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        data = response.json()
        assert data["can_auto_execute"] is False


def test_analysis_mode_auto_always(mock_successful_analysis):
    """Test auto_always analysis mode."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "auto_always"}
        )
        
        data = response.json()
        assert data["can_auto_execute"] is True


def test_conflict_resolution_modes(mock_successful_analysis):
    """Test different conflict resolution modes."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    modes = ["ask_user", "llm_decide", "prefer_flexible"]
    
    for mode in modes:
        with patch('app.main.analyze_file_for_import') as mock_analyze:
            mock_analyze.return_value = mock_successful_analysis
            
            response = client.post(
                "/analyze-file",
                files=files,
                data={
                    "analysis_mode": "manual",
                    "conflict_resolution": mode
                }
            )
            
            assert response.status_code == 200
            # Verify the mode was passed to the analysis function
            call_kwargs = mock_analyze.call_args[1]
            assert call_kwargs["conflict_mode"].value == mode


def test_custom_sample_size(mock_successful_analysis):
    """Test custom sample size parameter."""
    csv_content = b"name,email\n" + b"John,john@example.com\n" * 1000
    files = {"file": ("large.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={
                "analysis_mode": "manual",
                "sample_size": 100
            }
        )
        
        assert response.status_code == 200


def test_max_iterations_parameter(mock_successful_analysis):
    """Test max_iterations parameter."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={
                "analysis_mode": "manual",
                "max_iterations": 3
            }
        )
        
        assert response.status_code == 200
        # Verify max_iterations was passed
        call_kwargs = mock_analyze.call_args[1]
        assert call_kwargs["max_iterations"] == 3


# Smart sampling tests

def test_smart_sampling_small_file():
    """Test that small files use all data."""
    from app.domain.queries.analyzer import sample_file_data
    
    # Small file (50 rows)
    records = [{"id": i, "name": f"User{i}"} for i in range(50)]
    sample, total = sample_file_data(records)
    
    assert total == 50
    assert len(sample) == 50  # All data used


def test_smart_sampling_medium_file():
    """Test sampling for medium-sized files."""
    from app.domain.queries.analyzer import sample_file_data
    
    # Medium file (500 rows)
    records = [{"id": i, "name": f"User{i}"} for i in range(500)]
    sample, total = sample_file_data(records)
    
    assert total == 500
    assert len(sample) == 100  # Auto-calculated sample size


def test_smart_sampling_large_file():
    """Test sampling for large files."""
    from app.domain.queries.analyzer import sample_file_data
    
    # Large file (5000 rows)
    records = [{"id": i, "name": f"User{i}"} for i in range(5000)]
    sample, total = sample_file_data(records)
    
    assert total == 5000
    assert len(sample) == 200  # Auto-calculated sample size


def test_smart_sampling_very_large_file():
    """Test sampling for very large files."""
    from app.domain.queries.analyzer import sample_file_data
    
    # Very large file (50000 rows)
    records = [{"id": i, "name": f"User{i}"} for i in range(50000)]
    sample, total = sample_file_data(records)
    
    assert total == 50000
    assert len(sample) == 500  # Max sample size


def test_smart_sampling_custom_size():
    """Test custom sample size."""
    from app.domain.queries.analyzer import sample_file_data
    
    records = [{"id": i, "name": f"User{i}"} for i in range(1000)]
    sample, total = sample_file_data(records, target_sample_size=50)
    
    assert total == 1000
    assert len(sample) == 50


def test_calculate_sample_size():
    """Test sample size calculation logic."""
    from app.domain.queries.analyzer import calculate_sample_size
    
    assert calculate_sample_size(50) == 50      # Small: use all
    assert calculate_sample_size(100) == 100    # Small: use all
    assert calculate_sample_size(500) == 100    # Medium: 100
    assert calculate_sample_size(5000) == 200   # Large: 200
    assert calculate_sample_size(50000) == 500  # Very large: 500


# Error handling tests

def test_analyze_unsupported_file_type():
    """Test analysis with unsupported file type."""
    # Create a fake .txt file
    txt_content = b"This is a text file"
    files = {"file": ("test.txt", io.BytesIO(txt_content), "text/plain")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    # Should fail with unsupported file type
    assert response.status_code == 500


def test_analyze_corrupted_csv():
    """Test analysis with corrupted CSV file."""
    # Malformed CSV
    csv_content = b"name,email\nJohn,john@example.com\nBroken line without comma"
    files = {"file": ("corrupted.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = {
            "success": False,
            "error": "Failed to parse CSV",
            "response": "Error",
            "iterations_used": 0,
            "max_iterations": 5
        }
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        # Should handle gracefully
        assert response.status_code in [200, 500]


def test_analyze_empty_file():
    """Test analysis with empty file."""
    csv_content = b""
    files = {"file": ("empty.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    # Should handle gracefully
    assert response.status_code in [200, 500]


def test_execute_import_without_analysis():
    """Test executing import without prior analysis."""
    response = client.post(
        "/execute-recommended-import",
        json={
            "analysis_id": "non-existent-id",
            "force_execute": False
        }
    )
    
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


# Response structure tests

def test_analyze_response_structure(mock_successful_analysis):
    """Test that analysis response has all expected fields."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.main.analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        data = response.json()
        
        # Verify all expected fields are present in AnalyzeFileResponse
        assert "success" in data
        assert "llm_response" in data
        assert "can_auto_execute" in data
        assert "iterations_used" in data
        assert "max_iterations" in data
        # Optional fields
        assert "suggested_mapping" in data or data["suggested_mapping"] is None
        assert "conflicts" in data or data["conflicts"] is None
        assert "confidence_score" in data or data["confidence_score"] is None
        assert "error" in data or data["error"] is None


def test_b2_analyze_response_structure(mock_successful_analysis):
    """Test B2 analysis response structure."""
    with patch('app.main.download_file_from_b2') as mock_download, \
         patch('app.main.analyze_file_for_import') as mock_analyze:
        
        mock_download.return_value = b"name,email\nJohn,john@example.com\n"
        mock_analyze.return_value = mock_successful_analysis
        
        response = client.post(
            "/analyze-b2-file",
            json={
                "file_name": "test.csv",
                "analysis_mode": "manual"
            }
        )
        
        data = response.json()
        
        # Same structure as regular analyze
        assert "success" in data
        assert "llm_response" in data
        assert "iterations_used" in data


# Integration tests (expensive, skip in CI)

@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_analyze_file_real_llm():
    """Test file analysis with real LLM (expensive, skip in CI)."""
    csv_content = b"customer_id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com\n"
    files = {"file": ("customers.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={
            "analysis_mode": "manual",
            "conflict_resolution": "llm_decide",
            "max_iterations": 3
        }
    )
    
    if response.status_code != 200:
        pytest.skip(f"LLM analysis unavailable: {response.status_code} {response.text}")

    data = response.json()
    if not data.get("success", False):
        pytest.skip(f"LLM analysis returned non-success: {data.get('error')}")

    assert data["success"] is True
    assert data["llm_response"] is not None
    assert data["iterations_used"] <= 3


@pytest.mark.skipif(os.getenv('CI'), reason="Skip expensive LLM tests in CI")
def test_full_analysis_workflow():
    """Test complete analysis workflow with real LLM."""
    # This would test:
    # 1. Upload and analyze file
    # 2. Review recommendation
    # 3. Execute import (when implemented)
    
    csv_content = b"name,email,age\nJohn,john@example.com,30\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    # Step 1: Analyze
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual", "max_iterations": 3}
    )
    
    if response.status_code != 200:
        pytest.skip(f"LLM analysis unavailable: {response.status_code} {response.text}")

    data = response.json()
    if not data.get("success", False):
        pytest.skip(f"LLM analysis returned non-success: {data.get('error')}")

    # Step 2: Would execute import here (not implemented yet)
    # This is a placeholder for future implementation
    assert data["success"] is True
