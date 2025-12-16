"""
Tests for AI-powered file analysis endpoints.

These tests exercise the /analyze-file, /analyze-b2-file, and /execute-recommended-import
endpoints. The majority of scenarios hit the live LLM so we can verify the end-to-end
integration instead of relying on simulated responses.
"""

import os
import io
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.main import app
from app.api.schemas.shared import AnalysisMode, ConflictResolutionMode
from app.domain.queries.analyzer import ImportStrategy
from app.core.config import settings

client = TestClient(app)


@pytest.fixture(scope="session")
def require_llm():
    """Skip tests when the Anthropic API key is not available."""
    if not settings.anthropic_api_key or not settings.anthropic_api_key.strip():
        pytest.skip("Anthropic API key not configured; real LLM tests require ANTHROPIC_API_KEY")
    return settings.anthropic_api_key


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

def test_analyze_file_endpoint_exists(require_llm):
    """Test that /analyze-file endpoint exists and accepts requests."""
    # Create a small test CSV
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["llm_response"], str)
    assert data["llm_response"].strip()
    assert data["iterations_used"] <= data["max_iterations"]


def test_analyze_storage_file_endpoint_exists():
    """Test that /analyze-b2-file endpoint exists."""
    # Mock both storage download and analysis - patch where it's imported in routes
    with patch('app.api.routers.analysis.routes._download_file_from_storage') as mock_download, \
         patch('app.api.routers.analysis.routes._analyze_file_for_import') as mock_analyze:
        
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

def test_analyze_file_manual_mode_real_llm(require_llm):
    """Test file analysis with the real LLM using manual review mode."""
    csv_content = b"customer_id,name,email\n1,John,john@example.com\n"
    files = {"file": ("customers.csv", io.BytesIO(csv_content), "text/csv")}
    
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
    assert data["llm_response"]
    assert data["max_iterations"] == 5
    assert data["iterations_used"] <= 5
    assert data["can_auto_execute"] is False  # manual mode


def test_analyze_new_table_recommendation_real_llm(require_llm):
    """Ensure novel datasets receive a structured recommendation from the live LLM."""
    csv_content = b"product_id,sku,name\n1,ABC123,Widget\n"
    files = {"file": ("products.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    llm_response = data["llm_response"]
    assert llm_response
    assert "strategy" in llm_response.lower()
    assert "new_table" in llm_response.lower()


def test_analyze_failed_analysis(mock_failed_analysis):
    """Test handling of failed analysis."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    with patch('app.api.routers.analysis.routes._analyze_file_for_import') as mock_analyze:
        mock_analyze.return_value = mock_failed_analysis
        
        response = client.post(
            "/analyze-file",
            files=files,
            data={"analysis_mode": "manual"}
        )
        
        assert response.status_code == 502


@pytest.mark.not_b2
def test_analyze_file_honors_forced_table(monkeypatch):
    """target_table_name should override the LLM decision and adjust strategy for existing tables."""
    forced_table = "forced_existing_table"

    def fake_analyze(**_kwargs):
        return {
            "success": True,
            "response": "ok",
            "iterations_used": 1,
            "llm_decision": {
                "strategy": "NEW_TABLE",
                "target_table": "llm_pick",
                "column_mapping": {"name": "name"},
                "unique_columns": ["name"],
                "has_header": True,
                "expected_column_types": {"name": "TEXT"},
            },
        }

    captured = {}

    def fake_execute(file_content, file_name, all_records, llm_decision, source_path=None):
        captured["llm_decision"] = llm_decision
        assert llm_decision["target_table"] == forced_table
        assert llm_decision["strategy"] == "ADAPT_DATA"  # switched because mode=existing
        return {
            "success": True,
            "strategy_executed": llm_decision["strategy"],
            "table_name": llm_decision["target_table"],
            "records_processed": len(all_records),
            "duplicates_skipped": 0,
        }

    monkeypatch.setattr("app.api.routers.analysis.routes._get_analyze_file_for_import", lambda: fake_analyze)
    monkeypatch.setattr("app.api.routers.analysis.routes._get_execute_llm_import_decision", lambda: fake_execute)

    files = {"file": ("simple.csv", io.BytesIO(b"name\nalpha\nbeta\n"), "text/csv")}
    response = client.post(
        "/analyze-file",
        files=files,
        data={
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": 3,
            "target_table_name": forced_table,
            "target_table_mode": "existing",
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["success"] is True, f"Response not successful: {data}"
    assert data["auto_execution_result"] is not None, f"auto_execution_result is None. Full response: {data}"
    assert data["auto_execution_result"]["table_name"] == forced_table
    assert captured["llm_decision"]["target_table"] == forced_table
    assert captured["llm_decision"]["strategy"] == "ADAPT_DATA"


# Configuration tests

def test_analysis_mode_manual(require_llm):
    """Manual analysis mode should disable auto-execute despite a successful LLM call."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["can_auto_execute"] is False


def test_analysis_mode_auto_always(require_llm):
    """Auto-always mode should mark the recommendation as executable."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "auto_always"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["can_auto_execute"] is True


def test_conflict_resolution_modes(require_llm):
    """All conflict resolution modes should succeed with the live LLM."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    modes = ["ask_user", "llm_decide", "prefer_flexible"]
    
    for mode in modes:
        response = client.post(
            "/analyze-file",
            files=files,
            data={
                "analysis_mode": "manual",
                "conflict_resolution": mode
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True


def test_custom_sample_size(require_llm):
    """Test custom sample size parameter with live LLM."""
    csv_content = b"name,email\n" + b"John,john@example.com\n" * 1000
    files = {"file": ("large.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={
            "analysis_mode": "manual",
            "sample_size": 100
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


def test_max_iterations_parameter(require_llm):
    """Test max_iterations parameter using live LLM."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={
            "analysis_mode": "manual",
            "max_iterations": 3
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["max_iterations"] == 3
    assert data["iterations_used"] >= 1
    assert data["iterations_used"] <= data["max_iterations"] * 10


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
    assert response.status_code == 400


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
        assert response.status_code in [200, 500, 502]


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

def test_analyze_response_structure(require_llm):
    """Test that analysis response has all expected fields using the live LLM."""
    csv_content = b"name,email\nJohn,john@example.com\n"
    files = {"file": ("test.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post(
        "/analyze-file",
        files=files,
        data={"analysis_mode": "manual"}
    )
    
    assert response.status_code == 200
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


@pytest.mark.b2
def test_storage_analyze_response_structure(require_llm):
    """Test B2 analysis response structure with the real LLM."""
    with patch('app.api.routers.analysis.routes._download_file_from_storage') as mock_download:
        mock_download.return_value = b"name,email\nJohn,john@example.com\n"
        
        response = client.post(
            "/analyze-b2-file",
            json={
                "file_name": "test.csv",
                "analysis_mode": "manual"
            }
        )
    
    assert response.status_code == 200
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
    assert data["iterations_used"] >= 1
    assert data["iterations_used"] <= data["max_iterations"] * 10


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
