"""
Integration tests for LLM column selection in multi-value scenarios.

These tests verify that the LLM correctly interprets user instructions
and selects the appropriate columns for explode_columns transformations.
"""

import pytest
from pathlib import Path
from typing import Dict, Any, List

from app.domain.queries.analyzer import analyze_file_for_import
from app.api.schemas.shared import AnalysisMode, ConflictResolutionMode
from app.domain.imports.processors.csv_processor import process_csv


FIXTURE_DIR = Path("tests/csv")


def _create_test_csv_with_multiple_email_columns() -> bytes:
    """
    Create a test CSV with both named and numbered email columns.
    This simulates the real-world scenario where users have:
    - Named columns: "Primary Email", "Personal Email"
    - Numbered columns: "Email 1", "Email 2"
    """
    csv_content = """First Name,Last Name,Primary Email,Personal Email,Email 1,Email 2,Phone
John,Doe,john.primary@example.com,john.personal@example.com,john1@example.com,john2@example.com,555-1234
Jane,Smith,jane.primary@example.com,jane.personal@example.com,jane1@example.com,jane2@example.com,555-5678
Bob,Johnson,bob.primary@example.com,bob.personal@example.com,bob1@example.com,bob2@example.com,555-9012
"""
    return csv_content.encode('utf-8')


def _create_test_csv_with_duplicate_emails() -> bytes:
    """
    Create a test CSV where the same email appears in multiple columns.
    This tests deduplication logic.
    """
    csv_content = """First Name,Last Name,Primary Email,Personal Email,Phone
John,Doe,john@example.com,john@example.com,555-1234
Jane,Smith,jane@example.com,jane.work@example.com,555-5678
Bob,Johnson,bob@example.com,bob@example.com,555-9012
"""
    return csv_content.encode('utf-8')


def _extract_explode_columns_transformation(llm_decision: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the explode_columns transformation from LLM decision."""
    row_transformations = llm_decision.get("row_transformations", [])
    for transform in row_transformations:
        if isinstance(transform, dict) and transform.get("type") == "explode_columns":
            return transform
    return {}


def _extract_source_columns(llm_decision: Dict[str, Any]) -> List[str]:
    """Extract source_columns from explode_columns transformation."""
    transform = _extract_explode_columns_transformation(llm_decision)
    return transform.get("source_columns", [])


@pytest.mark.integration
def test_llm_selects_named_columns_when_user_mentions_primary_and_personal():
    """
    Test that LLM selects "Primary Email" and "Personal Email" when user says
    "keep primary and personal email", NOT "Email 1" and "Email 2".
    
    This is the core bug we're fixing.
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    # User instruction explicitly mentions "primary" and "personal"
    user_instruction = (
        "Keep only the primary email and personal email. "
        "Create only one column email in the new table. "
        "If the row has two different email addresses, create a new entry per unique email."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    source_columns = _extract_source_columns(llm_decision)
    
    # CRITICAL ASSERTION: Should select named columns, not numbered
    assert "Primary Email" in source_columns, (
        f"Expected 'Primary Email' in source_columns, got: {source_columns}"
    )
    assert "Personal Email" in source_columns, (
        f"Expected 'Personal Email' in source_columns, got: {source_columns}"
    )
    
    # Should NOT include numbered columns
    assert "Email 1" not in source_columns, (
        f"Should NOT include 'Email 1' when user said 'primary email', got: {source_columns}"
    )
    assert "Email 2" not in source_columns, (
        f"Should NOT include 'Email 2' when user said 'personal email', got: {source_columns}"
    )
    
    # Should have exactly 2 source columns
    assert len(source_columns) == 2, (
        f"Expected exactly 2 source columns (Primary Email, Personal Email), got {len(source_columns)}: {source_columns}"
    )


@pytest.mark.integration
def test_llm_selects_numbered_columns_when_user_explicitly_mentions_them():
    """
    Test that LLM selects "Email 1" and "Email 2" when user explicitly says
    "keep email 1 and email 2", NOT "Primary Email" and "Personal Email".
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    # User instruction explicitly mentions numbered columns
    user_instruction = (
        "Keep only email 1 and email 2. "
        "Create only one column email in the new table. "
        "If the row has two different email addresses, create a new entry per unique email."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    source_columns = _extract_source_columns(llm_decision)
    
    # Should select numbered columns as explicitly requested
    assert "Email 1" in source_columns, (
        f"Expected 'Email 1' in source_columns, got: {source_columns}"
    )
    assert "Email 2" in source_columns, (
        f"Expected 'Email 2' in source_columns, got: {source_columns}"
    )
    
    # Should NOT include named columns
    assert "Primary Email" not in source_columns, (
        f"Should NOT include 'Primary Email' when user said 'email 1', got: {source_columns}"
    )
    assert "Personal Email" not in source_columns, (
        f"Should NOT include 'Personal Email' when user said 'email 2', got: {source_columns}"
    )
    
    # Should have exactly 2 source columns
    assert len(source_columns) == 2, (
        f"Expected exactly 2 source columns (Email 1, Email 2), got {len(source_columns)}: {source_columns}"
    )


@pytest.mark.integration
def test_llm_enables_deduplication_for_explode_columns():
    """
    Test that LLM enables deduplication when creating explode_columns transformation.
    This ensures duplicate emails are removed.
    """
    csv_content = _create_test_csv_with_duplicate_emails()
    records = process_csv(csv_content)
    
    user_instruction = (
        "Keep only the primary email and personal email. "
        "Create only one column email in the new table. "
        "If the row has two different email addresses, create a new entry per unique email."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    transform = _extract_explode_columns_transformation(llm_decision)
    
    # Should enable deduplication
    assert transform.get("dedupe_values", False) is True, (
        "dedupe_values should be True to prevent duplicate emails"
    )
    assert transform.get("case_insensitive_dedupe", False) is True, (
        "case_insensitive_dedupe should be True for email deduplication"
    )


@pytest.mark.integration
def test_llm_creates_single_target_column_for_multiple_sources():
    """
    Test that LLM creates a single target column when user says
    "create only one column email".
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    user_instruction = (
        "Keep only the primary email and personal email. "
        "Create only one column email in the new table. "
        "If the row has two different email addresses, create a new entry per unique email."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    transform = _extract_explode_columns_transformation(llm_decision)
    
    # Should have a single target column
    target_column = transform.get("target_column")
    assert target_column is not None, "Should have a target_column"
    assert target_column.lower() == "email", (
        f"Target column should be 'email', got: {target_column}"
    )
    
    # Column mapping should map both sources to the same target
    column_mapping = llm_decision.get("column_mapping", {})
    
    # After explode_columns, the mapping should point to the exploded target
    assert "email" in column_mapping.values() or "email" in column_mapping.keys(), (
        f"Column mapping should include 'email', got: {column_mapping}"
    )


@pytest.mark.integration
def test_llm_respects_user_column_names_case_insensitive():
    """
    Test that LLM matches column names case-insensitively.
    User says "primary email" (lowercase) but column is "Primary Email" (title case).
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    # User instruction uses lowercase and is explicit about row explosion
    user_instruction = (
        "Keep only the primary email and personal email. "
        "Create only one column email in the new table. "
        "Use explode_columns to create a separate row for each email address."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    source_columns = _extract_source_columns(llm_decision)
    
    # Should match case-insensitively
    assert "Primary Email" in source_columns, (
        f"Should match 'primary email' to 'Primary Email', got: {source_columns}"
    )
    assert "Personal Email" in source_columns, (
        f"Should match 'personal email' to 'Personal Email', got: {source_columns}"
    )


@pytest.mark.integration
def test_llm_does_not_auto_expand_to_all_email_columns():
    """
    Test that LLM does NOT automatically include all email columns
    when user only mentions specific ones.
    
    This prevents the bug where LLM includes both named AND numbered columns.
    
    When only ONE column is requested, the LLM should use direct mapping
    (not explode_columns), so we check column_mapping instead.
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    # User only mentions "primary email"
    user_instruction = (
        "Keep only the primary email. "
        "Create only one column email in the new table."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    column_mapping = llm_decision.get("column_mapping", {})
    
    # When only one column is requested, LLM should use direct mapping
    # Check that "Primary Email" is mapped (either as key or value)
    mapping_keys = list(column_mapping.keys())
    mapping_values = list(column_mapping.values())
    
    # Should map Primary Email to email
    assert "Primary Email" in mapping_keys or "Primary Email" in mapping_values, (
        f"Expected 'Primary Email' in column_mapping, got: {column_mapping}"
    )
    
    # Should NOT include other email columns in the mapping
    assert "Personal Email" not in mapping_keys and "Personal Email" not in mapping_values, (
        f"Should NOT include 'Personal Email' in mapping, got: {column_mapping}"
    )
    assert "Email 1" not in mapping_keys and "Email 1" not in mapping_values, (
        f"Should NOT include 'Email 1' in mapping, got: {column_mapping}"
    )
    assert "Email 2" not in mapping_keys and "Email 2" not in mapping_values, (
        f"Should NOT include 'Email 2' in mapping, got: {column_mapping}"
    )
    
    # Verify no explode_columns transformation was created (not needed for single column)
    source_columns = _extract_source_columns(llm_decision)
    # It's OK if explode_columns exists with just one column, or if it doesn't exist at all
    if source_columns:
        assert len(source_columns) <= 1, (
            f"For single column request, should have at most 1 source column, got {len(source_columns)}: {source_columns}"
        )


@pytest.mark.integration
def test_llm_column_mapping_consistency():
    """
    Test that column_mapping is consistent with row_transformations.
    
    When explode_columns is used, the column_mapping should reflect
    the exploded target column.
    """
    csv_content = _create_test_csv_with_multiple_email_columns()
    records = process_csv(csv_content)
    
    user_instruction = (
        "Keep only the primary email and personal email. "
        "Create only one column email in the new table. "
        "Use explode_columns to create a separate row for each email address."
    )
    
    file_metadata = {
        "name": "test_contacts.csv",
        "total_rows": len(records),
        "file_type": "csv"
    }
    
    result = analyze_file_for_import(
        file_sample=records[:10],
        file_metadata=file_metadata,
        analysis_mode=AnalysisMode.MANUAL,
        conflict_mode=ConflictResolutionMode.LLM_DECIDE,
        llm_instruction=user_instruction,
        max_iterations=10
    )
    
    assert result["success"], f"Analysis failed: {result.get('error')}"
    assert result["llm_decision"] is not None, "LLM should have made a decision"
    
    llm_decision = result["llm_decision"]
    column_mapping = llm_decision.get("column_mapping", {})
    transform = _extract_explode_columns_transformation(llm_decision)
    target_column = transform.get("target_column")
    
    # The target column from explode_columns should appear in column_mapping
    # Either as a key (target->source) or value (source->target)
    mapping_values = list(column_mapping.values())
    mapping_keys = list(column_mapping.keys())
    
    assert target_column in mapping_values or target_column in mapping_keys, (
        f"Target column '{target_column}' should appear in column_mapping. "
        f"Got mapping: {column_mapping}"
    )


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
