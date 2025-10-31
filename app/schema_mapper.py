"""
Schema mapping and transformation for intelligent data merging.

This module handles mapping columns from source files to target table schemas,
enabling merging of semantically similar data with different column structures.
"""

from typing import Dict, List, Any, Optional, Tuple
import re
from difflib import SequenceMatcher
import logging

logger = logging.getLogger(__name__)


def normalize_column_name(name: str) -> str:
    """
    Normalize a column name for comparison.
    
    - Convert to lowercase
    - Replace spaces, hyphens, underscores with nothing
    - Remove special characters
    
    Examples:
        "Contact Full Name" -> "contactfullname"
        "contact_full_name" -> "contactfullname"
        "Contact-Full-Name" -> "contactfullname"
    """
    # Convert to lowercase
    normalized = name.lower()
    # Remove spaces, hyphens, underscores
    normalized = re.sub(r'[\s\-_]+', '', normalized)
    # Remove special characters
    normalized = re.sub(r'[^a-z0-9]', '', normalized)
    return normalized


def calculate_similarity(str1: str, str2: str) -> float:
    """Calculate similarity ratio between two strings (0.0 to 1.0)."""
    return SequenceMatcher(None, str1, str2).ratio()


def find_column_mapping(
    source_columns: List[str],
    target_columns: List[str],
    similarity_threshold: float = 0.6
) -> Dict[str, Optional[str]]:
    """
    Find the best mapping from source columns to target columns.
    
    Uses multiple strategies:
    1. Exact match (case-insensitive)
    2. Normalized match (removing spaces, hyphens, underscores)
    3. Semantic similarity (using common patterns)
    4. String similarity (fuzzy matching)
    
    Args:
        source_columns: Column names from the source file
        target_columns: Column names in the target table
        similarity_threshold: Minimum similarity score for fuzzy matching
        
    Returns:
        Dictionary mapping source column names to target column names.
        If no match found, value is None (indicating new column).
    """
    mapping = {}
    
    # Normalize all target columns for comparison
    normalized_targets = {normalize_column_name(col): col for col in target_columns}
    
    # Common semantic equivalents
    semantic_patterns = {
        # Name variations
        'name': ['fullname', 'contactname', 'personname', 'contactfullname'],
        'fullname': ['name', 'contactname', 'personname', 'contactfullname'],
        'firstname': ['fname', 'givenname'],
        'lastname': ['lname', 'surname', 'familyname'],
        'middlename': ['mname', 'middleinitial'],
        
        # Title/Position variations
        'title': ['jobtitle', 'position', 'role'],
        'jobtitle': ['title', 'position', 'role'],
        'position': ['title', 'jobtitle', 'role'],
        
        # Company variations
        'company': ['companyname', 'organization', 'org', 'business'],
        'companyname': ['company', 'organization', 'org', 'business'],
        'organization': ['company', 'companyname', 'org', 'business'],
        
        # Email variations
        'email': ['emailaddress', 'primaryemail', 'email1', 'contactemail'],
        'emailaddress': ['email', 'primaryemail', 'email1', 'contactemail'],
        'primaryemail': ['email', 'emailaddress', 'email1'],
        
        # Phone variations
        'phone': ['phonenumber', 'telephone', 'mobile', 'cell'],
        'phonenumber': ['phone', 'telephone', 'mobile'],
        
        # LinkedIn variations
        'linkedin': ['linkedinurl', 'linkedinprofile', 'contactliprofileurl'],
        'linkedinprofile': ['linkedin', 'linkedinurl', 'contactliprofileurl'],
        
        # Location variations
        'location': ['address', 'city', 'region', 'area'],
        'city': ['location', 'locality'],
        
        # Industry variations
        'industry': ['industrysector', 'sector', 'vertical'],
        'industrysector': ['industry', 'sector'],
    }
    
    for source_col in source_columns:
        source_normalized = normalize_column_name(source_col)
        best_match = None
        best_score = 0.0
        
        # Strategy 1: Exact normalized match
        if source_normalized in normalized_targets:
            best_match = normalized_targets[source_normalized]
            best_score = 1.0
            logger.debug(f"Exact match: '{source_col}' -> '{best_match}'")
        
        # Strategy 2: Semantic pattern matching
        if not best_match:
            for pattern, equivalents in semantic_patterns.items():
                if source_normalized == pattern:
                    # Check if any equivalent exists in target
                    for equiv in equivalents:
                        if equiv in normalized_targets:
                            best_match = normalized_targets[equiv]
                            best_score = 0.95
                            logger.debug(f"Semantic match: '{source_col}' -> '{best_match}' (via pattern '{pattern}')")
                            break
                    if best_match:
                        break
                elif source_normalized in equivalents:
                    # Check if the pattern exists in target
                    if pattern in normalized_targets:
                        best_match = normalized_targets[pattern]
                        best_score = 0.95
                        logger.debug(f"Semantic match: '{source_col}' -> '{best_match}' (via pattern '{pattern}')")
                        break
        
        # Strategy 3: Fuzzy string matching
        if not best_match:
            for target_col in target_columns:
                target_normalized = normalize_column_name(target_col)
                similarity = calculate_similarity(source_normalized, target_normalized)
                
                if similarity > best_score and similarity >= similarity_threshold:
                    best_match = target_col
                    best_score = similarity
            
            if best_match:
                logger.debug(f"Fuzzy match: '{source_col}' -> '{best_match}' (score: {best_score:.2f})")
        
        # Record the mapping (None if no match found)
        mapping[source_col] = best_match
        if not best_match:
            logger.info(f"No match found for source column '{source_col}' - will be added as new column")
    
    return mapping


def transform_record(
    source_record: Dict[str, Any],
    column_mapping: Dict[str, Optional[str]],
    target_schema: Dict[str, str]
) -> Dict[str, Any]:
    """
    Transform a source record to match the target schema.
    
    Args:
        source_record: Record from source file
        column_mapping: Mapping from source columns to target columns
        target_schema: Target table schema (column_name -> data_type)
        
    Returns:
        Transformed record with target column names ONLY (no source columns)
    """
    transformed = {}
    
    for source_col, value in source_record.items():
        target_col = column_mapping.get(source_col)
        
        if target_col:
            # Map to target column - only include if mapped
            # Avoid duplicates by checking if already set
            if target_col not in transformed:
                transformed[target_col] = value
            else:
                # Multiple source columns map to same target - keep first non-null value
                if transformed[target_col] is None and value is not None:
                    transformed[target_col] = value
        # Note: Unmapped source columns are intentionally ignored
        # They should not appear in the final record
    
    # Fill in missing target columns with None
    for target_col in target_schema.keys():
        if target_col not in transformed:
            transformed[target_col] = None
    
    return transformed


def get_new_columns(
    column_mapping: Dict[str, Optional[str]]
) -> List[str]:
    """
    Get list of source columns that don't map to existing target columns.
    
    These are columns that will need to be added to the target table.
    
    Args:
        column_mapping: Mapping from source columns to target columns
        
    Returns:
        List of source column names that have no target mapping
    """
    return [source_col for source_col, target_col in column_mapping.items() if target_col is None]


def analyze_schema_compatibility(
    source_columns: List[str],
    target_columns: List[str],
    similarity_threshold: float = 0.6
) -> Dict[str, Any]:
    """
    Analyze compatibility between source and target schemas.
    
    Args:
        source_columns: Column names from source file
        target_columns: Column names in target table
        similarity_threshold: Minimum similarity for fuzzy matching
        
    Returns:
        Analysis results including:
        - column_mapping: Dict mapping source to target columns
        - matched_columns: List of successfully mapped columns
        - new_columns: List of columns that need to be added
        - match_percentage: Percentage of source columns that matched
        - compatibility_score: Overall compatibility (0.0 to 1.0)
    """
    column_mapping = find_column_mapping(source_columns, target_columns, similarity_threshold)
    
    matched_columns = [src for src, tgt in column_mapping.items() if tgt is not None]
    new_columns = get_new_columns(column_mapping)
    
    match_percentage = (len(matched_columns) / len(source_columns) * 100) if source_columns else 0
    
    # Calculate compatibility score
    # - High match percentage = good compatibility
    # - Few new columns = good compatibility
    match_score = len(matched_columns) / len(source_columns) if source_columns else 0
    new_col_penalty = len(new_columns) / (len(source_columns) + len(target_columns)) if (source_columns or target_columns) else 0
    compatibility_score = match_score * (1 - new_col_penalty * 0.5)
    
    return {
        "column_mapping": column_mapping,
        "matched_columns": matched_columns,
        "new_columns": new_columns,
        "match_percentage": match_percentage,
        "compatibility_score": compatibility_score,
        "total_source_columns": len(source_columns),
        "total_target_columns": len(target_columns),
        "matched_count": len(matched_columns),
        "new_count": len(new_columns)
    }
