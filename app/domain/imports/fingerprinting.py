
import hashlib
import json
import logging
import re
from typing import List, Optional, Tuple, Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

def normalize_column_name(name: str) -> str:
    """Normalize column name: lowercase, alphanumeric only."""
    if not name:
        return ""
    # Keep only alphanumeric characters and lowercase
    return re.sub(r'[^a-z0-9]', '', str(name).lower())

def calculate_fingerprint(columns: List[str]) -> Tuple[str, List[str]]:
    """
    Calculate a deterministic fingerprint for a list of columns.
    Returns (fingerprint_hash, normalized_sorted_columns).
    """
    normalized = [normalize_column_name(c) for c in columns if c]
    normalized = [n for n in normalized if n]  # Remove empty strings
    normalized.sort()  # Sort to ensure order independence
    
    # Create hash from sorted list
    content = "|".join(normalized)
    fingerprint_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    return fingerprint_hash, normalized

def store_table_fingerprint(engine: Engine, table_name: str, columns: List[str]) -> None:
    """
    Store or update the schema fingerprint for a table.
    """
    if not table_name or not columns:
        return
        
    try:
        fingerprint_hash, normalized_columns = calculate_fingerprint(columns)
        
        upsert_sql = """
        INSERT INTO table_fingerprints (table_name, column_names, fingerprint_hash, updated_at)
        VALUES (:table_name, :column_names, :fingerprint_hash, CURRENT_TIMESTAMP)
        ON CONFLICT (table_name) DO UPDATE
        SET column_names = EXCLUDED.column_names,
            fingerprint_hash = EXCLUDED.fingerprint_hash,
            updated_at = CURRENT_TIMESTAMP
        """
        
        with engine.begin() as conn:
            conn.execute(text(upsert_sql), {
                "table_name": table_name,
                "column_names": json.dumps(normalized_columns),
                "fingerprint_hash": fingerprint_hash
            })
            
        logger.info(f"Stored fingerprint for table '{table_name}' (hash: {fingerprint_hash[:8]})")
        
    except Exception as e:
        logger.warning(f"Failed to store table fingerprint for '{table_name}': {e}")

def calculate_jaccard_similarity(set1: set, set2: set) -> float:
    """Calculate Jaccard similarity between two sets."""
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0
        
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    return intersection / union if union > 0 else 0.0

def find_matching_fingerprint(engine: Engine, columns: List[str], threshold: float = 0.9) -> Optional[Dict[str, Any]]:
    """
    Find an existing table with a matching schema fingerprint.
    
    Strategies:
    1. Exact match (hash lookup)
    2. Loose match (Jaccard similarity > threshold)
    
    Returns:
        Dict with keys: 'table_name', 'similarity', 'match_type' ('exact' or 'loose')
        or None if no match found.
    """
    if not columns:
        return None
        
    target_hash, target_normalized = calculate_fingerprint(columns)
    target_set = set(target_normalized)
    
    try:
        with engine.connect() as conn:
            # 1. Try exact match
            result = conn.execute(text("""
                SELECT table_name, column_names 
                FROM table_fingerprints 
                WHERE fingerprint_hash = :hash
            """), {"hash": target_hash})
            
            exact_match = result.fetchone()
            if exact_match:
                return {
                    "table_name": exact_match[0],
                    "similarity": 1.0,
                    "match_type": "exact"
                }
            
            # 2. Scan for loose match
            # If we didn't find exact match, we need to compare against all fingerprints
            # Since number of tables is usually small (<1000), this linear scan is acceptable
            result = conn.execute(text("SELECT table_name, column_names FROM table_fingerprints"))
            
            best_match = None
            best_score = 0.0
            
            for row in result:
                table_name = row[0]
                stored_columns = json.loads(row[1]) if isinstance(row[1], str) else row[1]
                stored_set = set(stored_columns)
                
                similarity = calculate_jaccard_similarity(target_set, stored_set)
                
                if similarity > best_score:
                    best_score = similarity
                    best_match = table_name
            
            if best_match and best_score >= threshold:
                return {
                    "table_name": best_match,
                    "similarity": best_score,
                    "match_type": "loose"
                }
                
    except Exception as e:
        logger.warning(f"Error finding matching fingerprint: {e}")
        
    return None
