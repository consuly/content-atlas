"""
Table metadata management for semantic-aware data consolidation.

This module handles storage and retrieval of table purpose/semantic information
to enable intelligent merging of similar datasets.
"""

from typing import Dict, Any, Optional, List
from sqlalchemy import text
from .session import get_engine
import logging

logger = logging.getLogger(__name__)


def create_table_metadata_table():
    """
    Create the table_metadata table if it doesn't exist.
    
    This table stores semantic information about each table to enable
    intelligent matching and merging of similar datasets.
    """
    engine = get_engine()
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS table_metadata (
        table_name VARCHAR(255) PRIMARY KEY,
        purpose_short VARCHAR(500) NOT NULL,
        purpose_detailed TEXT,
        data_domain VARCHAR(100),
        key_entities TEXT[],
        sample_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_table_metadata_domain ON table_metadata(data_domain);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("table_metadata table created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating table_metadata table: {str(e)}")
        raise


def store_table_metadata(
    table_name: str,
    purpose_short: str,
    purpose_detailed: Optional[str] = None,
    data_domain: Optional[str] = None,
    key_entities: Optional[List[str]] = None,
    sample_data: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Store metadata for a newly created table.
    
    Args:
        table_name: Name of the table
        purpose_short: Brief description of table purpose
        purpose_detailed: Detailed description (optional)
        data_domain: Category/domain (e.g., "contacts", "sales")
        key_entities: List of key entity types in the data
        sample_data: Representative data examples
        
    Returns:
        True if successful, False otherwise
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            # Check if metadata already exists
            check_sql = "SELECT table_name FROM table_metadata WHERE table_name = :table_name"
            result = conn.execute(text(check_sql), {"table_name": table_name})
            
            if result.fetchone():
                logger.info(f"Metadata for table '{table_name}' already exists, skipping")
                return True
            
            # Insert new metadata
            insert_sql = """
            INSERT INTO table_metadata 
                (table_name, purpose_short, purpose_detailed, data_domain, key_entities, sample_data)
            VALUES 
                (:table_name, :purpose_short, :purpose_detailed, :data_domain, :key_entities, :sample_data)
            """
            
            conn.execute(text(insert_sql), {
                "table_name": table_name,
                "purpose_short": purpose_short,
                "purpose_detailed": purpose_detailed,
                "data_domain": data_domain,
                "key_entities": key_entities,
                "sample_data": sample_data
            })
            
        logger.info(f"Stored metadata for table '{table_name}'")
        return True
        
    except Exception as e:
        logger.error(f"Error storing table metadata: {str(e)}")
        return False


def get_table_metadata(table_name: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve metadata for a specific table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with metadata or None if not found
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            sql = "SELECT * FROM table_metadata WHERE table_name = :table_name"
            result = conn.execute(text(sql), {"table_name": table_name})
            row = result.fetchone()
            
            if row:
                return {
                    "table_name": row[0],
                    "purpose_short": row[1],
                    "purpose_detailed": row[2],
                    "data_domain": row[3],
                    "key_entities": row[4],
                    "sample_data": row[5],
                    "created_at": row[6],
                    "updated_at": row[7]
                }
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving table metadata: {str(e)}")
        return None


def get_all_table_metadata() -> Dict[str, Dict[str, Any]]:
    """
    Retrieve metadata for all tables.
    
    Returns:
        Dictionary mapping table names to their metadata
    """
    engine = get_engine()
    
    try:
        with engine.connect() as conn:
            sql = "SELECT * FROM table_metadata ORDER BY created_at DESC"
            result = conn.execute(text(sql))
            
            metadata = {}
            for row in result:
                metadata[row[0]] = {
                    "table_name": row[0],
                    "purpose_short": row[1],
                    "purpose_detailed": row[2],
                    "data_domain": row[3],
                    "key_entities": row[4],
                    "sample_data": row[5],
                    "created_at": row[6],
                    "updated_at": row[7]
                }
            
            return metadata
            
    except Exception as e:
        logger.error(f"Error retrieving all table metadata: {str(e)}")
        return {}


def enrich_table_metadata(
    table_name: str,
    additional_purpose: Optional[str] = None,
    new_entities: Optional[List[str]] = None
) -> bool:
    """
    Enrich existing table metadata with additional information.
    
    This is called when new data is merged into an existing table
    to update the purpose description if needed.
    
    Args:
        table_name: Name of the table
        additional_purpose: Additional purpose text to append
        new_entities: New entity types to add
        
    Returns:
        True if successful, False otherwise
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            # Get current metadata
            current = get_table_metadata(table_name)
            if not current:
                logger.warning(f"No metadata found for table '{table_name}'")
                return False
            
            # Build update
            updates = []
            params = {"table_name": table_name}
            
            if additional_purpose:
                # Append to detailed purpose
                new_detailed = current.get("purpose_detailed", current["purpose_short"])
                new_detailed += f"\n\nAdditional data: {additional_purpose}"
                updates.append("purpose_detailed = :purpose_detailed")
                params["purpose_detailed"] = new_detailed
            
            if new_entities:
                # Merge entity lists
                current_entities = current.get("key_entities", []) or []
                merged_entities = list(set(current_entities + new_entities))
                updates.append("key_entities = :key_entities")
                params["key_entities"] = merged_entities
            
            if updates:
                updates.append("updated_at = NOW()")
                update_sql = f"""
                UPDATE table_metadata 
                SET {', '.join(updates)}
                WHERE table_name = :table_name
                """
                conn.execute(text(update_sql), params)
                logger.info(f"Enriched metadata for table '{table_name}'")
                return True
            
            return True
            
    except Exception as e:
        logger.error(f"Error enriching table metadata: {str(e)}")
        return False


def delete_table_metadata(table_name: str) -> bool:
    """
    Delete metadata for a table (e.g., when table is dropped).
    
    Args:
        table_name: Name of the table
        
    Returns:
        True if successful, False otherwise
    """
    engine = get_engine()
    
    try:
        with engine.begin() as conn:
            sql = "DELETE FROM table_metadata WHERE table_name = :table_name"
            conn.execute(text(sql), {"table_name": table_name})
        logger.info(f"Deleted metadata for table '{table_name}'")
        return True
        
    except Exception as e:
        logger.error(f"Error deleting table metadata: {str(e)}")
        return False
