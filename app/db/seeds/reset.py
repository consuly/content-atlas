"""
Database Reset Utilities for Development

This module provides functions to reset the database for testing purposes.
It clears all application tables (including user accounts) and tracking information.

SAFETY: This module includes production environment checks to prevent accidental
data loss in production environments.
"""

from sqlalchemy import text
from sqlalchemy.engine import Engine
from typing import List, Dict, Any
import logging

from ..session import get_engine
from app.core.config import settings

logger = logging.getLogger(__name__)


class ProductionEnvironmentError(Exception):
    """Raised when attempting to reset a production database."""
    pass


def is_production_environment() -> bool:
    """
    Check if the current database URL indicates a production environment.
    
    Returns:
        True if production environment detected, False otherwise
    """
    db_url = settings.database_url.lower()
    
    # Check for common production indicators
    production_indicators = [
        'production',
        'prod',
        'amazonaws.com/prod',
        'rds.amazonaws.com',
        'azure.com',
        'cloudapp.net'
    ]
    
    return any(indicator in db_url for indicator in production_indicators)


def get_user_created_tables(engine: Engine) -> List[str]:
    """
    Get list of all application tables (excluding system tables).
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        List of table names
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name NOT IN (
                'spatial_ref_sys', 'geography_columns', 'geometry_columns', 
                'raster_columns', 'raster_overviews'
            )
            AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """))
        
        return [row[0] for row in result]


def get_all_storage_files() -> List[Dict[str, Any]]:
    """
    Get list of all files in storage under the uploads folder.
    
    Returns:
        List of file information dictionaries
    """
    try:
        from app.integrations.storage import list_files
        
        # List all files in the uploads folder
        files = list_files(folder='uploads')
        
        return files
        
    except ValueError as e:
        # Storage not configured
        logger.warning(f"Storage not configured, skipping file listing: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing storage files: {e}")
        return []


def delete_all_storage_files() -> Dict[str, Any]:
    """
    Delete all files from storage under the uploads folder.
    
    Returns:
        Dictionary with deletion results
    """
    try:
        from app.integrations.storage import delete_all_files
        
        # Delete all files in the uploads folder
        result = delete_all_files(folder='uploads')
        
        return result
        
    except ValueError as e:
        # Storage not configured
        logger.warning(f"Storage not configured, skipping file deletion: {e}")
        return {
            'success': True,
            'deleted_count': 0,
            'failed_count': 0,
            'message': 'Storage not configured'
        }
    except Exception as e:
        logger.error(f"Error deleting storage files: {e}")
        return {
            'success': False,
            'deleted_count': 0,
            'failed_count': 0,
            'error': str(e)
        }


def reset_database_data(force_production: bool = False) -> Dict[str, Any]:
    """
    Reset database data, including user accounts.
    
    This function:
    1. Drops all application tables (public schema, excluding system tables)
    2. Deletes all files from B2 storage
    
    Args:
        force_production: If True, allows reset in production (DANGEROUS!)
        
    Returns:
        Dictionary with reset results
        
    Raises:
        ProductionEnvironmentError: If production environment detected and not forced
    """
    # Safety check: prevent production resets
    if is_production_environment() and not force_production:
        raise ProductionEnvironmentError(
            "Cannot reset database in production environment. "
            "If you really want to do this, use force_production=True flag."
        )
    
    engine = get_engine()
    results = {
        'success': False,
        'tables_dropped': [],
        'tables_truncated': [],
        'b2_files_deleted': 0,
        'errors': []
    }
    
    try:
        with engine.begin() as conn:
            # Get list of application tables
            application_tables = get_user_created_tables(engine)
            logger.info(f"Found {len(application_tables)} application tables to drop")
            
            # Drop all application tables
            for table_name in application_tables:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    results['tables_dropped'].append(table_name)
                    logger.info(f"Dropped table: {table_name}")
                except Exception as e:
                    error_msg = f"Failed to drop table {table_name}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
        # Database operations successful, now clean up storage
        logger.info("Database reset successful, cleaning up storage files...")
        storage_result = delete_all_storage_files()
        results['storage_files_deleted'] = storage_result.get('deleted_count', 0)
        
        if not storage_result.get('success', False):
            error_msg = f"Storage cleanup had issues: {storage_result.get('error', 'Unknown error')}"
            results['errors'].append(error_msg)
            logger.warning(error_msg)
        
        # Overall success if no critical errors
        results['success'] = len(results['errors']) == 0
        
        logger.info(f"Reset complete: {len(results['tables_dropped'])} tables dropped, "
                   f"{len(results['tables_truncated'])} tables truncated, "
                   f"{results['storage_files_deleted']} storage files deleted")
        
        return results
        
    except Exception as e:
        error_msg = f"Database reset failed: {e}"
        results['errors'].append(error_msg)
        logger.error(error_msg)
        raise
