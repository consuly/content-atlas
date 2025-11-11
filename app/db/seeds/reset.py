"""
Database Reset Utilities for Development

This module provides functions to reset the database for testing purposes.
It preserves user accounts while clearing all data tables and tracking information.

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
    Get list of all user-created tables (excluding system tables).
    
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
                'raster_columns', 'raster_overviews',
                'file_imports', 'table_metadata', 'import_history', 
                'uploaded_files', 'users', 'api_keys', 'mapping_errors', 'import_jobs'
            )
            AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """))
        
        return [row[0] for row in result]


def get_all_b2_files() -> List[Dict[str, Any]]:
    """
    Get list of all files in B2 storage under the uploads folder.
    
    Returns:
        List of file information dictionaries
    """
    try:
        from app.integrations.b2 import get_b2_api
        
        b2_api = get_b2_api()
        bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)
        
        # List all files in the uploads folder
        files = []
        for file_version, _ in bucket.ls(folder_to_list='uploads/', recursive=True):
            files.append({
                'file_name': file_version.file_name,
                'file_id': file_version.id_,
                'size': file_version.size
            })
        
        return files
        
    except ValueError as e:
        # B2 not configured
        logger.warning(f"B2 not configured, skipping file listing: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing B2 files: {e}")
        return []


def delete_all_b2_files() -> Dict[str, Any]:
    """
    Delete all files from B2 storage under the uploads folder.
    
    Returns:
        Dictionary with deletion results
    """
    try:
        from app.integrations.b2 import get_b2_api
        
        b2_api = get_b2_api()
        bucket = b2_api.get_bucket_by_name(settings.b2_bucket_name)
        
        # List and delete all files
        deleted_count = 0
        failed_count = 0
        
        for file_version, _ in bucket.ls(folder_to_list='uploads/', recursive=True):
            try:
                b2_api.delete_file_version(file_version.id_, file_version.file_name)
                deleted_count += 1
                logger.info(f"Deleted B2 file: {file_version.file_name}")
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to delete B2 file {file_version.file_name}: {e}")
        
        return {
            'success': failed_count == 0,
            'deleted_count': deleted_count,
            'failed_count': failed_count
        }
        
    except ValueError as e:
        # B2 not configured
        logger.warning(f"B2 not configured, skipping file deletion: {e}")
        return {
            'success': True,
            'deleted_count': 0,
            'failed_count': 0,
            'message': 'B2 not configured'
        }
    except Exception as e:
        logger.error(f"Error deleting B2 files: {e}")
        return {
            'success': False,
            'deleted_count': 0,
            'failed_count': 0,
            'error': str(e)
        }


def reset_database_data(force_production: bool = False) -> Dict[str, Any]:
    """
    Reset database data while preserving user accounts.
    
    This function:
    1. Drops all user-created tables
    2. Drops tracking tables (file_imports, table_metadata, import_history, import_jobs, uploaded_files)
       - These will be recreated on startup with the latest schema
    3. Deletes all files from B2 storage
    4. Preserves the users table
    
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
            # Get list of user-created tables
            user_tables = get_user_created_tables(engine)
            logger.info(f"Found {len(user_tables)} user-created tables to drop")
            
            # Drop all user-created tables
            for table_name in user_tables:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    results['tables_dropped'].append(table_name)
                    logger.info(f"Dropped table: {table_name}")
                except Exception as e:
                    error_msg = f"Failed to drop table {table_name}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            # Drop all tracking tables (to ensure schema updates are applied)
            # These tables will be recreated by the application on startup with the latest schema
            tracking_tables_to_drop = [
                'uploaded_files',
                'file_imports',
                'table_metadata',
                'import_history',
                'import_jobs'
            ]
            for table_name in tracking_tables_to_drop:
                try:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))
                    results['tables_dropped'].append(table_name)
                    logger.info(f"Dropped tracking table: {table_name} (will be recreated on startup)")
                except Exception as e:
                    error_msg = f"Failed to drop table {table_name}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
        
        # Database operations successful, now clean up B2
        logger.info("Database reset successful, cleaning up B2 files...")
        b2_result = delete_all_b2_files()
        results['b2_files_deleted'] = b2_result.get('deleted_count', 0)
        
        if not b2_result.get('success', False):
            error_msg = f"B2 cleanup had issues: {b2_result.get('error', 'Unknown error')}"
            results['errors'].append(error_msg)
            logger.warning(error_msg)
        
        # Overall success if no critical errors
        results['success'] = len(results['errors']) == 0
        
        logger.info(f"Reset complete: {len(results['tables_dropped'])} tables dropped, "
                   f"{len(results['tables_truncated'])} tables truncated, "
                   f"{results['b2_files_deleted']} B2 files deleted")
        
        return results
        
    except Exception as e:
        error_msg = f"Database reset failed: {e}"
        results['errors'].append(error_msg)
        logger.error(error_msg)
        raise
