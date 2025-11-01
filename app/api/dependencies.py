"""
Shared dependencies, state, and utility functions for the API.

This module contains global state (caches, storage) and helper functions
that are used across multiple routers.
"""
from typing import Dict, Any
from fastapi import HTTPException
from app.api.schemas.shared import AsyncTaskStatus, AnalyzeFileResponse

# Global task storage (in production, use Redis or database)
task_storage: Dict[str, AsyncTaskStatus] = {}
analysis_storage: Dict[str, AnalyzeFileResponse] = {}

# Cache for parsed file records to avoid double processing
# Key: file_hash, Value: dict with 'raw_records', 'mapped_records', 'config_hash', 'timestamp'
records_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL_SECONDS = 300  # 5 minutes


def detect_file_type(filename: str) -> str:
    """
    Detect file type from filename extension.
    
    Parameters:
    - filename: Name of the file
    
    Returns:
    - File type: 'csv', 'excel', 'json', or 'xml'
    
    Raises:
    - HTTPException: If file type is not supported
    """
    if filename.endswith('.csv'):
        return 'csv'
    elif filename.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif filename.endswith('.json'):
        return 'json'
    elif filename.endswith('.xml'):
        return 'xml'
    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")
