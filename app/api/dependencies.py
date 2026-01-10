"""
Shared dependencies, state, and utility functions for the API.

This module contains global state (caches, storage) and helper functions
that are used across multiple routers.
"""
from typing import Dict, Any, Optional
from fastapi import HTTPException, Depends, Query
from sqlalchemy.orm import Session
from app.api.schemas.shared import AsyncTaskStatus, AnalyzeFileResponse
from app.core.security import User, get_current_user
from app.db.session import get_db
from app.db.organization import get_organization_by_id

# Global task storage (in production, use Redis or database)
task_storage: Dict[str, AsyncTaskStatus] = {}
analysis_storage: Dict[str, AnalyzeFileResponse] = {}
interactive_sessions: Dict[str, Any] = {}

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


def get_current_organization(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    override_organization_id: Optional[int] = Query(
        None,
        description="Admin users can override to access other organizations"
    )
) -> int:
    """
    Get the current user's organization ID with admin override capability.
    
    This dependency ensures all data operations are scoped to the user's organization,
    providing multi-tenancy isolation. Admin users can optionally override to access
    other organizations using the override_organization_id query parameter.
    
    Parameters:
    - current_user: Authenticated user from JWT token
    - db: Database session
    - override_organization_id: Optional organization ID for admin override
    
    Returns:
    - organization_id: The organization ID to use for filtering
    
    Raises:
    - HTTPException 400: If user has no organization assigned
    - HTTPException 403: If non-admin user tries to use override
    - HTTPException 404: If override organization doesn't exist
    """
    # Admin override: allow admins to access any organization
    if override_organization_id is not None:
        if current_user.role != "admin":
            raise HTTPException(
                status_code=403,
                detail="Only admin users can override organization context"
            )
        
        # Verify the override organization exists
        org = get_organization_by_id(db, override_organization_id)
        if not org:
            raise HTTPException(
                status_code=404,
                detail=f"Organization {override_organization_id} not found"
            )
        
        return override_organization_id
    
    # Standard case: use user's organization
    if not current_user.organization_id:
        raise HTTPException(
            status_code=400,
            detail="User is not assigned to an organization. Please contact support."
        )
    
    return current_user.organization_id


def get_optional_organization(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
    override_organization_id: Optional[int] = Query(
        None,
        description="Admin users can override to access other organizations"
    )
) -> Optional[int]:
    """
    Get organization ID but return None instead of raising if not set.
    
    Useful for endpoints that optionally filter by organization.
    """
    try:
        return get_current_organization(current_user, db, override_organization_id)
    except HTTPException as e:
        if e.status_code == 400:  # User has no org
            return None
        raise  # Re-raise 403/404 errors
