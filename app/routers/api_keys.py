"""
API key management endpoints (Admin only).
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import Optional

from ..database import get_db
from ..auth import get_current_user, User
from ..api_key_auth import create_api_key, list_api_keys, delete_api_key, ApiKey
from ..api_key_schemas import (
    CreateApiKeyRequest, CreateApiKeyResponse, ListApiKeysResponse, ApiKeyInfo,
    RevokeApiKeyResponse, UpdateApiKeyRequest, UpdateApiKeyResponse
)

router = APIRouter(prefix="/admin/api-keys", tags=["api-keys"])


@router.post("", response_model=CreateApiKeyResponse)
async def create_api_key_endpoint(
    request: CreateApiKeyRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Create a new API key (Admin only).
    
    This endpoint generates a new API key for external application access.
    The plain API key is only shown once and cannot be retrieved later.
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - app_name: Name of the application
    - description: Optional description
    - expires_in_days: Optional expiration in days
    - rate_limit_per_minute: Rate limit (default 60)
    - allowed_endpoints: Optional list of allowed endpoint patterns
    
    Returns:
    - The generated API key (only shown once)
    - API key metadata
    """
    try:
        # Create API key
        api_key_record, plain_key = create_api_key(
            db=db,
            app_name=request.app_name,
            description=request.description,
            created_by=current_user.id,
            expires_in_days=request.expires_in_days,
            rate_limit_per_minute=request.rate_limit_per_minute,
            allowed_endpoints=request.allowed_endpoints
        )
        
        return CreateApiKeyResponse(
            success=True,
            message="API key created successfully. Save this key securely - it won't be shown again.",
            api_key=plain_key,
            key_id=api_key_record.id,
            app_name=api_key_record.app_name,
            expires_at=api_key_record.expires_at
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create API key: {str(e)}")


@router.get("", response_model=ListApiKeysResponse)
async def list_api_keys_endpoint(
    is_active: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    List all API keys (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - is_active: Filter by active status
    - limit: Maximum number of keys to return
    - offset: Number of keys to skip
    
    Returns:
    - List of API keys (without the actual key values)
    """
    try:
        api_keys = list_api_keys(
            db=db,
            created_by=None,  # Show all keys for admin
            is_active=is_active,
            limit=limit,
            offset=offset
        )
        
        # Convert to response format with key preview
        api_key_infos = []
        for key in api_keys:
            # Show last 4 characters of the hash as preview
            key_preview = f"...{key.key_hash[-4:]}"
            
            api_key_infos.append(ApiKeyInfo(
                id=key.id,
                app_name=key.app_name,
                description=key.description,
                created_at=key.created_at,
                last_used_at=key.last_used_at,
                expires_at=key.expires_at,
                is_active=key.is_active,
                rate_limit_per_minute=key.rate_limit_per_minute,
                allowed_endpoints=key.allowed_endpoints,
                key_preview=key_preview
            ))
        
        return ListApiKeysResponse(
            success=True,
            api_keys=api_key_infos,
            total_count=len(api_key_infos),
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list API keys: {str(e)}")


@router.delete("/{key_id}", response_model=RevokeApiKeyResponse)
async def delete_api_key_endpoint(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Permanently delete an API key (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - key_id: UUID of the API key to delete
    
    Returns:
    - Success message
    """
    try:
        success = delete_api_key(db, key_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
        
        return RevokeApiKeyResponse(
            success=True,
            message="API key deleted successfully",
            key_id=key_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete API key: {str(e)}")


@router.patch("/{key_id}", response_model=UpdateApiKeyResponse)
async def update_api_key_endpoint(
    key_id: str,
    request: UpdateApiKeyRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Update an API key's settings (Admin only).
    
    Requires: JWT authentication (Bearer token)
    
    Parameters:
    - key_id: UUID of the API key to update
    - description: Optional new description
    - rate_limit_per_minute: Optional new rate limit
    - allowed_endpoints: Optional new allowed endpoints list
    - is_active: Optional active status
    
    Returns:
    - Updated API key metadata
    """
    try:
        # Get API key
        api_key = db.query(ApiKey).filter(ApiKey.id == key_id).first()
        
        if not api_key:
            raise HTTPException(status_code=404, detail=f"API key {key_id} not found")
        
        # Update fields
        if request.description is not None:
            api_key.description = request.description
        if request.rate_limit_per_minute is not None:
            api_key.rate_limit_per_minute = request.rate_limit_per_minute
        if request.allowed_endpoints is not None:
            api_key.allowed_endpoints = request.allowed_endpoints
        if request.is_active is not None:
            api_key.is_active = request.is_active
        
        db.commit()
        db.refresh(api_key)
        
        # Convert to response format
        key_preview = f"...{api_key.key_hash[-4:]}"
        
        api_key_info = ApiKeyInfo(
            id=api_key.id,
            app_name=api_key.app_name,
            description=api_key.description,
            created_at=api_key.created_at,
            last_used_at=api_key.last_used_at,
            expires_at=api_key.expires_at,
            is_active=api_key.is_active,
            rate_limit_per_minute=api_key.rate_limit_per_minute,
            allowed_endpoints=api_key.allowed_endpoints,
            key_preview=key_preview
        )
        
        return UpdateApiKeyResponse(
            success=True,
            message="API key updated successfully",
            api_key=api_key_info
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update API key: {str(e)}")
