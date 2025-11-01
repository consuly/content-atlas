"""
Pydantic schemas for API Key management.
"""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class CreateApiKeyRequest(BaseModel):
    """Request schema for creating a new API key."""
    app_name: str = Field(..., description="Name of the application")
    description: Optional[str] = Field(None, description="Optional description of the API key usage")
    expires_in_days: Optional[int] = Field(None, description="Optional expiration in days", ge=1, le=3650)
    rate_limit_per_minute: int = Field(60, description="Rate limit per minute", ge=1, le=1000)
    allowed_endpoints: Optional[List[str]] = Field(None, description="Optional list of allowed endpoint patterns")


class CreateApiKeyResponse(BaseModel):
    """Response schema for API key creation."""
    success: bool
    message: str
    api_key: str = Field(..., description="The generated API key (only shown once)")
    key_id: str = Field(..., description="The API key ID for management")
    app_name: str
    expires_at: Optional[datetime] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "API key created successfully",
                "api_key": "atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
                "key_id": "550e8400-e29b-41d4-a716-446655440000",
                "app_name": "Mobile App",
                "expires_at": "2025-12-31T23:59:59"
            }
        }


class ApiKeyInfo(BaseModel):
    """Information about an API key (without the actual key)."""
    id: str
    app_name: str
    description: Optional[str] = None
    created_at: datetime
    last_used_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    is_active: bool
    rate_limit_per_minute: int
    allowed_endpoints: Optional[List[str]] = None
    key_preview: str = Field(..., description="Last 4 characters of the key for identification")
    
    class Config:
        from_attributes = True


class ListApiKeysResponse(BaseModel):
    """Response schema for listing API keys."""
    success: bool
    api_keys: List[ApiKeyInfo]
    total_count: int
    limit: int
    offset: int


class RevokeApiKeyResponse(BaseModel):
    """Response schema for revoking an API key."""
    success: bool
    message: str
    key_id: str


class UpdateApiKeyRequest(BaseModel):
    """Request schema for updating an API key."""
    description: Optional[str] = None
    rate_limit_per_minute: Optional[int] = Field(None, ge=1, le=1000)
    allowed_endpoints: Optional[List[str]] = None
    is_active: Optional[bool] = None


class UpdateApiKeyResponse(BaseModel):
    """Response schema for updating an API key."""
    success: bool
    message: str
    api_key: ApiKeyInfo
