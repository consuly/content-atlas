"""
Pydantic schemas for organization-related requests and responses.
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class OrganizationBase(BaseModel):
    """Base organization schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Organization name")


class OrganizationCreate(OrganizationBase):
    """Schema for creating an organization."""
    slug: Optional[str] = Field(None, max_length=100, description="Custom URL-friendly slug (auto-generated if not provided)")


class OrganizationResponse(OrganizationBase):
    """Schema for organization response."""
    id: int
    slug: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OrganizationUpdate(BaseModel):
    """Schema for updating an organization."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
