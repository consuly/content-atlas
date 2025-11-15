"""
Pydantic schemas for authentication endpoints.
"""
from pydantic import BaseModel, EmailStr, ConfigDict
from typing import Optional
from datetime import datetime


class UserLogin(BaseModel):
    """Login request schema."""
    email: EmailStr
    password: str


class UserRegister(BaseModel):
    """User registration schema."""
    email: EmailStr
    password: str
    full_name: Optional[str] = None
    # Role is controlled by admins; registration always creates a standard user


class Token(BaseModel):
    """JWT token response."""
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    """User information response."""
    id: int
    email: str
    full_name: Optional[str] = None
    created_at: datetime
    is_active: bool
    role: str

    model_config = ConfigDict(from_attributes=True)


class AuthResponse(BaseModel):
    """Authentication response with token and user info."""
    success: bool
    token: Token
    user: UserResponse
