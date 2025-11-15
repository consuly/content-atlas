"""
Schemas for administrative user management endpoints.
"""
from typing import List, Literal, Optional

from pydantic import BaseModel, EmailStr

from app.api.schemas.auth import UserResponse


class AdminCreateUserRequest(BaseModel):
    """Payload for creating a user as an administrator."""

    email: EmailStr
    password: str
    full_name: Optional[str] = None
    role: Literal["admin", "user"] = "user"


class AdminCreateUserResponse(BaseModel):
    """Response returned when a user is created by an admin."""

    success: bool
    user: UserResponse


class AdminListUsersResponse(BaseModel):
    """Response with the current set of users."""

    success: bool
    users: List[UserResponse]


class AdminSetPasswordRequest(BaseModel):
    """Payload for resetting a user's password."""

    password: str


class AdminSetPasswordResponse(BaseModel):
    """Response returned after updating a user's password."""

    success: bool
    user: UserResponse


class AdminDeleteUserResponse(BaseModel):
    """Response returned when deleting a user."""

    success: bool
    deleted_user_id: int
