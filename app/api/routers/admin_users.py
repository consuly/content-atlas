"""
Administrative endpoints for managing application users.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.core.security import (
    User,
    create_user,
    delete_user,
    require_admin,
    set_user_password,
)
from app.api.schemas.auth import UserResponse
from app.api.schemas.users import (
    AdminCreateUserRequest,
    AdminCreateUserResponse,
    AdminDeleteUserResponse,
    AdminListUsersResponse,
    AdminSetPasswordRequest,
    AdminSetPasswordResponse,
)

router = APIRouter(prefix="/admin/users", tags=["admin-users"])


@router.get("", response_model=AdminListUsersResponse)
async def list_users(
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    List all users in the system. Admin access only.
    """
    users = db.query(User).order_by(User.created_at.desc()).all()
    return AdminListUsersResponse(
        success=True,
        users=[UserResponse.model_validate(user) for user in users],
    )


@router.post("", response_model=AdminCreateUserResponse)
async def create_user_admin(
    request: AdminCreateUserRequest,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Create a user (admin or standard) via the admin console.
    """
    user = create_user(
        db=db,
        email=request.email,
        password=request.password,
        full_name=request.full_name,
        role=request.role,
    )

    return AdminCreateUserResponse(
        success=True,
        user=UserResponse.model_validate(user),
    )


@router.patch("/{user_id}/password", response_model=AdminSetPasswordResponse)
async def set_user_password_admin(
    user_id: int,
    request: AdminSetPasswordRequest,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Reset a user's password. Admin access only.
    """
    updated_user = set_user_password(db=db, user_id=user_id, new_password=request.password)
    return AdminSetPasswordResponse(
        success=True,
        user=UserResponse.model_validate(updated_user),
    )


@router.delete("/{user_id}", response_model=AdminDeleteUserResponse)
async def delete_user_admin(
    user_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    """
    Delete a user. Admin access only.
    """
    if user_id == current_user.id:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete the currently authenticated admin user",
        )

    deleted = delete_user(db=db, user_id=user_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    return AdminDeleteUserResponse(success=True, deleted_user_id=user_id)
