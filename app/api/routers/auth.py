"""
Authentication endpoints for user registration and login.
"""
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.api.schemas.auth import (
    AuthResponse,
    BootstrapStatusResponse,
    Token,
    UserLogin,
    UserRegister,
    UserResponse,
)
from app.core.security import (
    User,
    authenticate_user,
    create_access_token,
    create_user,
    get_current_user,
)
from app.db.session import get_db

router = APIRouter(prefix="/auth", tags=["authentication"])


def _requires_admin_setup(db: Session) -> bool:
    """Return True when no users exist yet."""
    existing_users = db.query(func.count(User.id)).scalar() or 0
    return existing_users == 0


@router.post("/register", response_model=AuthResponse)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    """
    Register a new user.
    
    Parameters:
    - email: User's email address
    - password: User's password (will be hashed)
    - full_name: Optional full name
    
    Returns:
    - JWT access token
    - User information
    """
    try:
        # First user becomes an admin so new deployments require manual setup
        role = "admin" if _requires_admin_setup(db) else "user"

        # Create user
        user = create_user(
            db=db,
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name,
            role=role,
        )
        
        # Generate JWT token
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=timedelta(minutes=60 * 24)  # 24 hours
        )
        
        return AuthResponse(
            success=True,
            token=Token(access_token=access_token),
            user=UserResponse.model_validate(user)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")


@router.post("/login", response_model=AuthResponse)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """
    Login with email and password.
    
    Parameters:
    - email: User's email address
    - password: User's password
    
    Returns:
    - JWT access token
    - User information
    """
    try:
        if _requires_admin_setup(db):
            raise HTTPException(
                status_code=400,
                detail="No users exist yet. Please create the first account to continue.",
            )

        # Authenticate user
        user = authenticate_user(db, credentials.email, credentials.password)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="Incorrect email or password"
            )
        
        # Generate JWT token
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=timedelta(minutes=60 * 24)  # 24 hours
        )
        
        return AuthResponse(
            success=True,
            token=Token(access_token=access_token),
            user=UserResponse.model_validate(user)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """
    Get current authenticated user information.
    
    Requires: Bearer token in Authorization header
    
    Returns:
    - User information
    """
    return UserResponse.model_validate(current_user)


@router.get("/bootstrap-status", response_model=BootstrapStatusResponse)
async def get_bootstrap_status(db: Session = Depends(get_db)):
    """
    Return whether the deployment still needs an initial admin account.
    """
    return BootstrapStatusResponse(requires_admin_setup=_requires_admin_setup(db))
