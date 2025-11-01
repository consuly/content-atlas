"""
Authentication endpoints for user registration and login.
"""
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import timedelta

from app.db.session import get_db
from app.core.security import (
    authenticate_user, create_access_token, get_current_user,
    create_user, User
)
from app.api.schemas.auth import UserLogin, UserRegister, AuthResponse, Token, UserResponse

router = APIRouter(prefix="/auth", tags=["authentication"])


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
        # Create user
        user = create_user(
            db=db,
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name
        )
        
        # Generate JWT token
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=timedelta(minutes=60 * 24)  # 24 hours
        )
        
        return AuthResponse(
            success=True,
            token=Token(access_token=access_token),
            user=UserResponse.from_orm(user)
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
            user=UserResponse.from_orm(user)
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
    return UserResponse.from_orm(current_user)
