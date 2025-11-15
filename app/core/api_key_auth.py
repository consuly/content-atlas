"""
API Key Authentication module for Content Atlas.
Provides API key-based authentication for external applications.
"""
import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, JSON
from app.db.session import Base, get_db

def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


# Security scheme for API key in header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class ApiKey(Base):
    """API Key model for application authentication."""
    __tablename__ = "api_keys"

    id = Column(String, primary_key=True)  # UUID
    key_hash = Column(String, unique=True, index=True, nullable=False)
    app_name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    created_by = Column(Integer, nullable=True)  # FK to users.id
    created_at = Column(DateTime, default=_utcnow)
    last_used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    rate_limit_per_minute = Column(Integer, default=60)
    allowed_endpoints = Column(JSON, nullable=True)  # List of allowed endpoint patterns


def generate_api_key() -> str:
    """
    Generate a secure API key with format:
    atlas_live_sk_<32_random_chars>
    
    Returns:
        Formatted API key string
    """
    # Generate 32 random characters (hex)
    random_part = secrets.token_hex(16)  # 16 bytes = 32 hex chars
    
    # Format: atlas_live_sk_<random>
    api_key = f"atlas_live_sk_{random_part}"
    
    return api_key


def hash_api_key(api_key: str) -> str:
    """
    Hash an API key for secure storage.
    Uses SHA-256 for fast lookups.
    
    Args:
        api_key: The plain API key to hash
        
    Returns:
        Hexadecimal hash string
    """
    return hashlib.sha256(api_key.encode()).hexdigest()


def verify_api_key(db: Session, api_key: str) -> Optional[ApiKey]:
    """
    Verify an API key and return the associated ApiKey record.
    
    Args:
        db: Database session
        api_key: Plain API key to verify
        
    Returns:
        ApiKey record if valid, None otherwise
    """
    # Hash the provided key
    key_hash = hash_api_key(api_key)
    
    # Look up in database
    api_key_record = db.query(ApiKey).filter(
        ApiKey.key_hash == key_hash,
        ApiKey.is_active == True
    ).first()
    
    if not api_key_record:
        return None
    
    # Check expiration
    if api_key_record.expires_at and api_key_record.expires_at < _utcnow():
        return None
    
    return api_key_record


def update_last_used(db: Session, api_key_id: str):
    """
    Update the last_used_at timestamp for an API key.
    
    Args:
        db: Database session
        api_key_id: ID of the API key to update
    """
    db.query(ApiKey).filter(ApiKey.id == api_key_id).update({
        "last_used_at": _utcnow()
    })
    db.commit()


def get_api_key_from_header(
    api_key: Optional[str] = Depends(api_key_header),
    db: Session = Depends(get_db)
) -> ApiKey:
    """
    Dependency to extract and validate API key from request header.
    
    Args:
        api_key: API key from X-API-Key header
        db: Database session
        
    Returns:
        Validated ApiKey record
        
    Raises:
        HTTPException: If API key is missing or invalid
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required. Provide X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Verify API key
    api_key_record = verify_api_key(db, api_key)
    
    if not api_key_record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Update last used timestamp (async in background would be better)
    update_last_used(db, api_key_record.id)
    
    return api_key_record


def create_api_key(
    db: Session,
    app_name: str,
    description: Optional[str] = None,
    created_by: Optional[int] = None,
    expires_in_days: Optional[int] = None,
    rate_limit_per_minute: int = 60,
    allowed_endpoints: Optional[list] = None
) -> tuple[ApiKey, str]:
    """
    Create a new API key.
    
    Args:
        db: Database session
        app_name: Name of the application
        description: Optional description
        created_by: User ID who created the key
        expires_in_days: Optional expiration in days
        rate_limit_per_minute: Rate limit (default 60)
        allowed_endpoints: Optional list of allowed endpoint patterns
        
    Returns:
        Tuple of (ApiKey record, plain API key string)
        Note: The plain key is only returned once and never stored
    """
    import uuid
    
    # Generate API key
    plain_key = generate_api_key()
    key_hash = hash_api_key(plain_key)
    
    # Calculate expiration
    expires_at = None
    if expires_in_days:
        expires_at = _utcnow() + timedelta(days=expires_in_days)
    
    # Create record
    api_key_record = ApiKey(
        id=str(uuid.uuid4()),
        key_hash=key_hash,
        app_name=app_name,
        description=description,
        created_by=created_by,
        expires_at=expires_at,
        rate_limit_per_minute=rate_limit_per_minute,
        allowed_endpoints=allowed_endpoints
    )
    
    db.add(api_key_record)
    db.commit()
    db.refresh(api_key_record)
    
    return api_key_record, plain_key


def revoke_api_key(db: Session, api_key_id: str) -> bool:
    """
    Revoke (deactivate) an API key.
    
    Args:
        db: Database session
        api_key_id: ID of the API key to revoke
        
    Returns:
        True if revoked, False if not found
    """
    result = db.query(ApiKey).filter(ApiKey.id == api_key_id).update({
        "is_active": False
    })
    db.commit()
    
    return result > 0


def delete_api_key(db: Session, api_key_id: str) -> bool:
    """
    Permanently delete an API key from the database.
    
    Args:
        db: Database session
        api_key_id: ID of the API key to delete
        
    Returns:
        True if deleted, False if not found
    """
    api_key = db.query(ApiKey).filter(ApiKey.id == api_key_id).first()
    
    if not api_key:
        return False
    
    db.delete(api_key)
    db.commit()
    
    return True


def list_api_keys(
    db: Session,
    created_by: Optional[int] = None,
    is_active: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0
) -> list[ApiKey]:
    """
    List API keys with optional filters.
    
    Args:
        db: Database session
        created_by: Filter by creator user ID
        is_active: Filter by active status
        limit: Maximum records to return
        offset: Number of records to skip
        
    Returns:
        List of ApiKey records
    """
    query = db.query(ApiKey)
    
    if created_by is not None:
        query = query.filter(ApiKey.created_by == created_by)
    
    if is_active is not None:
        query = query.filter(ApiKey.is_active == is_active)
    
    query = query.order_by(ApiKey.created_at.desc())
    query = query.limit(limit).offset(offset)
    
    return query.all()


def init_api_key_tables():
    """Initialize API key tables."""
    from app.db.session import get_engine
    Base.metadata.create_all(bind=get_engine(), tables=[ApiKey.__table__])
