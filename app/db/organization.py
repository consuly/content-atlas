"""
Organization model and helper functions for multi-tenancy.
"""
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, text
from sqlalchemy.orm import Session, relationship
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from app.db.session import Base, get_engine
import re
import logging

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class Organization(Base):
    """Organization model for multi-tenancy."""
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(100), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)

    # Relationships
    # users = relationship("User", back_populates="organization")


def generate_organization_slug(name: str) -> str:
    """
    Generate a URL-friendly slug from organization name.
    
    Args:
        name: Organization name
        
    Returns:
        Slugified version of the name
    """
    # Convert to lowercase and replace spaces/special chars with hyphens
    slug = name.lower()
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[-\s]+', '-', slug)
    slug = slug.strip('-')
    
    # Ensure it's not empty
    if not slug:
        slug = "organization"
    
    # Limit length
    slug = slug[:100]
    
    return slug


def create_organization(
    db: Session,
    name: str,
    slug: Optional[str] = None
) -> Organization:
    """
    Create a new organization.
    
    Args:
        db: Database session
        name: Organization name
        slug: Optional custom slug (auto-generated if not provided)
        
    Returns:
        Created Organization instance
    """
    if not slug:
        slug = generate_organization_slug(name)
    
    # Ensure slug is unique by appending number if needed
    base_slug = slug
    counter = 1
    while True:
        existing = db.query(Organization).filter(Organization.slug == slug).first()
        if not existing:
            break
        counter += 1
        slug = f"{base_slug}-{counter}"
    
    try:
        org = Organization(name=name, slug=slug)
        db.add(org)
        db.commit()
        db.refresh(org)
        logger.info(f"Created organization: {name} (slug: {slug})")
        return org
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Failed to create organization: {e}")
        raise HTTPException(
            status_code=400,
            detail="Organization with this slug already exists"
        )


def get_organization_by_id(db: Session, organization_id: int) -> Optional[Organization]:
    """Get organization by ID."""
    return db.query(Organization).filter(Organization.id == organization_id).first()


def get_organization_by_slug(db: Session, slug: str) -> Optional[Organization]:
    """Get organization by slug."""
    return db.query(Organization).filter(Organization.slug == slug).first()


def init_organization_tables():
    """Initialize organization tables."""
    engine = get_engine()
    
    # Create organizations table
    create_org_sql = """
    CREATE TABLE IF NOT EXISTS organizations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        slug VARCHAR(100) UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_organizations_slug ON organizations(slug);
    """
    
    # Add organization_id to users table
    add_org_to_users_sql = """
    ALTER TABLE users 
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL;
    
    CREATE INDEX IF NOT EXISTS idx_users_organization ON users(organization_id);
    """
    
    # Add organization_id to import_history
    add_org_to_imports_sql = """
    ALTER TABLE import_history
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
    
    CREATE INDEX IF NOT EXISTS idx_import_history_organization ON import_history(organization_id);
    """
    
    # Add organization_id to uploaded_files if table exists
    add_org_to_uploads_sql = """
    DO $$ 
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'uploaded_files') THEN
            ALTER TABLE uploaded_files
            ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
            
            CREATE INDEX IF NOT EXISTS idx_uploaded_files_organization ON uploaded_files(organization_id);
        END IF;
    END $$;
    """
    
    # Add organization_id to file_imports
    add_org_to_file_imports_sql = """
    ALTER TABLE file_imports
    ADD COLUMN IF NOT EXISTS organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE;
    
    CREATE INDEX IF NOT EXISTS idx_file_imports_organization ON file_imports(organization_id);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_org_sql))
            conn.execute(text(add_org_to_users_sql))
            conn.execute(text(add_org_to_imports_sql))
            conn.execute(text(add_org_to_uploads_sql))
            conn.execute(text(add_org_to_file_imports_sql))
        
        logger.info("Organization tables initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing organization tables: {e}")
        raise
