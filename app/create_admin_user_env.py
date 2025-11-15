"""
Admin user initialization script for Content Atlas.
Runs automatically on application startup if ADMIN_EMAIL is set and user doesn't exist.
Can also be run manually with: python create_admin_user_env.py
"""
import os
import sys
from app.db.session import get_engine
from app.core.security import create_user, User
from sqlalchemy.orm import Session


def create_admin_user_if_not_exists():
    """
    Create admin user from environment variables if they don't already exist.
    Returns True if user was created, False if already exists or skipped.
    """
    # Get values from environment variables
    email = os.getenv('ADMIN_EMAIL')
    password = os.getenv('ADMIN_PASSWORD')
    full_name = os.getenv('ADMIN_NAME')

    # Skip if no admin email configured
    if not email:
        return False

    if not password:
        print("Warning: ADMIN_EMAIL is set but ADMIN_PASSWORD is missing")
        return False

    if len(password) < 8:
        print("Warning: ADMIN_PASSWORD must be at least 8 characters")
        return False

    # Initialize auth tables
    try:
        from app.core.security import init_auth_tables
        init_auth_tables()
    except Exception as e:
        print(f"Warning initializing auth tables: {e}")

    # Check if user already exists
    engine = get_engine()
    with Session(engine) as db:
        existing_user = db.query(User).filter(User.email == email).first()
        if existing_user:
            print(f"✓ Admin user already exists: {existing_user.email}")
            return False

        # Create user if they don't exist
        try:
            user = create_user(
                db=db,
                email=email,
                password=password,
                full_name=full_name,
                role="admin"
            )
            print("✓ Admin user created successfully!")
            print(f"  Email: {user.email}")
            print(f"  Name: {user.full_name or 'N/A'}")
            print(f"  Created: {user.created_at}")
            return True
        except Exception as e:
            print(f"Error creating admin user: {e}")
            return False


def main():
    """Main function when script is run directly."""
    print("=" * 60)
    print("Content Atlas - Admin User Creation")
    print("=" * 60)
    print()

    created = create_admin_user_if_not_exists()

    if created:
        print()
        print("You can now login to the dashboard with these credentials.")
    elif os.getenv('ADMIN_EMAIL'):
        print()
        print("Admin user already exists - no action needed.")
    else:
        print("No ADMIN_EMAIL environment variable set - skipping admin user creation.")

    print("=" * 60)


if __name__ == "__main__":
    main()
