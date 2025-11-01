"""
Utility script to create an admin user for Content Atlas.
Run this script to create the first user account.

Usage:
    python create_admin_user.py
"""
from app.db.session import get_engine
from app.core.security import create_user, init_auth_tables
from sqlalchemy.orm import Session
import getpass


def main():
    print("=" * 60)
    print("Content Atlas - Create Admin User")
    print("=" * 60)
    print()
    
    # Initialize auth tables
    try:
        init_auth_tables()
        print("✓ Database tables initialized")
    except Exception as e:
        print(f"Warning: {e}")
    
    # Get user input
    email = input("Enter email address: ").strip()
    if not email:
        print("Error: Email is required")
        return
    
    full_name = input("Enter full name (optional): ").strip() or None
    
    password = getpass.getpass("Enter password: ")
    password_confirm = getpass.getpass("Confirm password: ")
    
    if password != password_confirm:
        print("Error: Passwords do not match")
        return
    
    if len(password) < 8:
        print("Error: Password must be at least 8 characters")
        return
    
    # Create user
    engine = get_engine()
    with Session(engine) as db:
        try:
            user = create_user(
                db=db,
                email=email,
                password=password,
                full_name=full_name
            )
            print()
            print("=" * 60)
            print("✓ User created successfully!")
            print("=" * 60)
            print(f"Email: {user.email}")
            print(f"Name: {user.full_name or 'N/A'}")
            print(f"Created: {user.created_at}")
            print()
            print("You can now login to the dashboard with these credentials.")
            
        except Exception as e:
            print(f"Error creating user: {e}")


if __name__ == "__main__":
    main()
