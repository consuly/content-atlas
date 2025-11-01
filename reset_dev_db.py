#!/usr/bin/env python3
"""
Development Database Reset Script

This script resets the database for testing purposes while preserving user accounts.

Usage:
    python reset_dev_db.py              # Interactive mode with confirmation
    python reset_dev_db.py --yes        # Auto-confirm (for automation)
    python reset_dev_db.py --force-production --yes  # Force reset in production (DANGEROUS!)

What gets reset:
    - All user-created data tables (contacts, products, etc.)
    - Tracking tables (file_imports, table_metadata, import_history, uploaded_files)
    - All files in B2 storage under "uploads/" folder

What is preserved:
    - Users table (login accounts remain intact)
"""

import sys
import argparse
from app.db.seeds.reset import reset_database_data, is_production_environment, ProductionEnvironmentError
from app.core.config import settings


def print_banner():
    """Print warning banner."""
    print("\n" + "=" * 80)
    print("DATABASE RESET UTILITY - DEVELOPMENT ONLY")
    print("=" * 80)


def print_warning():
    """Print what will be reset."""
    print("\n⚠️  WARNING: This will reset the following:")
    print("   • All user-created data tables (contacts, products, etc.)")
    print("   • Tracking tables (file_imports, table_metadata, import_history, uploaded_files)")
    print("     - These will be dropped and recreated with the latest schema on startup")
    print("   • All files in B2 storage (uploads folder)")
    print("\n✓  The following will be PRESERVED:")
    print("   • Users table (your login accounts)")
    print()


def confirm_reset() -> bool:
    """
    Ask user to confirm the reset operation.
    
    Returns:
        True if user confirms, False otherwise
    """
    print("To confirm, type 'RESET' (in capital letters): ", end="")
    confirmation = input().strip()
    return confirmation == "RESET"


def main():
    """Main entry point for the reset script."""
    parser = argparse.ArgumentParser(
        description="Reset development database while preserving user accounts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python reset_dev_db.py                    # Interactive mode
  python reset_dev_db.py --yes              # Auto-confirm
  python reset_dev_db.py --force-production # Force production reset (DANGEROUS!)
        """
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Auto-confirm without prompting (for automation)'
    )
    parser.add_argument(
        '--force-production',
        action='store_true',
        help='Allow reset in production environment (DANGEROUS!)'
    )
    
    args = parser.parse_args()
    
    # Print banner
    print_banner()
    
    # Check environment
    is_prod = is_production_environment()
    db_url_display = settings.database_url.split('@')[-1] if '@' in settings.database_url else settings.database_url
    
    print(f"\nDatabase: {db_url_display}")
    print(f"Environment: {'PRODUCTION' if is_prod else 'DEVELOPMENT'}")
    
    # Production safety check
    if is_prod and not args.force_production:
        print("\n❌ ERROR: Production environment detected!")
        print("   This script is designed for development use only.")
        print("   If you really want to reset production data, use --force-production flag.")
        print("   (This is EXTREMELY DANGEROUS and will cause data loss!)")
        sys.exit(1)
    
    if is_prod and args.force_production:
        print("\n🚨 DANGER: You are about to reset a PRODUCTION database!")
        print("   This will cause PERMANENT DATA LOSS!")
        if not args.yes:
            print("\n   Type 'I UNDERSTAND THE RISKS' to continue: ", end="")
            risk_confirmation = input().strip()
            if risk_confirmation != "I UNDERSTAND THE RISKS":
                print("\n❌ Reset cancelled.")
                sys.exit(0)
    
    # Show what will be reset
    print_warning()
    
    # Confirmation
    if not args.yes:
        if not confirm_reset():
            print("\n❌ Reset cancelled.")
            sys.exit(0)
    
    # Perform reset
    print("\n🔄 Starting database reset...")
    print("-" * 80)
    
    try:
        results = reset_database_data(force_production=args.force_production)
        
        # Print results
        print("\n✅ Database reset completed!")
        print("-" * 80)
        
        if results['tables_dropped']:
            print(f"\n📋 Dropped {len(results['tables_dropped'])} tables:")
            for table in results['tables_dropped']:
                print(f"   • {table}")
        else:
            print("\n📋 No tables to drop")
        
        if results['tables_truncated']:
            print(f"\n⚠️  Note: {len(results['tables_truncated'])} tables were truncated (legacy):")
            for table in results['tables_truncated']:
                print(f"   • {table}")
        
        if results['b2_files_deleted'] > 0:
            print(f"\n☁️  Deleted {results['b2_files_deleted']} files from B2 storage")
        else:
            print("\n☁️  No B2 files to delete (or B2 not configured)")
        
        if results['errors']:
            print(f"\n⚠️  Warnings/Errors ({len(results['errors'])}):")
            for error in results['errors']:
                print(f"   • {error}")
        
        print("\n" + "=" * 80)
        print("✅ Reset complete! Your user accounts are preserved.")
        print("=" * 80 + "\n")
        
        sys.exit(0)
        
    except ProductionEnvironmentError as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: Reset failed!")
        print(f"   {type(e).__name__}: {e}")
        print("\n   The database may be in an inconsistent state.")
        print("   Please check the logs and database manually.")
        sys.exit(1)


if __name__ == "__main__":
    main()
