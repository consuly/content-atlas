#!/usr/bin/env python3
"""
Development Database Reset Script

This script resets the database for testing purposes and drops all data, including user accounts.

Usage:
    python reset_dev_db.py              # Interactive mode with confirmation
    python reset_dev_db.py --yes        # Auto-confirm (for automation)
    python reset_dev_db.py --force-production --yes  # Force reset in production (DANGEROUS!)

What gets reset:
    - All tables, including users and API keys
    - Tracking tables (file_imports, table_metadata, import_history, import_jobs, uploaded_files)
    - All files in storage under "uploads/" folder
    - All log files in the logs/ directory
"""

import sys
import os
import argparse
from pathlib import Path
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
    print("   • All tables, including user accounts and API keys")
    print("   • Tracking tables (file_imports, table_metadata, import_history, import_jobs, uploaded_files)")
    print("     - Tables will be dropped and need to be recreated via migrations/app startup")
    print("   • All files in storage (uploads folder)")
    print("   • All log files in the logs/ directory")
    print()


def clean_log_files() -> dict:
    """
    Remove all log files from the logs/ directory.
    
    Returns:
        Dictionary with cleanup results
    """
    logs_dir = Path("logs")
    results = {
        'files_deleted': 0,
        'errors': []
    }
    
    if not logs_dir.exists():
        return results
    
    try:
        # Clean all log file types (.log, .jsonl, etc.)
        for pattern in ["*.log", "*.jsonl"]:
            for log_file in logs_dir.glob(pattern):
                try:
                    log_file.unlink()
                    results['files_deleted'] += 1
                except Exception as e:
                    results['errors'].append(f"Failed to delete {log_file.name}: {e}")
    except Exception as e:
        results['errors'].append(f"Failed to access logs directory: {e}")
    
    return results


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
        description="Reset development database (drops all data, including users)",
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
        # Clean log files first
        log_results = clean_log_files()
        
        # Reset database
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
        
        # Check for storage_files_deleted (new) or b2_files_deleted (legacy)
        storage_files_deleted = results.get('storage_files_deleted', results.get('b2_files_deleted', 0))
        if storage_files_deleted > 0:
            print(f"\n☁️  Deleted {storage_files_deleted} files from storage")
        else:
            print("\n☁️  No storage files to delete (or storage not configured)")
        
        if log_results['files_deleted'] > 0:
            print(f"\n📝 Deleted {log_results['files_deleted']} log files")
        else:
            print("\n📝 No log files to delete")
        
        # Combine errors from both operations
        all_errors = results['errors'] + log_results['errors']
        if all_errors:
            print(f"\n⚠️  Warnings/Errors ({len(all_errors)}):")
            for error in all_errors:
                print(f"   • {error}")
        
        print("\n" + "=" * 80)
        print("✅ Reset complete! All tables were dropped (including users).")
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
