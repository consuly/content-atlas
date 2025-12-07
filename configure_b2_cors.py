#!/usr/bin/env python3
"""
Helper script to configure CORS rules for Backblaze B2 bucket.

This script automates the process of setting up CORS rules for direct browser uploads
to B2 using presigned URLs. It uses the B2 CLI to apply the necessary CORS configuration.

Usage:
    python configure_b2_cors.py --bucket-name <bucket-name> --environment <dev|prod>

Requirements:
    - B2 CLI installed: pip install b2
    - B2 credentials configured (B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY in .env)
"""

import argparse
import json
import subprocess
import sys
import os
from pathlib import Path


def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        print("❌ Error: .env file not found")
        print("Please create a .env file with B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY")
        sys.exit(1)
    
    env_vars = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    
    return env_vars


def check_b2_cli():
    """Check if B2 CLI is installed."""
    try:
        result = subprocess.run(['b2', 'version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ B2 CLI found: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        pass
    
    print("❌ Error: B2 CLI not found")
    print("Install it with: pip install b2")
    return False


def authorize_b2(key_id: str, app_key: str):
    """Authorize B2 CLI with credentials."""
    print("\n🔐 Authorizing B2 CLI...")
    
    try:
        result = subprocess.run(
            ['b2', 'authorize-account', key_id, app_key],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✓ B2 CLI authorized successfully")
            return True
        else:
            print(f"❌ Authorization failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error during authorization: {e}")
        return False


def get_cors_rules(environment: str, custom_origins: list = None) -> list:
    """Generate CORS rules based on environment."""
    
    if environment == 'dev':
        origins = custom_origins or [
            "http://localhost:5173",
            "http://localhost:3000",
            "http://localhost:8000"
        ]
    elif environment == 'prod':
        if not custom_origins:
            print("❌ Error: Production environment requires --origins parameter")
            sys.exit(1)
        origins = custom_origins
    elif environment == 'test':
        origins = ["*"]
        print("⚠️  WARNING: Using wildcard origin (*) - NOT RECOMMENDED FOR PRODUCTION")
    else:
        print(f"❌ Error: Invalid environment '{environment}'. Use 'dev', 'prod', or 'test'")
        sys.exit(1)
    
    return [
        {
            "corsRuleName": "allowDirectUpload",
            "allowedOrigins": origins,
            "allowedOperations": [
                "s3_put",
                "s3_get",
                "s3_head"
            ],
            "allowedHeaders": ["*"],
            "exposeHeaders": ["ETag"],
            "maxAgeSeconds": 3600
        }
    ]


def get_bucket_type(bucket_name: str) -> str:
    """Get the current bucket type (allPublic or allPrivate)."""
    try:
        result = subprocess.run(
            ['b2', 'bucket', 'get', bucket_name],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            bucket_info = json.loads(result.stdout)
            bucket_type = bucket_info.get('bucketType', 'allPrivate')
            print(f"✓ Current bucket type: {bucket_type}")
            return bucket_type
        else:
            print(f"⚠️  Could not determine bucket type, defaulting to 'allPrivate'")
            return 'allPrivate'
    except Exception as e:
        print(f"⚠️  Error getting bucket type: {e}, defaulting to 'allPrivate'")
        return 'allPrivate'


def apply_cors_rules(bucket_name: str, cors_rules: list):
    """Apply CORS rules to B2 bucket."""
    print(f"\n📝 Applying CORS rules to bucket '{bucket_name}'...")
    
    # Get current bucket type first
    bucket_type = get_bucket_type(bucket_name)
    
    # Convert rules to JSON string
    cors_json = json.dumps(cors_rules)
    
    try:
        result = subprocess.run(
            ['b2', 'bucket', 'update', '--cors-rules', cors_json, bucket_name, bucket_type],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✓ CORS rules applied successfully")
            return True
        else:
            print(f"❌ Failed to apply CORS rules: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error applying CORS rules: {e}")
        return False


def verify_cors_rules(bucket_name: str):
    """Verify CORS rules are applied correctly."""
    print(f"\n🔍 Verifying CORS configuration for bucket '{bucket_name}'...")
    
    try:
        result = subprocess.run(
            ['b2', 'bucket', 'get', bucket_name],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            bucket_info = json.loads(result.stdout)
            cors_rules = bucket_info.get('corsRules', [])
            
            if cors_rules:
                print("✓ CORS rules verified:")
                print(json.dumps(cors_rules, indent=2))
                return True
            else:
                print("⚠️  Warning: No CORS rules found on bucket")
                return False
        else:
            print(f"❌ Failed to verify CORS rules: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error verifying CORS rules: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Configure CORS rules for Backblaze B2 bucket',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Development environment (localhost)
  python configure_b2_cors.py --bucket-name content-atlas --environment dev

  # Production environment with custom origins
  python configure_b2_cors.py --bucket-name content-atlas --environment prod --origins https://app.example.com https://www.example.com

  # Test environment (allow all origins - NOT RECOMMENDED FOR PRODUCTION)
  python configure_b2_cors.py --bucket-name content-atlas --environment test
        """
    )
    
    parser.add_argument(
        '--bucket-name',
        required=True,
        help='Name of the B2 bucket'
    )
    
    parser.add_argument(
        '--environment',
        required=True,
        choices=['dev', 'prod', 'test'],
        help='Environment: dev (localhost), prod (custom origins), or test (all origins)'
    )
    
    parser.add_argument(
        '--origins',
        nargs='+',
        help='Custom origins for CORS (required for prod environment)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("B2 CORS Configuration Script")
    print("=" * 60)
    
    # Check B2 CLI
    if not check_b2_cli():
        sys.exit(1)
    
    # Load environment variables
    env_vars = load_env_file()
    key_id = env_vars.get('B2_APPLICATION_KEY_ID') or env_vars.get('STORAGE_ACCESS_KEY_ID')
    app_key = env_vars.get('B2_APPLICATION_KEY') or env_vars.get('STORAGE_SECRET_ACCESS_KEY')
    
    if not key_id or not app_key:
        print("❌ Error: B2 credentials not found in .env file")
        print("Required: B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY")
        sys.exit(1)
    
    # Authorize B2 CLI
    if not authorize_b2(key_id, app_key):
        sys.exit(1)
    
    # Generate CORS rules
    cors_rules = get_cors_rules(args.environment, args.origins)
    
    print("\n📋 CORS Rules to be applied:")
    print(json.dumps(cors_rules, indent=2))
    
    # Confirm before applying
    response = input("\nApply these CORS rules? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("❌ Operation cancelled")
        sys.exit(0)
    
    # Apply CORS rules
    if not apply_cors_rules(args.bucket_name, cors_rules):
        sys.exit(1)
    
    # Verify CORS rules
    if not verify_cors_rules(args.bucket_name):
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✓ CORS configuration completed successfully!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Clear your browser cache")
    print("2. Restart your frontend development server")
    print("3. Try uploading a file again")
    print("\nIf you still experience CORS issues:")
    print("- Check browser console for exact error message")
    print("- Verify the origin matches your frontend URL exactly")
    print("- Ensure you're using the S3 Compatible API (not B2 Native API)")


if __name__ == '__main__':
    main()
