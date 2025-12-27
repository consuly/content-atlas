"""
Script to investigate failed imports by querying uploaded files, import jobs, and import history.

Usage:
    python investigate_import_failure.py                    # Show recent failures
    python investigate_import_failure.py "Marketing Agency" # Search for specific file
"""
import requests
import json
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict

# API Configuration
API_BASE_URL = "http://localhost:8000"


def format_timestamp(ts: Optional[str]) -> str:
    """Format ISO timestamp to readable format"""
    if not ts:
        return "N/A"
    try:
        dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ts


def search_files(search_term: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Search uploaded files by name"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/uploaded-files",
            params={"limit": limit}
        )
        response.raise_for_status()
        data = response.json()
        files = data.get("files", [])
        
        if search_term:
            search_lower = search_term.lower()
            files = [f for f in files if search_lower in f.get("file_name", "").lower()]
        
        return files
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching uploaded files: {e}")
        return []


def get_import_jobs(file_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Get import jobs, optionally filtered by file_id"""
    try:
        params = {"limit": limit}
        if file_id:
            params["file_id"] = file_id
            
        response = requests.get(
            f"{API_BASE_URL}/import-jobs",
            params=params
        )
        response.raise_for_status()
        data = response.json()
        return data.get("jobs", [])
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching import jobs: {e}")
        return []


def get_import_history(import_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Get import history records"""
    try:
        params = {"limit": limit}
        if import_id:
            # Query by specific import_id
            response = requests.get(f"{API_BASE_URL}/import-history/{import_id}")
        else:
            response = requests.get(
                f"{API_BASE_URL}/import-history",
                params=params
            )
        response.raise_for_status()
        data = response.json()
        
        if import_id:
            # Single record response
            return [data.get("import_record")] if data.get("import_record") else []
        else:
            # List response
            return data.get("imports", [])
    except requests.exceptions.RequestException as e:
        if "404" not in str(e):
            print(f"❌ Error fetching import history: {e}")
        return []


def get_mapping_errors(import_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get mapping errors for a specific import"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/import-history/{import_id}/mapping-errors",
            params={"limit": limit}
        )
        response.raise_for_status()
        data = response.json()
        return data.get("errors", [])
    except requests.exceptions.RequestException as e:
        if "404" not in str(e):
            print(f"❌ Error fetching mapping errors: {e}")
        return []


def print_file_summary(file_record: Dict[str, Any], show_details: bool = True):
    """Print a formatted summary of a file"""
    status = file_record.get("status", "unknown")
    
    status_emoji = {
        "uploaded": "📤",
        "mapping": "🔄",
        "mapped": "✅",
        "failed": "❌"
    }.get(status, "❓")
    
    print(f"\n{status_emoji} FILE: {file_record.get('file_name', 'N/A')}")
    print(f"   File ID: {file_record.get('id', 'N/A')}")
    print(f"   Status: {status}")
    print(f"   Size: {file_record.get('file_size', 0):,} bytes")
    print(f"   Uploaded: {format_timestamp(file_record.get('uploaded_at'))}")
    
    if status == "mapped":
        print(f"   Mapped Table: {file_record.get('mapped_table_name', 'N/A')}")
        print(f"   Rows Mapped: {file_record.get('mapped_rows', 0):,}")
    
    if file_record.get("error_message"):
        print(f"   ⚠️  Error: {file_record.get('error_message')}")
    
    if show_details and file_record.get("active_job_id"):
        print(f"   Active Job: {file_record.get('active_job_id')}")
        print(f"   Job Status: {file_record.get('active_job_status', 'N/A')}")
        print(f"   Job Stage: {file_record.get('active_job_stage', 'N/A')}")


def print_job_summary(job: Dict[str, Any], show_metadata: bool = True):
    """Print a formatted summary of an import job"""
    status = job.get("status", "unknown")
    
    status_emoji = {
        "running": "🔄",
        "succeeded": "✅",
        "failed": "❌",
        "waiting_user": "⏸️"
    }.get(status, "❓")
    
    print(f"\n{status_emoji} JOB: {job.get('id', 'N/A')}")
    print(f"   File ID: {job.get('file_id', 'N/A')}")
    print(f"   Status: {status}")
    print(f"   Stage: {job.get('stage', 'N/A')}")
    print(f"   Progress: {job.get('progress', 0)}%")
    print(f"   Trigger: {job.get('trigger_source', 'N/A')}")
    print(f"   Analysis Mode: {job.get('analysis_mode', 'N/A')}")
    print(f"   Created: {format_timestamp(job.get('created_at'))}")
    
    if job.get("completed_at"):
        print(f"   Completed: {format_timestamp(job.get('completed_at'))}")
    
    if job.get("error_message"):
        print(f"\n   ⚠️  ERROR MESSAGE:")
        error_lines = job["error_message"].split("\n")
        for line in error_lines[:10]:  # Show first 10 lines
            print(f"      {line}")
        if len(error_lines) > 10:
            print(f"      ... ({len(error_lines) - 10} more lines)")
    
    if show_metadata:
        metadata = job.get("metadata")
        if metadata and isinstance(metadata, dict):
            print(f"\n   📋 JOB METADATA:")
            for key, value in list(metadata.items())[:10]:  # Show first 10 items
                if key in ("llm_decision", "llm_response"):
                    print(f"      {key}: <present>")
                elif isinstance(value, (list, dict)):
                    print(f"      {key}: {type(value).__name__} ({len(value)} items)")
                else:
                    print(f"      {key}: {value}")
        
        result_metadata = job.get("result_metadata")
        if result_metadata and isinstance(result_metadata, dict):
            print(f"\n   📊 RESULT METADATA:")
            for key, value in list(result_metadata.items())[:10]:
                if isinstance(value, (list, dict)):
                    print(f"      {key}: {type(value).__name__} ({len(value)} items)")
                else:
                    print(f"      {key}: {value}")


def print_import_summary(imp: Dict[str, Any]):
    """Print a formatted summary of an import history record"""
    status = imp.get("status", "unknown")
    
    status_emoji = {
        "success": "✅",
        "failed": "❌",
        "partial": "⚠️",
        "in_progress": "🔄"
    }.get(status, "❓")
    
    print(f"\n{status_emoji} IMPORT: {imp.get('import_id', 'N/A')}")
    print(f"   File Name: {imp.get('file_name', 'N/A')}")
    print(f"   Table: {imp.get('table_name', 'N/A')}")
    print(f"   Status: {status}")
    print(f"   Strategy: {imp.get('import_strategy', 'N/A')}")
    print(f"   Timestamp: {format_timestamp(imp.get('import_timestamp'))}")
    print(f"   Rows Processed: {imp.get('rows_processed') or 0:,}")
    print(f"   Rows Inserted: {imp.get('rows_inserted') or 0:,}")
    print(f"   Duplicates Found: {imp.get('duplicates_found') or 0:,}")
    print(f"   Mapping Errors: {imp.get('mapping_errors_count') or 0:,}")
    
    duration = imp.get('duration_seconds')
    if duration is not None:
        print(f"   Duration: {duration:.2f}s")
    
    if imp.get("error_message"):
        print(f"\n   ⚠️  ERROR MESSAGE:")
        error_lines = imp["error_message"].split("\n")
        for line in error_lines[:10]:
            print(f"      {line}")
        if len(error_lines) > 10:
            print(f"      ... ({len(error_lines) - 10} more lines)")


def print_mapping_errors(errors: List[Dict[str, Any]], limit: int = 20):
    """Print formatted mapping errors"""
    if not errors:
        return
    
    print(f"\n📋 MAPPING ERRORS ({len(errors)} total):")
    print("=" * 80)
    
    # Group errors by type
    error_groups = defaultdict(list)
    for error in errors[:limit]:
        error_type = error.get("error_type", "unknown")
        error_groups[error_type].append(error)
    
    for error_type, type_errors in error_groups.items():
        print(f"\n🔴 Error Type: {error_type} ({len(type_errors)} occurrences)")
        print("-" * 80)
        
        for i, error in enumerate(type_errors[:5], 1):
            print(f"\n  {i}. Record #{error.get('record_number', 'N/A')}")
            if error.get('source_field'):
                print(f"     Source Field: {error.get('source_field')}")
            if error.get('target_field'):
                print(f"     Target Field: {error.get('target_field')}")
            if error.get('source_value'):
                value_str = str(error.get('source_value'))
                if len(value_str) > 100:
                    value_str = value_str[:97] + "..."
                print(f"     Source Value: {value_str}")
            print(f"     Message: {error.get('error_message', 'N/A')}")
        
        if len(type_errors) > 5:
            print(f"\n     ... and {len(type_errors) - 5} more errors of this type")


def investigate_file(file_record: Dict[str, Any]):
    """Comprehensive investigation of a single file"""
    file_id = file_record.get("id")
    file_name = file_record.get("file_name", "Unknown")
    
    print("\n" + "=" * 80)
    print(f"🔍 INVESTIGATING FILE: {file_name}")
    print("=" * 80)
    
    # 1. Show file details
    print_file_summary(file_record, show_details=True)
    
    # 2. Get associated jobs
    jobs = get_import_jobs(file_id=file_id)
    if jobs:
        print(f"\n📦 FOUND {len(jobs)} ASSOCIATED JOB(S):")
        for job in jobs:
            print_job_summary(job, show_metadata=True)
            
            # Check for import history linked to this job
            result_metadata = job.get("result_metadata", {})
            if isinstance(result_metadata, dict):
                import_id = result_metadata.get("import_id")
                if import_id:
                    history_records = get_import_history(import_id=import_id)
                    if history_records:
                        print(f"\n   📊 LINKED IMPORT HISTORY:")
                        for hist in history_records:
                            print_import_summary(hist)
                            
                            # Get mapping errors
                            errors = get_mapping_errors(import_id)
                            if errors:
                                print_mapping_errors(errors)
    else:
        print("\n⚠️  No jobs found for this file")
    
    # 3. Search import history by file name (fallback)
    history_records = get_import_history()
    matching_history = [h for h in history_records if file_name in h.get("file_name", "")]
    if matching_history:
        print(f"\n📊 FOUND {len(matching_history)} IMPORT HISTORY RECORD(S):")
        for hist in matching_history:
            print_import_summary(hist)
            
            import_id = hist.get("import_id")
            if import_id:
                errors = get_mapping_errors(import_id)
                if errors:
                    print_mapping_errors(errors)


def show_recent_failures():
    """Show overview of recent failed imports, jobs, and files"""
    print("=" * 80)
    print("🔍 RECENT FAILURES OVERVIEW")
    print("=" * 80)
    
    # 1. Failed/mapping files
    print("\n📤 RECENT PROBLEMATIC FILES:")
    files = search_files(limit=50)
    problem_files = [f for f in files if f.get("status") in ("failed", "mapping")]
    
    if problem_files:
        for file_record in problem_files[:10]:
            print_file_summary(file_record, show_details=False)
    else:
        print("   ✅ No problematic files found")
    
    # 2. Failed/waiting jobs
    print("\n📦 RECENT PROBLEMATIC JOBS:")
    jobs = get_import_jobs(limit=50)
    problem_jobs = [j for j in jobs if j.get("status") in ("failed", "waiting_user")]
    
    if problem_jobs:
        for job in problem_jobs[:10]:
            print_job_summary(job, show_metadata=False)
    else:
        print("   ✅ No problematic jobs found")
    
    # 3. Failed imports
    print("\n📊 RECENT FAILED IMPORTS:")
    imports = get_import_history(limit=50)
    failed_imports = [i for i in imports if i.get("status") in ("failed", "partial")]
    
    if failed_imports:
        for imp in failed_imports[:10]:
            print_import_summary(imp)
    else:
        print("   ✅ No failed imports found")


def main():
    if len(sys.argv) > 1:
        # Search for specific file
        search_term = " ".join(sys.argv[1:])
        print(f"🔍 Searching for files matching: '{search_term}'")
        
        files = search_files(search_term)
        
        if not files:
            print(f"\n❌ No files found matching '{search_term}'")
            print("\nTry a partial name or check recent failures:")
            print(f"   python {sys.argv[0]}")
            return
        
        print(f"\n✅ Found {len(files)} matching file(s)\n")
        
        for file_record in files:
            investigate_file(file_record)
    else:
        # Show recent failures
        show_recent_failures()
        
        print("\n" + "=" * 80)
        print("💡 TIP: Search for a specific file by name:")
        print(f"   python {sys.argv[0]} \"Marketing Agency\"")
        print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Investigation interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
