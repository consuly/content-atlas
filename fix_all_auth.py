"""Comprehensive script to fix authentication in all test files."""
import re
from pathlib import Path

files_to_fix = [
    "tests/test_file_analysis.py",
    "tests/test_fingerprint_loose_matching.py",
    "tests/test_headerless_merge_duplicates.py",
    "tests/test_llm_client_list_a_instructions.py",
    "tests/test_llm_sequential_merge.py",
    "tests/test_zip_duplicate_repro.py",
    "tests/test_complex_merge_repro.py",
    "tests/test_archive_same_structure_merge_with_instruction.py",
    "tests/test_workbook_auto_process.py",
]

for file_path in files_to_fix:
    path = Path(file_path)
    if not path.exists():
        print(f"✗ File not found: {file_path}")
        continue
    
    content = path.read_text()
    original = content
    
    # Add auth_headers to function definitions that have require_llm
    content = re.sub(
        r'^(def test_\w+\(require_llm\):)',
        r'\1, auth_headers:',
        content,
        flags=re.MULTILINE
    )
    
    # Add auth_headers to function definitions that have monkeypatch, tmp_path, etc.
    content = re.sub(
        r'^(def test_\w+\([^)]+\):)',
        lambda m: m.group(0).rstrip(':') + ', auth_headers):' if 'auth_headers' not in m.group(0) and 'def test_' in m.group(0) else m.group(0),
        content,
        flags=re.MULTILINE
    )
    
    # Add headers to /analyze-file POST requests (with files=files, data={...})
    content = re.sub(
        r'(client\.post\("/analyze-file",\s*files=files,\s*data=\{[^}]*\})\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /analyze-file POST requests (with files=files, data={...}, json={...})
    content = re.sub(
        r'(client\.post\("/analyze-file",\s*files=files,\s*data=\{[^}]*\},[^)]*)\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /analyze-file POST requests (with files=files, json={...})
    content = re.sub(
        r'(client\.post\("/analyze-file",\s*files=files,\s*json=\{[^}]*\})\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /tables GET requests
    content = re.sub(
        r'client\.get\("/tables([^"]+)"\)',
        r'client.get("/tables\1", headers=auth_headers)',
        content
    )
    
    # Add headers to /map-data POST requests (with files=files, data=data)
    content = re.sub(
        r'(client\.post\("/map-data",\s*files=files,\s*data=data)\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /map-data POST requests (with files=files, data=data, json={...})
    content = re.sub(
        r'(client\.post\("/map-data",\s*files=files,\s*data=data,\s*json=\{[^}]*\})\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /auto-process-archive POST requests
    content = re.sub(
        r'(client\.post\("/auto-process-archive",\s*data=\{[^}]*\})\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to /import-jobs/{job_id} GET requests
    content = re.sub(
        r'client\.get\("/import-jobs/\{job_id\}"\)',
        r'client.get("/import-jobs/{job_id}", headers=auth_headers)',
        content
    )
    
    # Add headers to _upload_zip calls
    content = re.sub(
        r'(_upload_zip\([^,]+,\s*filename="[^"]+")\)',
        r'\1, headers=auth_headers)',
        content
    )
    
    # Add headers to _wait_for_job calls
    content = re.sub(
        r'(_wait_for_job\(job_id,\s*headers=auth_headers,\s*timeout=[^)]+\))',
        r'\1',
        content
    )
    
    if content != original:
        path.write_text(content)
        print(f"✓ Fixed {file_path}")
    else:
        print(f"○ No changes needed: {file_path}")

print("\nDone!")
