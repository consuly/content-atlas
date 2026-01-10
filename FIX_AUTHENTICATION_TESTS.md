# Guide: Fixing Authentication in Tests

## Summary
Multiple test files need updating to pass `auth_headers` when calling authenticated endpoints (`/map-data`, `/tables`, etc.).

## Files Requiring Updates

Based on the test failures:
- `tests/test_duplicate_detection.py` (7 functions)
- `tests/test_archive_auto_process.py` (6 functions)
- `tests/test_archive_same_structure_merge_with_instruction.py` (4 functions)
- `tests/test_auto_execution_failure_status.py` (1 function)
- `tests/test_complex_merge_repro.py` (1 function)

## Pattern to Apply

### Step 1: Add `auth_headers` Parameter
Change:
```python
def test_example():
    """Test description"""
```

To:
```python
def test_example(auth_headers):
    """Test description"""
```

### Step 2: Pass Headers to Client Calls
Change:
```python
response = client.post("/map-data", files=files, data=data)
response = client.get("/tables/my_table")
```

To:
```python
response = client.post("/map-data", files=files, data=data, headers=auth_headers)
response = client.get("/tables/my_table", headers=auth_headers)
```

## Example: test_duplicate_detection.py

### Before:
```python
def test_duplicate_detection_file_level():
    """Test file-level duplicate detection prevents importing the same file twice."""
    # ... setup code ...
    
    response = client.post("/map-data", files=files, data=data)
    assert response.status_code in [200, 500]
    
    if response.status_code == 200:
        response = client.post("/map-data", files=files, data=data)
        assert response.status_code == 409
```

### After:
```python
def test_duplicate_detection_file_level(auth_headers):
    """Test file-level duplicate detection prevents importing the same file twice."""
    # ... setup code ...
    
    response = client.post("/map-data", files=files, data=data, headers=auth_headers)
    assert response.status_code in [200, 500]
    
    if response.status_code == 200:
        response = client.post("/map-data", files=files, data=data, headers=auth_headers)
        assert response.status_code == 409
```

## Bulk Find & Replace Strategy

For each test file, you can use these regex patterns in VS Code:

### Pattern 1: Add auth_headers parameter
- Find: `^def (test_\w+)\(\):`
- Replace: `def $1(auth_headers):`

### Pattern 2: Add headers to POST requests
- Find: `client\.post\("/map-data", files=([^,]+), data=([^)]+)\)`
- Replace: `client.post("/map-data", files=$1, data=$2, headers=auth_headers)`

### Pattern 3: Add headers to GET requests  
- Find: `client\.get\(("/[^"]+")(\))`
- Replace: `client.get($1, headers=auth_headers$2`

## Automated Fix Script

Run this Python script to auto-fix test files:

```python
import re
from pathlib import Path

test_files = [
    "tests/test_duplicate_detection.py",
    "tests/test_archive_auto_process.py",
    "tests/test_archive_same_structure_merge_with_instruction.py",
    "tests/test_auto_execution_failure_status.py",
    "tests/test_complex_merge_repro.py",
]

for file_path in test_files:
    path = Path(file_path)
    if not path.exists():
        continue
        
    content = path.read_text()
    
    # Add auth_headers to function definitions
    content = re.sub(
        r'^def (test_\w+)\(\):',
        r'def \1(auth_headers):',
        content,
        flags=re.MULTILINE
    )
    
    # Add headers to /map-data POST requests
    content = re.sub(
        r'client\.post\("/map-data", files=([^,]+), data=([^)]+)\)',
        r'client.post("/map-data", files=\1, data=\2, headers=auth_headers)',
        content
    )
    
    # Add headers to /tables GET requests
    content = re.sub(
        r'client\.get\(("/tables[^"]*")\)',
        r'client.get(\1, headers=auth_headers)',
        content
    )
    
    path.write_text(content)
    print(f"✓ Fixed {file_path}")
```

Save this as `fix_auth_tests.py` and run: `python fix_auth_tests.py`

## Notes

- The `auth_headers` fixture is already defined in `tests/conftest.py`
- It creates a test user + organization and returns authentication headers
- Tests will be skipped if database is unavailable (via `pytest.skip()`)
- No need to add skip decorators - the fixture handles that

## Verification

After applying fixes, run:
```bash
python -m pytest tests/test_duplicate_detection.py -v
python -m pytest tests/test_archive_auto_process.py -v  
# etc.
```

All tests should now pass or be properly skipped.
