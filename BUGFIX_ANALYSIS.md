# Bug Analysis: Incorrect Email Field Selection and Duplicate Emails

## Problem Statement

When the user provides an instruction like:
> "Keep only the primary email and phone number, personal email and phone number. Create only one column email in the new table. If the row has two different emails address create a new entry per unique email with the second email duplicate the information."

The system is incorrectly:
1. **Selecting the wrong email columns** - Including "Email 1", "Email 2" instead of "Primary Email", "Personal Email"
2. **Creating duplicate emails** - The same email appears multiple times in the output

## Root Cause Analysis

### Issue 1: Auto-Expansion of Email Columns

**Location**: `app/integrations/auto_import.py` - `_synthesize_multi_value_rules()` function

**Problem**: The function was previously designed to automatically detect and include "sibling" columns (e.g., if "Email 1" is mapped, it would auto-include "Email 2", "Email 3", etc.). This was removed in a recent refactor, but the LLM prompt in `analyzer.py` still doesn't explicitly warn against this behavior.

**Current Code** (simplified for multi-value synthesis):
```python
def _synthesize_multi_value_rules(
    column_mapping: Dict[str, str],
    column_transformations: List[Dict[str, Any]],
    row_transformations: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    instruction_text: str,
    *,
    multi_value_directives: Optional[List[Dict[str, Any]]] = None,
    require_explicit_multi_value: bool = True,
) -> tuple[Dict[str, str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process multi-value transformation rules by trusting the LLM's explicit directives.
    
    This function has been simplified to ONLY apply explicit directives provided by the LLM.
    It no longer performs automatic sibling detection or pattern-based explosion.
    """
```

The code now correctly trusts the LLM's directives, but the LLM is making incorrect decisions.

### Issue 2: LLM Prompt Doesn't Emphasize Column Selection Rules

**Location**: `app/domain/queries/analyzer.py` - `create_file_analyzer_agent()` system prompt

**Problem**: The system prompt for the LLM doesn't clearly state that it should:
1. **Use ONLY the columns explicitly mentioned by the user**
2. **NOT auto-expand to numbered siblings** (Email 1, Email 2, etc.)
3. **Respect the user's exact column names**

**Current Prompt Section** (row_transformations):
```python
6. **row_transformations**: When rows must be duplicated/filtered/cleaned before mapping...
   
   **CRITICAL - Multi-Column Consolidation Pattern:**
   When the user instruction asks to "create new entries/rows" or "keep only one [field] per row"...
```

The prompt explains HOW to use `explode_columns` but doesn't emphasize WHICH columns to select.

### Issue 3: Duplicate Detection Not Working

**Location**: `app/domain/imports/preprocessor.py` - `_apply_explode_columns()` function

**Problem**: The `dedupe_values` parameter is set to `True` by default, which should prevent duplicate emails. However, if the LLM is including the wrong source columns, the deduplication might not work as expected.

**Current Code**:
```python
def _apply_explode_columns(
    records: List[Dict[str, Any]],
    transformation: Dict[str, Any],
    *,
    row_offset: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Duplicate each row once per populated source column and place the value
    into a single target column.
    """
    # ...
    dedupe_values = transformation.get("dedupe_values", True)
    case_insensitive_dedupe = transformation.get("case_insensitive_dedupe", True)
    # ...
    if dedupe_values:
        stacked["_dedupe_key"] = stacked[target_column].apply(
            lambda v: v.lower() if case_insensitive_dedupe and isinstance(v, str) else v
        )
        stacked = stacked.loc[
            ~stacked[["_row_index", "_dedupe_key"]].duplicated()
        ].copy()
```

The deduplication logic looks correct - it should remove duplicate emails within the same source row.

## The Real Problem: LLM Interpretation

After analyzing the code, the issue is **NOT in the execution logic** but in **how the LLM interprets the user's instruction**.

When the user says:
> "Keep only the primary email and phone number, personal email and phone number"

The LLM needs to:
1. **Identify which columns match "primary email"** → Should be "Primary Email" column
2. **Identify which columns match "personal email"** → Should be "Personal Email" column
3. **NOT include other email columns** like "Email 1", "Email 2" unless explicitly mentioned

The LLM is likely:
- Seeing columns like "Email 1", "Email 2", "Primary Email", "Personal Email"
- Interpreting "primary" as "Email 1" (first/primary)
- Interpreting "personal" as "Email 2" (second/personal)
- Including both the numbered columns AND the named columns

## Solution

### Fix 1: Update LLM System Prompt (CRITICAL)

**File**: `app/domain/queries/analyzer.py`

**Change**: Add explicit column selection rules to the system prompt:

```python
**IMPORTANT - Column Selection Rules:**
1. **Use ONLY the columns the user explicitly mentions** - Do NOT auto-expand to numbered siblings
2. If user says "Primary Email and Personal Email", use EXACTLY those two columns
3. Do NOT include "Email 1", "Email 2", etc. unless the user specifically mentions them
4. The `source_columns` list in `explode_columns` must match the user's instruction EXACTLY
5. When in doubt, prefer named columns (e.g., "Primary Email") over numbered columns (e.g., "Email 1")
```

### Fix 2: Add Column Name Matching Guidance

Add examples to the prompt showing correct vs incorrect column selection:

```python
**Example - Correct Column Selection:**
User says: "Keep primary and personal email"
Available columns: ["Email 1", "Email 2", "Primary Email", "Personal Email"]

CORRECT:
source_columns: ["Primary Email", "Personal Email"]

INCORRECT:
source_columns: ["Email 1", "Email 2"]  // Wrong - user didn't mention numbered columns
source_columns: ["Email 1", "Email 2", "Primary Email", "Personal Email"]  // Wrong - too many
```

### Fix 3: Add Validation in Auto-Import

**File**: `app/integrations/auto_import.py`

**Change**: Add validation to warn when the LLM selects columns that don't match the user's instruction:

```python
def _validate_column_selection(
    source_columns: List[str],
    instruction_text: str,
    available_columns: Set[str]
) -> List[str]:
    """
    Validate that selected columns match the user's instruction.
    Warn if numbered columns are selected when named columns exist.
    """
    warnings = []
    
    # Check if instruction mentions specific column names
    instruction_lower = instruction_text.lower()
    
    # If user says "primary" and we have "Primary Email", prefer that over "Email 1"
    if "primary" in instruction_lower:
        primary_cols = [col for col in available_columns if "primary" in col.lower()]
        numbered_cols = [col for col in source_columns if re.match(r'email\s*\d+', col, re.I)]
        
        if primary_cols and numbered_cols:
            warnings.append(
                f"Warning: User mentioned 'primary' but numbered columns {numbered_cols} "
                f"were selected instead of {primary_cols}"
            )
    
    return warnings
```

## Testing Strategy

### Test Case 1: Named Columns Only
```python
def test_respects_named_email_columns_only():
    """LLM should use only the named columns mentioned by user."""
    instruction = "Keep only primary email and personal email. Create one email column."
    available_columns = ["Email 1", "Email 2", "Primary Email", "Personal Email", "Name"]
    
    # Expected: LLM should select ["Primary Email", "Personal Email"]
    # NOT: ["Email 1", "Email 2"]
    # NOT: ["Email 1", "Email 2", "Primary Email", "Personal Email"]
```

### Test Case 2: Numbered Columns When Explicitly Mentioned
```python
def test_uses_numbered_columns_when_explicit():
    """LLM should use numbered columns when user explicitly mentions them."""
    instruction = "Keep email 1 and email 2 only. Create one email column."
    available_columns = ["Email 1", "Email 2", "Primary Email", "Personal Email", "Name"]
    
    # Expected: LLM should select ["Email 1", "Email 2"]
    # NOT: ["Primary Email", "Personal Email"]
```

### Test Case 3: Deduplication Works
```python
def test_deduplicates_same_email_across_columns():
    """System should deduplicate when same email appears in multiple columns."""
    records = [
        {"Primary Email": "john@example.com", "Personal Email": "john@example.com", "Name": "John"}
    ]
    
    # Expected: Only ONE row with "john@example.com"
    # NOT: Two rows with the same email
```

## Recommended Implementation Order

1. **Update LLM system prompt** (analyzer.py) - HIGHEST PRIORITY
   - Add column selection rules
   - Add examples of correct vs incorrect selection
   - Emphasize matching user's exact column names

2. **Add validation warnings** (auto_import.py) - MEDIUM PRIORITY
   - Warn when column selection seems incorrect
   - Log mismatches for debugging

3. **Add test cases** - MEDIUM PRIORITY
   - Test named vs numbered column selection
   - Test deduplication
   - Test user instruction parsing

4. **Update documentation** - LOW PRIORITY
   - Document column selection behavior
   - Add examples to user guide

## Expected Outcome

After implementing these fixes:

1. **User says**: "Keep primary and personal email"
   - **System selects**: ["Primary Email", "Personal Email"]
   - **NOT**: ["Email 1", "Email 2"]

2. **User says**: "Keep email 1 and email 2"
   - **System selects**: ["Email 1", "Email 2"]
   - **NOT**: ["Primary Email", "Personal Email"]

3. **Deduplication works**:
   - If both columns have "john@example.com", only ONE row is created
   - No duplicate emails in the output

## Files to Modify

1. `app/domain/queries/analyzer.py` - Update system prompt (CRITICAL)
2. `app/integrations/auto_import.py` - Add validation (optional)
3. `tests/test_llm_column_selection.py` - **NEW FILE** - Integration tests for LLM column selection
4. `tests/test_auto_import_multi_value.py` - Add test cases
5. `docs/ROW_TRANSFORMATIONS.md` - Update documentation

## New Test File Created

### `tests/test_llm_column_selection.py`

A comprehensive integration test suite that validates the LLM's column selection behavior. These tests:

1. **Make actual LLM calls** using `analyze_file_for_import()` - not mocked
2. **Validate the LLM decision structure** returned by the agent
3. **Test real-world scenarios** with CSV files containing multiple email columns

#### Test Coverage

**Test 1: `test_llm_selects_named_columns_when_user_mentions_primary_and_personal`**
- **Purpose**: Core bug fix validation
- **Scenario**: User says "keep primary and personal email"
- **Expected**: LLM selects ["Primary Email", "Personal Email"]
- **NOT**: ["Email 1", "Email 2"]
- **Validates**: Correct column name matching

**Test 2: `test_llm_selects_numbered_columns_when_user_explicitly_mentions_them`**
- **Purpose**: Ensure LLM respects explicit numbered column requests
- **Scenario**: User says "keep email 1 and email 2"
- **Expected**: LLM selects ["Email 1", "Email 2"]
- **NOT**: ["Primary Email", "Personal Email"]
- **Validates**: LLM doesn't over-correct to named columns

**Test 3: `test_llm_enables_deduplication_for_explode_columns`**
- **Purpose**: Verify deduplication is enabled
- **Scenario**: CSV with duplicate emails in multiple columns
- **Expected**: `dedupe_values=True` and `case_insensitive_dedupe=True`
- **Validates**: Duplicate email prevention

**Test 4: `test_llm_creates_single_target_column_for_multiple_sources`**
- **Purpose**: Verify single target column creation
- **Scenario**: User says "create only one column email"
- **Expected**: Single target column named "email"
- **Validates**: Column consolidation works correctly

**Test 5: `test_llm_respects_user_column_names_case_insensitive`**
- **Purpose**: Verify case-insensitive matching
- **Scenario**: User says "primary email" (lowercase), column is "Primary Email"
- **Expected**: LLM matches correctly despite case difference
- **Validates**: Robust column name matching

**Test 6: `test_llm_does_not_auto_expand_to_all_email_columns`**
- **Purpose**: Prevent auto-expansion bug
- **Scenario**: User says "keep only primary email" (singular)
- **Expected**: Only ["Primary Email"] selected
- **NOT**: All email columns
- **Validates**: No unwanted column expansion

**Test 7: `test_llm_column_mapping_consistency`**
- **Purpose**: Verify mapping consistency
- **Scenario**: Multiple source columns to single target
- **Expected**: `column_mapping` reflects the exploded target column
- **Validates**: Mapping and transformations are aligned

#### Test Data

The tests use two helper functions to generate test CSV data:

1. **`_create_test_csv_with_multiple_email_columns()`**
   - Creates CSV with: "Primary Email", "Personal Email", "Email 1", "Email 2"
   - Simulates real-world scenario with mixed column naming

2. **`_create_test_csv_with_duplicate_emails()`**
   - Creates CSV where same email appears in multiple columns
   - Tests deduplication logic

#### Running the Tests

```bash
# Run all LLM column selection tests
pytest tests/test_llm_column_selection.py -v

# Run a specific test
pytest tests/test_llm_column_selection.py::test_llm_selects_named_columns_when_user_mentions_primary_and_personal -v

# Run with detailed output
pytest tests/test_llm_column_selection.py -v -s
```

#### Important Notes

1. **These are integration tests** - They require:
   - Valid `ANTHROPIC_API_KEY` in environment
   - Database connection (for schema context)
   - LangChain and Claude API access

2. **Tests are marked with `@pytest.mark.integration`** - Can be skipped in CI:
   ```bash
   pytest -m "not integration"  # Skip integration tests
   ```

3. **Tests validate LLM behavior** - They ensure the prompt changes work correctly

4. **Tests will fail BEFORE the fix** - This is expected! They document the bug.

5. **Tests should pass AFTER the fix** - Once the system prompt is updated in `analyzer.py`

#### Test Execution Flow

```
1. Create test CSV with multiple email columns
2. Call analyze_file_for_import() with user instruction
3. LLM analyzes file and makes decision
4. Extract row_transformations from llm_decision
5. Validate source_columns in explode_columns transformation
6. Assert correct columns were selected
```

#### Expected Test Results

**Before Fix** (Current State):
- ❌ Test 1 fails: LLM selects ["Email 1", "Email 2"] instead of ["Primary Email", "Personal Email"]
- ✅ Test 2 passes: LLM correctly selects numbered columns when explicit
- ✅ Test 3 passes: Deduplication is enabled
- ✅ Test 4 passes: Single target column created
- ⚠️ Test 5 may fail: Case-insensitive matching might not work
- ❌ Test 6 fails: LLM auto-expands to all email columns

**After Fix** (Expected State):
- ✅ All tests pass
- LLM correctly interprets user instructions
- Column selection matches user intent
- No unwanted auto-expansion
