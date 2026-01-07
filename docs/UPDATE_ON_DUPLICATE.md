# Update on Duplicate with Rollback

This feature allows you to automatically update existing rows when duplicate records are detected during import, instead of skipping them. It includes full audit trail and rollback capabilities.

## Overview

When importing data, you can now configure the system to:
1. **Detect duplicates** based on uniqueness columns (e.g., email, ID)
2. **Update existing rows** with new values instead of skipping them
3. **Track all updates** with before/after values for audit
4. **Rollback updates** individually or in bulk if needed
5. **Detect conflicts** when rolling back if rows have been modified since

## Configuration

### Basic Setup

Enable update-on-duplicate in your `MappingConfig`:

```python
from app.api.schemas.shared import MappingConfig, DuplicateCheckConfig

config = MappingConfig(
    table_name="customers",
    db_schema={
        "email": "VARCHAR(255)",
        "name": "VARCHAR(255)",
        "phone": "VARCHAR(50)",
        "address": "TEXT"
    },
    mappings={
        "email": "email",
        "name": "name",
        "phone": "phone",
        "address": "address"
    },
    duplicate_check=DuplicateCheckConfig(
        enabled=True,
        uniqueness_columns=["email"],  # Find duplicates by email
        update_on_duplicate=True       # Enable auto-update
    )
)
```

### Update Specific Columns Only

You can limit which columns get updated:

```python
duplicate_check=DuplicateCheckConfig(
    enabled=True,
    uniqueness_columns=["email"],
    update_on_duplicate=True,
    update_columns=["phone", "address"]  # Only update these columns
)
```

With this configuration:
- **Email** is used to find existing records
- **Name** is preserved (not updated)
- **Phone** and **Address** are updated with new values

### Example Workflow

```python
# Import 1: Initial data
records_1 = [
    {"email": "john@example.com", "name": "John Doe", "phone": "555-0001"},
    {"email": "jane@example.com", "name": "Jane Smith", "phone": "555-0002"}
]
# Result: 2 rows inserted

# Import 2: Updated data
records_2 = [
    {"email": "john@example.com", "name": "John Doe", "phone": "555-9999"},  # Updated phone
    {"email": "jane@example.com", "name": "Jane Smith", "phone": "555-8888"}, # Updated phone
    {"email": "bob@example.com", "name": "Bob Johnson", "phone": "555-7777"}   # New record
]
# Result: 
#   - 1 row inserted (Bob)
#   - 2 rows updated (John, Jane)
#   - import_history.rows_updated = 2
```

## API Endpoints

### List Row Updates

Get all row updates from an import:

```http
GET /import-history/{import_id}/updates?limit=100&offset=0&include_rolled_back=false
```

**Response:**
```json
{
  "success": true,
  "updates": [
    {
      "id": 123,
      "import_id": "uuid-here",
      "table_name": "customers",
      "row_id": 45,
      "previous_values": {
        "phone": "555-0001",
        "address": "123 Main St"
      },
      "new_values": {
        "phone": "555-9999",
        "address": "456 Oak Ave"
      },
      "updated_columns": ["phone", "address"],
      "updated_at": "2026-01-07T14:30:00Z",
      "rolled_back_at": null,
      "rolled_back_by": null,
      "has_conflict": false
    }
  ],
  "total_count": 15,
  "limit": 100,
  "offset": 0
}
```

### Get Update Detail

Get detailed information about a specific update including current row values:

```http
GET /import-history/{import_id}/updates/{update_id}
```

**Response:**
```json
{
  "success": true,
  "update": {
    "id": 123,
    "import_id": "uuid-here",
    "table_name": "customers",
    "row_id": 45,
    "previous_values": {"phone": "555-0001"},
    "new_values": {"phone": "555-9999"},
    "updated_columns": ["phone"],
    "updated_at": "2026-01-07T14:30:00Z"
  },
  "current_row": {
    "email": "john@example.com",
    "name": "John Doe",
    "phone": "555-9999",
    "address": "123 Main St"
  }
}
```

### Rollback Single Update

Rollback a specific update to restore previous values:

```http
POST /import-history/{import_id}/updates/{update_id}/rollback
Content-Type: application/json

{
  "rolled_back_by": "user@example.com",
  "force": false
}
```

**Parameters:**
- `rolled_back_by` (optional): User performing the rollback
- `force` (optional): Force rollback even if conflict detected (default: false)

**Response (Success):**
```json
{
  "success": true,
  "message": "Update successfully rolled back",
  "update": {
    "id": 123,
    "rolled_back_at": "2026-01-07T15:00:00Z",
    "rolled_back_by": "user@example.com",
    "has_conflict": false
  },
  "conflict": null
}
```

**Response (Conflict Detected):**
```json
{
  "success": false,
  "message": "Conflict detected. Use force=true to override.",
  "update": {...},
  "conflict": {
    "update_id": 123,
    "row_id": 45,
    "original_values": {"phone": "555-0001"},
    "values_at_update": {"phone": "555-9999"},
    "current_values": {"phone": "555-1234"},
    "message": "Row has been modified since the update..."
  }
}
```

### Rollback All Updates

Rollback all updates from an import:

```http
POST /import-history/{import_id}/rollback-all
Content-Type: application/json

{
  "rolled_back_by": "user@example.com",
  "skip_conflicts": false
}
```

**Parameters:**
- `rolled_back_by` (optional): User performing the rollback
- `skip_conflicts` (optional): Skip updates with conflicts instead of stopping (default: false)

**Response:**
```json
{
  "success": true,
  "message": "Successfully rolled back 15 of 15 updates",
  "updates_rolled_back": 15,
  "conflicts": null
}
```

## Conflict Detection

The system uses SHA-256 hashing to detect if a row has been modified since the update occurred.

### How It Works

1. **During Update**: System computes hash of updated values
2. **During Rollback**: System compares current hash with stored hash
3. **If Different**: Conflict detected - row was modified externally

### Handling Conflicts

**Option 1: Review and Decide**
```bash
# Get update detail to see what changed
GET /import-history/{import_id}/updates/{update_id}

# Review current_row vs previous_values
# Decide whether to force rollback
```

**Option 2: Force Rollback**
```json
{
  "force": true  // Override conflict detection
}
```

**Option 3: Skip Conflicting Updates**
```json
{
  "skip_conflicts": true  // Continue with non-conflicting updates
}
```

## Database Schema

### `row_updates` Table

Tracks all row updates for rollback:

```sql
CREATE TABLE row_updates (
    id SERIAL PRIMARY KEY,
    import_id UUID NOT NULL REFERENCES import_history(import_id),
    table_name VARCHAR(255) NOT NULL,
    row_id INTEGER NOT NULL,
    previous_values JSONB NOT NULL,
    new_values JSONB NOT NULL,
    updated_columns TEXT[] NOT NULL,
    current_values_hash VARCHAR(64),  -- SHA-256 for conflict detection
    updated_at TIMESTAMP DEFAULT NOW(),
    rolled_back_at TIMESTAMP,
    rolled_back_by VARCHAR(255),
    rollback_conflict BOOLEAN DEFAULT FALSE,
    rollback_conflict_details JSONB
);
```

### `import_history` Additions

New column tracks update count:

```sql
ALTER TABLE import_history
ADD COLUMN rows_updated INTEGER DEFAULT 0;
```

## Use Cases

### 1. Customer Data Maintenance

Update customer records when new information arrives:

```python
# Initial import: Basic customer data
# Later import: Updated phone numbers and addresses
duplicate_check=DuplicateCheckConfig(
    uniqueness_columns=["customer_id"],
    update_on_duplicate=True,
    update_columns=["phone", "address", "email"]
)
```

### 2. Product Catalog Updates

Keep product information current:

```python
# Daily import: Update prices and stock levels
duplicate_check=DuplicateCheckConfig(
    uniqueness_columns=["sku"],
    update_on_duplicate=True,
    update_columns=["price", "stock_quantity"]
)
```

### 3. Incremental Data Loads

Merge new data with existing records:

```python
# Weekly import: Update all fields except audit columns
duplicate_check=DuplicateCheckConfig(
    uniqueness_columns=["id"],
    update_on_duplicate=True
    # update_columns=None means update all non-empty columns
)
```

## Best Practices

### 1. Choose Appropriate Uniqueness Columns

- Use stable identifiers (ID, email, SKU)
- Avoid columns that might change (name, description)
- Consider composite keys for complex matching

### 2. Limit Update Columns

- Only update columns that should change
- Preserve audit columns (created_at, created_by)
- Exclude calculated or derived fields

### 3. Review Before Rollback

- Check `rows_updated` count in import_history
- Review update details before bulk rollback
- Use conflict detection to protect manual edits

### 4. Monitor and Audit

- Track who performs rollbacks
- Review rollback conflicts regularly
- Set up alerts for large-scale updates

### 5. Test Configuration

- Test with small datasets first
- Verify uniqueness columns match correctly
- Confirm update_columns list is correct

## Comparison: Skip vs Update

| Feature | Skip Duplicates | Update on Duplicate |
|---------|----------------|---------------------|
| **Behavior** | Leaves existing rows unchanged | Updates existing rows with new values |
| **Use Case** | Immutable data, historical records | Mutable data, current state |
| **Audit Trail** | Logs skipped records | Logs updates with before/after values |
| **Rollback** | Not applicable | Full rollback support |
| **Performance** | Slightly faster (no updates) | Slightly slower (performs updates) |
| **Data Integrity** | Preserves original data | Maintains current state |

## Troubleshooting

### Updates Not Happening

**Check configuration:**
```python
# Ensure update_on_duplicate is True
config.duplicate_check.update_on_duplicate = True

# Verify uniqueness columns exist
config.duplicate_check.uniqueness_columns = ["email"]
```

### Too Many Updates

**Limit update scope:**
```python
# Only update specific columns
config.duplicate_check.update_columns = ["phone", "address"]
```

### Rollback Conflicts

**Options:**
1. Review current values and decide
2. Force rollback if appropriate: `force=True`
3. Skip conflicting updates: `skip_conflicts=True`

### Performance Issues

**For large imports:**
- System automatically uses chunked processing (>20,000 records)
- Updates happen in parallel where possible
- Consider breaking imports into smaller batches

## Security Considerations

- Only authorized users should perform rollbacks
- Track `rolled_back_by` for audit purposes
- Review `rollback_conflict_details` for security events
- Monitor unusual update patterns

## Future Enhancements

Potential future features:
- Scheduled automatic rollbacks
- Rollback time windows (prevent old rollbacks)
- Update approval workflows
- Partial column rollbacks
- Rollback previews before execution

## Related Documentation

- [Duplicate Detection](./DUPLICATE_DETECTION.md)
- [Import Tracking](./IMPORT_TRACKING.md)
- [Data Models](./DATA_MODELS.md)
- [API Reference](./API_REFERENCE.md)
