# API Key Authentication

This document describes the API key authentication system for Content Atlas, which enables secure access for external applications.

## Overview

Content Atlas uses a dual authentication approach:
- **JWT Tokens**: For frontend users (email/password login)
- **API Keys**: For external applications (server-to-server)

This separation ensures:
- Users have short-lived tokens with refresh capability
- Applications have long-lived credentials for automated access
- Clear audit trail for both user and application actions

## Architecture

### Database Schema

```sql
CREATE TABLE api_keys (
    id VARCHAR PRIMARY KEY,              -- UUID
    key_hash VARCHAR UNIQUE NOT NULL,    -- SHA-256 hash of API key
    app_name VARCHAR NOT NULL,           -- Application name
    description TEXT,                    -- Optional description
    created_by INTEGER,                  -- FK to users.id
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,             -- Updated on each use
    expires_at TIMESTAMP,               -- Optional expiration
    is_active BOOLEAN DEFAULT TRUE,     -- Can be revoked
    rate_limit_per_minute INTEGER DEFAULT 60,
    allowed_endpoints JSON              -- Optional endpoint restrictions
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
```

### API Key Format

```
atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
│      │    │  └─────────────────────────────┘
│      │    │           Random portion (32 chars)
│      │    └─ Key type (sk = secret key)
│      └────── Environment (live/test)
└─────────── Prefix (atlas)
```

## Admin Endpoints (JWT Protected)

### Create API Key

**POST** `/admin/api-keys`

Creates a new API key. The plain key is only shown once.

**Authentication**: Bearer token (JWT)

**Request Body**:
```json
{
  "app_name": "Mobile App",
  "description": "iOS and Android app access",
  "expires_in_days": 365,
  "rate_limit_per_minute": 100,
  "allowed_endpoints": ["/api/v1/query", "/api/v1/tables"]
}
```

**Response**:
```json
{
  "success": true,
  "message": "API key created successfully. Save this key securely - it won't be shown again.",
  "api_key": "atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "key_id": "550e8400-e29b-41d4-a716-446655440000",
  "app_name": "Mobile App",
  "expires_at": "2025-12-31T23:59:59Z"
}
```

### List API Keys

**GET** `/admin/api-keys?is_active=true&limit=100&offset=0`

Lists all API keys (without actual key values).

**Authentication**: Bearer token (JWT)

**Response**:
```json
{
  "success": true,
  "api_keys": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "app_name": "Mobile App",
      "description": "iOS and Android app access",
      "created_at": "2025-01-01T00:00:00Z",
      "last_used_at": "2025-01-15T10:30:00Z",
      "expires_at": "2025-12-31T23:59:59Z",
      "is_active": true,
      "rate_limit_per_minute": 100,
      "allowed_endpoints": ["/api/v1/query"],
      "key_preview": "...o5p6"
    }
  ],
  "total_count": 1,
  "limit": 100,
  "offset": 0
}
```

### Revoke API Key

**DELETE** `/admin/api-keys/{key_id}`

Revokes (deactivates) an API key immediately.

**Authentication**: Bearer token (JWT)

**Response**:
```json
{
  "success": true,
  "message": "API key revoked successfully",
  "key_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Update API Key

**PATCH** `/admin/api-keys/{key_id}`

Updates API key settings (cannot change the key itself).

**Authentication**: Bearer token (JWT)

**Request Body**:
```json
{
  "description": "Updated description",
  "rate_limit_per_minute": 200,
  "is_active": true
}
```

## Public API Endpoints (API Key Protected)

### Query Database

**POST** `/api/v1/query`

Execute natural language queries against the database.

**Authentication**: `X-API-Key` header

**Request**:
```bash
curl -X POST https://api.yourdomain.com/api/v1/query \
  -H "X-API-Key: atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "Show all customers from California",
    "thread_id": "optional-conversation-id"
  }'
```

**Response**:
```json
{
  "success": true,
  "response": "Found 42 customers from California",
  "executed_sql": "SELECT * FROM customers WHERE state = 'CA'",
  "data_csv": "id,name,state\n1,John Doe,CA\n...",
  "execution_time_seconds": 0.15,
  "rows_returned": 42
}
```

### List Tables

**GET** `/api/v1/tables`

List all available tables.

**Authentication**: `X-API-Key` header

**Request**:
```bash
curl https://api.yourdomain.com/api/v1/tables \
  -H "X-API-Key: atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
```

**Response**:
```json
{
  "success": true,
  "tables": [
    {
      "table_name": "customers",
      "row_count": 1250
    },
    {
      "table_name": "orders",
      "row_count": 5430
    }
  ]
}
```

### Get Table Schema

**GET** `/api/v1/tables/{table_name}/schema`

Get column information for a specific table.

**Authentication**: `X-API-Key` header

**Request**:
```bash
curl https://api.yourdomain.com/api/v1/tables/customers/schema \
  -H "X-API-Key: atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
```

**Response**:
```json
{
  "success": true,
  "table_name": "customers",
  "columns": [
    {
      "name": "id",
      "type": "integer",
      "nullable": false
    },
    {
      "name": "name",
      "type": "character varying",
      "nullable": false
    }
  ]
}
```

## Security Features

### 1. Key Storage
- Only SHA-256 hashes stored in database
- Plain keys never stored or logged
- Keys shown only once at creation

### 2. Key Validation
- Fast hash-based lookup
- Automatic expiration checking
- Active status verification

### 3. Usage Tracking
- `last_used_at` updated on each request
- Enables usage monitoring and anomaly detection

### 4. Rate Limiting
- Configurable per-key limits
- Default: 60 requests/minute
- Can be adjusted per application needs

### 5. Endpoint Restrictions
- Optional whitelist of allowed endpoints
- Granular access control per application

### 6. Revocation
- Instant deactivation
- No need to wait for expiration
- Audit trail maintained

## Integration Examples

### Python

```python
import requests

API_KEY = "atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
BASE_URL = "https://api.yourdomain.com"

headers = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}

# Query database
response = requests.post(
    f"{BASE_URL}/api/v1/query",
    headers=headers,
    json={"prompt": "Show top 10 customers by revenue"}
)

data = response.json()
print(data["response"])
```

### JavaScript/Node.js

```javascript
const API_KEY = "atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6";
const BASE_URL = "https://api.yourdomain.com";

async function queryDatabase(prompt) {
  const response = await fetch(`${BASE_URL}/api/v1/query`, {
    method: "POST",
    headers: {
      "X-API-Key": API_KEY,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ prompt })
  });
  
  return await response.json();
}

// Usage
const result = await queryDatabase("Show all active users");
console.log(result.response);
```

### cURL

```bash
# Query database
curl -X POST https://api.yourdomain.com/api/v1/query \
  -H "X-API-Key: atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Show sales by month"}'

# List tables
curl https://api.yourdomain.com/api/v1/tables \
  -H "X-API-Key: atlas_live_sk_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
```

## Best Practices

### For Administrators

1. **Key Management**
   - Create separate keys for each application
   - Use descriptive names and descriptions
   - Set appropriate expiration dates
   - Review and revoke unused keys regularly

2. **Security**
   - Never commit keys to version control
   - Use environment variables for key storage
   - Rotate keys periodically (e.g., annually)
   - Monitor `last_used_at` for anomalies

3. **Rate Limiting**
   - Set conservative limits initially
   - Increase based on actual usage patterns
   - Monitor for abuse or unusual patterns

### For Developers

1. **Key Storage**
   ```bash
   # .env file (never commit!)
   ATLAS_API_KEY=atlas_live_sk_...
   ```

2. **Error Handling**
   ```python
   try:
       response = requests.post(url, headers=headers, json=data)
       response.raise_for_status()
   except requests.exceptions.HTTPError as e:
       if e.response.status_code == 401:
           print("Invalid API key")
       elif e.response.status_code == 429:
           print("Rate limit exceeded")
   ```

3. **Key Rotation**
   - Support multiple keys during rotation
   - Implement graceful fallback
   - Update keys without downtime

## Error Responses

### 401 Unauthorized
```json
{
  "detail": "API key required. Provide X-API-Key header."
}
```

### 401 Invalid Key
```json
{
  "detail": "Invalid or expired API key"
}
```

### 429 Rate Limit
```json
{
  "detail": "Rate limit exceeded. Try again later."
}
```

## Monitoring and Auditing

### Key Metrics to Track

1. **Usage Patterns**
   - Requests per key per day
   - Peak usage times
   - Most queried endpoints

2. **Security Events**
   - Failed authentication attempts
   - Revoked key usage attempts
   - Unusual access patterns

3. **Performance**
   - Average response times per key
   - Error rates per application
   - Rate limit hits

### Database Queries

```sql
-- Most active API keys
SELECT app_name, last_used_at, 
       COUNT(*) as request_count
FROM api_keys
WHERE is_active = true
GROUP BY app_name, last_used_at
ORDER BY request_count DESC;

-- Expired but active keys (should be revoked)
SELECT id, app_name, expires_at
FROM api_keys
WHERE is_active = true
  AND expires_at < NOW();

-- Unused keys (potential cleanup)
SELECT id, app_name, created_at, last_used_at
FROM api_keys
WHERE is_active = true
  AND (last_used_at IS NULL OR last_used_at < NOW() - INTERVAL '90 days');
```

## Migration Guide

### From No Authentication

1. Create API keys for existing integrations
2. Update client applications with keys
3. Deploy updated applications
4. Enable authentication requirement
5. Monitor for authentication errors

### Key Rotation Process

1. Create new API key
2. Update application configuration
3. Deploy application with new key
4. Verify new key works
5. Revoke old key
6. Monitor for errors

## Troubleshooting

### "API key required" Error
- Ensure `X-API-Key` header is present
- Check header name spelling (case-sensitive)
- Verify key is included in request

### "Invalid or expired API key" Error
- Verify key hasn't been revoked
- Check expiration date
- Ensure key is copied correctly (no extra spaces)

### Rate Limit Errors
- Check current rate limit setting
- Implement exponential backoff
- Request limit increase if needed

## Future Enhancements

Potential improvements for future versions:

1. **Scoped Permissions**
   - Read-only vs read-write access
   - Table-level permissions
   - Column-level restrictions

2. **Advanced Rate Limiting**
   - Sliding window algorithm
   - Burst allowance
   - Per-endpoint limits

3. **Key Rotation**
   - Automatic rotation reminders
   - Overlapping validity periods
   - Zero-downtime rotation

4. **Analytics Dashboard**
   - Real-time usage metrics
   - Cost tracking per application
   - Performance insights

5. **Webhook Notifications**
   - Key expiration warnings
   - Unusual activity alerts
   - Rate limit notifications
