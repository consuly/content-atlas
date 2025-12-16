# Backblaze B2 Setup Guide

This guide explains how to configure Backblaze B2 storage for the Content Atlas file upload system.

## Prerequisites

1. A Backblaze account (sign up at https://www.backblaze.com/b2/sign-up.html)
2. B2 Cloud Storage enabled on your account

## Step 1: Create a B2 Bucket

1. Log in to your Backblaze account
2. Navigate to **B2 Cloud Storage** → **Buckets**
3. Click **Create a Bucket**
4. Configure your bucket:
   - **Bucket Name**: Choose a unique name (e.g., `content-atlas-uploads`)
   - **Files in Bucket**: Private (recommended for security)
   - **Default Encryption**: Enabled (recommended)
   - **Object Lock**: Disabled (unless you need compliance features)
5. Click **Create a Bucket**
6. Note the **Endpoint** URL (format: `s3.region.backblazeb2.com`)

## Step 2: Create Application Keys

1. Go to **App Keys** in the Backblaze dashboard
2. Click **Add a New Application Key**
3. Configure the key:
   - **Name**: `content-atlas-api` (or any descriptive name)
   - **Allow access to Bucket(s)**: Select your bucket
   - **Type of Access**: Read and Write
   - **Allow List All Bucket Names**: Yes (recommended)
   - **File name prefix**: Leave empty (or set to `uploads/` for extra security)
   - **Duration**: Leave empty for no expiration
4. Click **Create New Key**
5. **IMPORTANT**: Copy and save these credentials immediately (they won't be shown again):
   - **keyID** (Application Key ID)
   - **applicationKey** (Application Key)

## Step 3: Configure CORS Rules for Direct Browser Uploads

**CRITICAL**: The B2 web console's basic CORS settings are **not sufficient** for direct browser uploads using presigned URLs. You must configure custom CORS rules using the B2 CLI.

### Why Custom CORS Rules Are Required

When browsers upload files directly to B2 using presigned URLs, they send an OPTIONS preflight request before the actual PUT request. The B2 web console's simple CORS settings don't properly configure:
- The OPTIONS method for preflight requests
- Required headers like `Content-Type` and authorization headers
- Exposing the `ETag` header (needed for upload confirmation)

### Install B2 CLI

```bash
pip install b2
```

### Authorize B2 CLI

Use the Application Key credentials from Step 2:

```bash
b2 authorize-account <your_application_key_id> <your_application_key>
```

### Create CORS Rules File

Create a file named `cors_rules.json` with the following content:

**For Development (localhost):**
```json
[
  {
    "corsRuleName": "allowDirectUpload",
    "allowedOrigins": [
      "http://localhost:5173",
      "http://localhost:3000",
      "http://localhost:8000"
    ],
    "allowedOperations": [
      "s3_put",
      "s3_get",
      "s3_head"
    ],
    "allowedHeaders": ["*"],
    "exposeHeaders": ["ETag", "x-amz-meta-*"],
    "maxAgeSeconds": 3600
  }
]
```

**For Production:**
```json
[
  {
    "corsRuleName": "allowDirectUpload",
    "allowedOrigins": [
      "https://yourdomain.com",
      "https://www.yourdomain.com"
    ],
    "allowedOperations": [
      "s3_put",
      "s3_get",
      "s3_head"
    ],
    "allowedHeaders": ["*"],
    "exposeHeaders": ["ETag", "x-amz-meta-*"],
    "maxAgeSeconds": 3600
  }
]
```

**For Testing (Allow All Origins - NOT RECOMMENDED FOR PRODUCTION):**
```json
[
  {
    "corsRuleName": "allowDirectUpload",
    "allowedOrigins": ["*"],
    "allowedOperations": [
      "s3_put",
      "s3_get",
      "s3_head"
    ],
    "allowedHeaders": ["*"],
    "exposeHeaders": ["ETag", "x-amz-meta-*"],
    "maxAgeSeconds": 3600
  }
]
```

### Apply CORS Rules

Apply the CORS rules to your bucket:

```bash
b2 bucket update --cors-rules "$(cat cors_rules.json)" <your-bucket-name> allPrivate
```

**Windows PowerShell:**
```powershell
$corsRules = Get-Content cors_rules.json -Raw
b2 bucket update --cors-rules $corsRules <your-bucket-name> allPrivate
```

**Windows Command Prompt:**
```cmd
b2 bucket update --cors-rules @cors_rules.json <your-bucket-name> allPrivate
```

### Verify CORS Configuration

Check that CORS rules were applied correctly:

```bash
b2 bucket get <your-bucket-name>
```

Look for the `corsRules` section in the output. It should show your configured rules.

### Understanding CORS Rule Parameters

- **corsRuleName**: A descriptive name for the rule
- **allowedOrigins**: List of origins (domains) allowed to make requests. Use `["*"]` for all origins (development only)
- **allowedOperations**: S3 operations allowed:
  - `s3_put`: Upload files (required for direct uploads)
  - `s3_get`: Download files
  - `s3_head`: Check file existence/metadata
- **allowedHeaders**: Headers the browser can send. Use `["*"]` to allow all headers
- **exposeHeaders**: Headers the browser can read from responses. `ETag` is required for upload confirmation
- **maxAgeSeconds**: How long browsers can cache the preflight response (3600 = 1 hour)

## Step 4: Configure Environment Variables

### Backend Configuration

Create or update the `.env` file in the root directory of your project:

```bash
# Database Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/data_mapper

# Authentication
SECRET_KEY=your-secret-key-change-in-production

# Backblaze B2 Configuration
STORAGE_PROVIDER=b2
STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
STORAGE_ACCESS_KEY_ID=your_key_id_here
STORAGE_SECRET_ACCESS_KEY=your_application_key_here
STORAGE_BUCKET_NAME=your-bucket-name
STORAGE_REGION=us-west-004

# LangChain API Keys
ANTHROPIC_API_KEY=your_anthropic_key_here
```

Replace the placeholders:
- `your_key_id_here` → Your Application Key ID from Step 2
- `your_application_key_here` → Your Application Key from Step 2
- `your-bucket-name` → Your bucket name from Step 1

### Frontend Configuration

The frontend automatically uses the backend API, so no additional configuration is needed.

## Step 5: Verify Configuration

### Test B2 Connection

You can test your B2 configuration by running:

```bash
# Start the backend server
cd /path/to/content-atlas
python -m uvicorn app.main:app --reload

# In another terminal, test the upload endpoint
curl -X POST http://localhost:8000/upload-to-b2 \
  -H "Content-Type: multipart/form-data" \
  -F "file=@test.csv"
```

### Check Backend Logs

When the backend starts, you should see:
```
✓ Database tables initialized successfully
```

If B2 credentials are missing, you'll see an error when attempting to upload:
```
ValueError: B2 configuration is incomplete. Please set B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY, and B2_BUCKET_NAME in your environment.
```

## Step 6: Restart Services

After configuring the environment variables:

```bash
# If using Docker
docker-compose down
docker-compose up -d

# If running locally
# Stop the backend (Ctrl+C)
# Restart it
python -m uvicorn app.main:app --reload
```

## Troubleshooting

### CORS Error: "No 'Access-Control-Allow-Origin' header is present"

**Cause**: CORS rules are not properly configured for direct browser uploads.

**Solution**:
1. Follow Step 3 to configure custom CORS rules using the B2 CLI
2. The B2 web console's basic CORS settings are **not sufficient** for presigned URL uploads
3. Verify CORS rules are applied: `b2 bucket get <your-bucket-name>`
4. Clear browser cache and try uploading again
5. Check browser console for the exact error message

**Common CORS Issues:**
- **Web UI CORS settings don't work**: You must use the B2 CLI to set custom CORS rules
- **Wrong origin**: Ensure your frontend URL (e.g., `http://localhost:5173`) is in `allowedOrigins`
- **Missing operations**: Must include `s3_put` in `allowedOperations`
- **Missing headers**: Use `["*"]` for `allowedHeaders` to allow all headers
- **Missing ETag exposure**: Must include `"ETag"` in `exposeHeaders`


### Error: "B2 configuration is incomplete"

**Cause**: Environment variables are not set or not loaded properly.

**Solution**:
1. Verify `.env` file exists in the project root
2. Check that variable names match exactly (case-sensitive)
3. Restart the backend server
4. Verify with: `echo $B2_APPLICATION_KEY_ID` (Linux/Mac) or `echo %B2_APPLICATION_KEY_ID%` (Windows)

### Error: "Invalid authentication token"

**Cause**: Application Key ID or Application Key is incorrect.

**Solution**:
1. Verify credentials in Backblaze dashboard
2. Create a new Application Key if needed
3. Update `.env` file with correct credentials
4. Restart backend

### Error: "Bucket not found"

**Cause**: Bucket name is incorrect or doesn't exist.

**Solution**:
1. Verify bucket name in Backblaze dashboard (case-sensitive)
2. Ensure the Application Key has access to this bucket
3. Update `B2_BUCKET_NAME` in `.env`
4. Restart backend

### Files Upload Successfully but Don't Appear in B2

**Cause**: B2 credentials are not configured, causing the upload to fail silently.

**Solution**:
1. Check backend logs for errors
2. Verify all three B2 environment variables are set
3. Test B2 connection using the verification steps above
4. Check that the Application Key has write permissions

### Permission Denied Errors

**Cause**: Application Key doesn't have sufficient permissions.

**Solution**:
1. Go to Backblaze dashboard → App Keys
2. Delete the old key
3. Create a new key with "Read and Write" access
4. Update `.env` with new credentials
5. Restart backend

## Security Best Practices

1. **Never commit `.env` files** to version control
2. **Use different keys** for development and production
3. **Set file name prefix** to `uploads/` in Application Key settings
4. **Enable bucket encryption** for sensitive data
5. **Rotate keys regularly** (every 90 days recommended)
6. **Use private buckets** unless public access is required
7. **Monitor usage** in Backblaze dashboard for unusual activity

## File Organization in B2

Files are organized in the following structure:

```
your-bucket-name/
└── uploads/
    ├── file1.csv
    ├── file2.xlsx
    └── file3.csv
```

All uploaded files are stored in the `uploads/` folder within your bucket.

## Cost Considerations

Backblaze B2 pricing (as of 2024):
- **Storage**: $0.005/GB/month (first 10GB free)
- **Download**: $0.01/GB (first 1GB/day free)
- **Upload**: Free
- **API Calls**: Free (Class C transactions)

For typical usage with CSV/Excel files:
- 1000 files × 1MB each = 1GB storage = $0.005/month
- Very cost-effective for data import workflows

## Additional Resources

- [Backblaze B2 Documentation](https://www.backblaze.com/b2/docs/)
- [B2 Python SDK Documentation](https://b2-sdk-python.readthedocs.io/)
- [B2 Pricing Calculator](https://www.backblaze.com/b2/cloud-storage-pricing.html)
- [Content Atlas API Reference](./API_REFERENCE.md)

## Support

If you continue to experience issues:
1. Check the backend logs for detailed error messages
2. Verify all environment variables are set correctly
3. Test B2 credentials using the Backblaze web interface
4. Review the [API Reference](./API_REFERENCE.md) for endpoint details
