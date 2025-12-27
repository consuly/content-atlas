# S3-Compatible Storage Setup

Content Atlas uses S3-compatible storage for file uploads. This guide covers setup for multiple storage providers.

## Supported Storage Providers

- **Backblaze B2** (S3-compatible API)
- **AWS S3**
- **MinIO** (self-hosted)
- **Wasabi**
- **DigitalOcean Spaces**
- Any S3-compatible storage service

---

## Configuration

All storage providers use the same environment variables in your `.env` file:

```env
# Storage Provider Configuration
STORAGE_PROVIDER=b2                                    # Provider name (for reference)
STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com  # S3 endpoint (leave empty for AWS S3)
STORAGE_ACCESS_KEY_ID=your_access_key_id              # Access key / Application Key ID
STORAGE_SECRET_ACCESS_KEY=your_secret_access_key      # Secret key / Application Key
STORAGE_BUCKET_NAME=your-bucket-name                  # Bucket name
STORAGE_REGION=us-west-004                            # Region
```

---

## Provider-Specific Setup

### Backblaze B2

1. **Create a B2 Account**
   - Sign up at [backblaze.com/b2](https://www.backblaze.com/b2/cloud-storage.html)

2. **Create a Bucket**
   - Go to "Buckets" → "Create a Bucket"
   - Choose a unique bucket name
   - Set to **Private** (recommended for security)
   - Note the region (e.g., `us-west-004`)

3. **Create Application Key**
   - Go to "App Keys" → "Add a New Application Key"
   - Name: `content-atlas-app-key`
   - Access: **Read and Write** to your bucket
   - Copy the **Application Key ID** and **Application Key**

4. **Configure Environment Variables**
   ```env
   STORAGE_PROVIDER=b2
   STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
   STORAGE_ACCESS_KEY_ID=<your_application_key_id>
   STORAGE_SECRET_ACCESS_KEY=<your_application_key>
   STORAGE_BUCKET_NAME=<your_bucket_name>
   STORAGE_REGION=us-west-004
   ```

   **Important:** Replace `us-west-004` with your bucket's region.

---

### AWS S3

1. **Create an S3 Bucket**
   - Go to AWS Console → S3 → "Create bucket"
   - Choose a unique bucket name
   - Select a region (e.g., `us-east-1`)
   - Block public access (recommended)

2. **Create IAM User**
   - Go to IAM → Users → "Add user"
   - Enable "Programmatic access"
   - Attach policy: `AmazonS3FullAccess` (or create custom policy)
   - Save the **Access Key ID** and **Secret Access Key**

3. **Configure Environment Variables**
   ```env
   STORAGE_PROVIDER=s3
   STORAGE_ENDPOINT_URL=                              # Leave empty for AWS S3
   STORAGE_ACCESS_KEY_ID=<your_aws_access_key_id>
   STORAGE_SECRET_ACCESS_KEY=<your_aws_secret_key>
   STORAGE_BUCKET_NAME=<your_bucket_name>
   STORAGE_REGION=us-east-1
   ```

---

### MinIO (Self-Hosted)

1. **Install MinIO**
   - Follow [MinIO installation guide](https://min.io/docs/minio/linux/operations/installation.html)
   - Start MinIO server: `minio server /data`

2. **Create a Bucket**
   - Access MinIO Console (default: `http://localhost:9001`)
   - Create a new bucket

3. **Create Access Keys**
   - Go to "Access Keys" → "Create Access Key"
   - Save the **Access Key** and **Secret Key**

4. **Configure Environment Variables**
   ```env
   STORAGE_PROVIDER=minio
   STORAGE_ENDPOINT_URL=http://localhost:9000          # MinIO endpoint
   STORAGE_ACCESS_KEY_ID=<your_minio_access_key>
   STORAGE_SECRET_ACCESS_KEY=<your_minio_secret_key>
   STORAGE_BUCKET_NAME=<your_bucket_name>
   STORAGE_REGION=us-east-1                            # Can be any value for MinIO
   ```

---

### Wasabi

1. **Create a Wasabi Account**
   - Sign up at [wasabi.com](https://wasabi.com/)

2. **Create a Bucket**
   - Go to "Buckets" → "Create Bucket"
   - Choose a region (e.g., `us-east-1`)

3. **Create Access Keys**
   - Go to "Access Keys" → "Create New Access Key"
   - Save the **Access Key** and **Secret Key**

4. **Configure Environment Variables**
   ```env
   STORAGE_PROVIDER=wasabi
   STORAGE_ENDPOINT_URL=https://s3.us-east-1.wasabisys.com
   STORAGE_ACCESS_KEY_ID=<your_wasabi_access_key>
   STORAGE_SECRET_ACCESS_KEY=<your_wasabi_secret_key>
   STORAGE_BUCKET_NAME=<your_bucket_name>
   STORAGE_REGION=us-east-1
   ```

---

### DigitalOcean Spaces

1. **Create a Space**
   - Go to DigitalOcean Console → Spaces → "Create Space"
   - Choose a region (e.g., `nyc3`)

2. **Generate API Keys**
   - Go to API → Spaces Keys → "Generate New Key"
   - Save the **Access Key** and **Secret Key**

3. **Configure Environment Variables**
   ```env
   STORAGE_PROVIDER=digitalocean
   STORAGE_ENDPOINT_URL=https://nyc3.digitaloceanspaces.com
   STORAGE_ACCESS_KEY_ID=<your_spaces_access_key>
   STORAGE_SECRET_ACCESS_KEY=<your_spaces_secret_key>
   STORAGE_BUCKET_NAME=<your_space_name>
   STORAGE_REGION=nyc3
   ```

---

## Testing Your Configuration

After configuring your storage provider, test the connection:

```bash
# Start the backend
uvicorn app.main:app --reload

# Upload a test file via the API or frontend
# Check logs for any storage connection errors
```

---

## CORS Configuration (Required for Direct Uploads)

For direct browser-to-storage uploads, you need to configure CORS on your bucket.

### Backblaze B2 CORS

**CRITICAL**: The B2 web console's basic CORS settings are **not sufficient** for direct browser uploads using presigned URLs. You must configure custom CORS rules using the B2 CLI.

#### Why Custom CORS Rules Are Required

When browsers upload files directly to B2 using presigned URLs, they send an OPTIONS preflight request before the actual PUT request. The B2 web console's simple CORS settings don't properly configure:
- The OPTIONS method for preflight requests
- Required headers like `Content-Type` and authorization headers
- Exposing the `ETag` header (needed for upload confirmation)

#### Install B2 CLI

```bash
pip install b2
```

#### Authorize B2 CLI

Use the Application Key credentials:

```bash
b2 authorize-account <your_application_key_id> <your_application_key>
```

#### Create CORS Rules File

Create a file named `cors_rules.json` with the following content:

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

#### Apply CORS Rules

Apply the CORS rules to your bucket:

```bash
b2 bucket update --cors-rules "$(cat cors_rules.json)" <your-bucket-name> allPrivate
```

**Windows PowerShell:**
```powershell
$corsRules = Get-Content cors_rules.json -Raw
b2 bucket update --cors-rules $corsRules <your-bucket-name> allPrivate
```

#### Verify CORS Configuration

Check that CORS rules were applied correctly:

```bash
b2 bucket get <your-bucket-name>
```

Look for the `corsRules` section in the output.

### AWS S3 CORS

1. Go to your bucket → Permissions → CORS
2. Add CORS configuration:
   ```json
   [
     {
       "AllowedOrigins": [
         "http://localhost:5173",
         "http://localhost:3000",
         "https://your-production-domain.com"
       ],
       "AllowedMethods": ["PUT"],
       "AllowedHeaders": ["*"],
       "ExposeHeaders": ["ETag"],
       "MaxAgeSeconds": 3600
     }
   ]
   ```

### MinIO CORS

MinIO CORS is configured via `mc` (MinIO Client):

```bash
mc cors set content-atlas-bucket --allow-origin "http://localhost:5173" --allow-methods "PUT" --allow-headers "*"
```

---

## Security Best Practices

1. **Use Private Buckets** - Never make your bucket public
2. **Restrict Access Keys** - Limit permissions to only what's needed
3. **Rotate Keys Regularly** - Change access keys periodically
4. **Use HTTPS** - Always use secure endpoints in production
5. **Enable Versioning** - Protect against accidental deletions (optional)
6. **Monitor Usage** - Set up alerts for unusual activity

---

## Troubleshooting

### Connection Errors

**Error:** `Failed to connect to storage`

- Verify `STORAGE_ENDPOINT_URL` is correct
- Check network connectivity to the endpoint
- Ensure credentials are valid

### Upload Failures

**Error:** `Upload failed: 403 Forbidden`

- Verify access key has write permissions
- Check bucket name is correct
- Ensure CORS is configured properly

### CORS Errors (Browser Console)

**Error:** `CORS policy: No 'Access-Control-Allow-Origin' header`

- Add your frontend URL to CORS allowed origins
- Ensure CORS exposes the `ETag` header
- Clear browser cache and retry

### Persistent CORS Issues During Development/Testing

If you've configured CORS correctly but still experience upload failures, you can use **proxied upload mode** as a workaround:

#### Option 1: Use the Automated CORS Configuration Script (Recommended)

Run the convenience script to configure B2 CORS automatically:

```bash
# For development (localhost)
python configure_b2_cors.py --bucket-name content-atlas --environment dev

# For testing (allow all origins - NOT for production)
python configure_b2_cors.py --bucket-name content-atlas --environment test
```

#### Option 2: Enable Proxied Upload Mode

If CORS configuration doesn't work or you need a quick workaround for testing:

1. **Update your `.env` file** (both root and `frontend/.env` if it exists):
   ```env
   VITE_UPLOAD_MODE=proxied
   ```

2. **Restart the frontend development server**:
   ```bash
   cd frontend
   npm run dev
   ```

3. **How it works**:
   - `proxied`: Files upload through the FastAPI backend (bypasses CORS)
   - `direct`: Files upload directly from browser to B2 (requires CORS)

**Proxied mode is ideal for**:
- Local development without CORS setup
- Running automated tests
- CI/CD pipelines
- Environments where CORS configuration is difficult

**Direct mode is ideal for**:
- Production deployments (better performance)
- When CORS is properly configured
- Reducing backend load for large files

To switch back to direct uploads, change `VITE_UPLOAD_MODE=direct` and restart the frontend.

---

## Migration from B2-Specific Setup

If you're migrating from the old B2-specific configuration:

1. **Update Environment Variables**
   ```env
   # Old (deprecated)
   B2_APPLICATION_KEY_ID=...
   B2_APPLICATION_KEY=...
   B2_BUCKET_NAME=...
   
   # New (S3-compatible)
   STORAGE_PROVIDER=b2
   STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
   STORAGE_ACCESS_KEY_ID=...
   STORAGE_SECRET_ACCESS_KEY=...
   STORAGE_BUCKET_NAME=...
   STORAGE_REGION=us-west-004
   ```

2. **Install boto3**
   ```bash
   pip install boto3
   ```

3. **Restart the application**
   ```bash
   uvicorn app.main:app --reload
   ```

Your existing files in B2 will continue to work - no data migration needed!

---

## Cost Comparison

| Provider | Storage (per GB/month) | Download (per GB) | Free Tier |
|----------|------------------------|-------------------|-----------|
| **Backblaze B2** | $0.005 | $0.01 (first 3x free) | 10 GB storage |
| **AWS S3** | $0.023 | $0.09 | 5 GB storage, 20K requests |
| **Wasabi** | $0.0059 | Free | 1 TB minimum |
| **DigitalOcean** | $0.02 | $0.01 | 250 GB included |
| **MinIO** | Self-hosted | Self-hosted | Free (open source) |

*Prices as of 2024, subject to change*

---

## Additional Resources

- [Backblaze B2 S3 API Documentation](https://www.backblaze.com/b2/docs/s3_compatible_api.html)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
