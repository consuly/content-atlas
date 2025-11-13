# Setup Guide

Complete guide for setting up the Data Mapper API application.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Configuration](#environment-configuration)
- [Database Setup](#database-setup)
- [Running the Application](#running-the-application)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before setting up the application, ensure you have the following installed:

### Required Software

- **Python 3.8+**: The application is built with Python
- **PostgreSQL 12+**: Database for storing mapped data
- **Docker & Docker Compose** (optional): For containerized deployment
- **Git**: For cloning the repository

### Optional Software

- **pip**: Python package manager (usually comes with Python)
- **virtualenv** or **venv**: For creating isolated Python environments

### System Requirements

- **Memory**: Minimum 2GB RAM (4GB+ recommended for large file processing)
- **Disk Space**: At least 1GB free space
- **Network**: Internet connection for downloading dependencies and accessing B2 storage

---

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd content-atlas
```

### 2. Create a Virtual Environment (Recommended)

**Using venv:**
```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

**Using virtualenv:**
```bash
virtualenv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

The `requirements.txt` includes all necessary packages:
- FastAPI: Web framework
- Uvicorn: ASGI server
- SQLAlchemy: Database ORM
- Pandas: Data processing
- psycopg2-binary: PostgreSQL adapter
- python-multipart: File upload support
- b2sdk: Backblaze B2 integration
- openpyxl: Excel file support
- And more...

---

## Environment Configuration

### Required Environment Variables

Create a `.env` file in the project root or set these environment variables:

#### Database Configuration

```bash
# PostgreSQL connection details
DATABASE_URL=postgresql://user:password@localhost:5432/datamapper

# Or set individual components
DB_HOST=localhost
DB_PORT=5432
DB_NAME=datamapper
DB_USER=user
DB_PASSWORD=password
```

#### Backblaze B2 Configuration (Optional)

Required only if using B2 storage endpoints:

```bash
B2_APPLICATION_KEY_ID=your_key_id_here
B2_APPLICATION_KEY=your_application_key_here
B2_BUCKET_NAME=your_bucket_name_here
```

#### Console/LLM Configuration (Optional)

Required only if using the natural language query console:

```bash
ANTHROPIC_API_KEY=your_anthropic_api_key_here
```

#### Admin Bootstrap (Optional)

Set these when you want the application to auto-create the first admin user
during startup (recommended for production deployments):

```bash
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=super-secret-password
ADMIN_NAME=Content Atlas Admin  # optional
```

When `ADMIN_EMAIL` is present, `app/main.py` invokes
`create_admin_user_env.create_admin_user_if_not_exists()` during startup. The
script is idempotent — it only creates the user if no existing record matches
`ADMIN_EMAIL`, so it is safe to leave these variables set in production.

### Example .env File

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/datamapper

# Admin bootstrap (optional)
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=super-secret-password
ADMIN_NAME=Content Atlas Admin

# Backblaze B2 (optional)
B2_APPLICATION_KEY_ID=your_key_id
B2_APPLICATION_KEY=your_key
B2_BUCKET_NAME=your_bucket

# LLM for console (optional)
ANTHROPIC_API_KEY=your_anthropic_key
```

### Getting Backblaze B2 Credentials

1. Sign up for a Backblaze account at https://www.backblaze.com/b2/
2. Navigate to "App Keys" in your account settings
3. Create a new application key
4. Copy the Key ID and Application Key
5. Create a bucket and note its name

---

## Database Setup

### Option 1: Using Docker Compose (Recommended)

The easiest way to set up PostgreSQL:

```bash
# Start PostgreSQL container
docker-compose up -d db

# Verify it's running
docker-compose ps
```

This will:
- Start PostgreSQL on port 5432
- Create a database named `datamapper`
- Set up user `postgres` with password `postgres`
- Persist data in a Docker volume

### Option 2: Local PostgreSQL Installation

If you have PostgreSQL installed locally:

1. **Start PostgreSQL service:**
   ```bash
   # macOS (using Homebrew)
   brew services start postgresql

   # Linux (systemd)
   sudo systemctl start postgresql

   # Windows
   # Start from Services or pgAdmin
   ```

2. **Create the database:**
   ```bash
   # Connect to PostgreSQL
   psql -U postgres

   # Create database
   CREATE DATABASE datamapper;

   # Create user (if needed)
   CREATE USER datamapper_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE datamapper TO datamapper_user;

   # Exit
   \q
   ```

3. **Update your DATABASE_URL** in `.env` to match your configuration

### Database Schema

The application automatically creates tables as needed. The following system tables are created on first run:

- `file_imports`: Tracks imported files for duplicate detection
- Dynamic tables: Created based on your mapping configurations

---

## Running the Application

### Development Mode

Start the application with auto-reload enabled:

```bash
uvicorn app.main:app --reload
```

The API will be available at:
- **API**: http://localhost:8000
- **Interactive Docs**: http://localhost:8000/docs
- **Alternative Docs**: http://localhost:8000/redoc

### Production Mode

For production deployment:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

Options:
- `--host 0.0.0.0`: Listen on all network interfaces
- `--port 8000`: Port to listen on
- `--workers 4`: Number of worker processes (adjust based on CPU cores)

### Using Docker

Build and run with Docker:

```bash
# Build the image
docker build -t data-mapper .

# Run the container
docker run -p 8000:8000 --env-file .env data-mapper
```

### Using Docker Compose (Full Stack)

Start the complete stack (API + Database):

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

---

## Verification

### 1. Check API Health

Visit http://localhost:8000/docs in your browser. You should see the FastAPI interactive documentation.

### 2. Test Database Connection

The application will log database connection status on startup:

```
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Database connection established
INFO:     Application startup complete.
```

If you see connection errors, verify:
- PostgreSQL is running
- DATABASE_URL is correct
- Database exists and is accessible

### 3. Test a Simple Endpoint

Using curl:
```bash
curl http://localhost:8000/tables
```

Expected response:
```json
{
  "success": true,
  "tables": []
}
```

### 4. Test File Upload

Create a test CSV file (`test.csv`):
```csv
name,email
John Doe,john@example.com
Jane Smith,jane@example.com
```

Upload it:
```bash
curl -X POST "http://localhost:8000/map-data" \
  -F "file=@test.csv" \
  -F 'mapping_json={"table_name":"test_users","db_schema":{"name":"VARCHAR(255)","email":"VARCHAR(255)"},"mappings":{"name":"name","email":"email"}}'
```

---

## Troubleshooting

### Database Connection Issues

**Error:** `could not connect to server: Connection refused`

**Solutions:**
- Verify PostgreSQL is running: `docker-compose ps` or `pg_isready`
- Check DATABASE_URL is correct
- Ensure PostgreSQL is listening on the correct port
- Check firewall settings

### Port Already in Use

**Error:** `Address already in use`

**Solutions:**
- Stop other services using port 8000
- Use a different port: `uvicorn app.main:app --port 8001`
- Find and kill the process: `lsof -ti:8000 | xargs kill -9` (macOS/Linux)

### Module Import Errors

**Error:** `ModuleNotFoundError: No module named 'fastapi'`

**Solutions:**
- Ensure virtual environment is activated
- Reinstall dependencies: `pip install -r requirements.txt`
- Verify Python version: `python --version` (should be 3.8+)

### B2 Authentication Errors

**Error:** `B2 authentication failed`

**Solutions:**
- Verify B2 credentials are correct
- Check environment variables are set
- Ensure application key has proper permissions
- Verify bucket name is correct

### Large File Processing Issues

**Error:** `Memory error` or `Process killed`

**Solutions:**
- Increase available memory
- Use async endpoints for large files
- Reduce chunk size in configuration
- Process files in smaller batches

### Permission Errors

**Error:** `Permission denied` when accessing files

**Solutions:**
- Check file permissions
- Run with appropriate user privileges
- Verify Docker volume permissions (if using Docker)

---

## Next Steps

After successful setup:

1. **Explore the API**: Visit http://localhost:8000/docs
2. **Read the API Reference**: See [API_REFERENCE.md](API_REFERENCE.md)
3. **Test with sample data**: See [TESTING.md](TESTING.md)
4. **Configure duplicate detection**: See [DUPLICATE_DETECTION.md](DUPLICATE_DETECTION.md)
5. **Learn about console interface**: See [CONSOLE.md](CONSOLE.md)

## Related Documentation

- [API Reference](API_REFERENCE.md) - Complete API endpoint documentation
- [Testing Guide](TESTING.md) - Testing strategies and examples
- [Deployment Guide](DEPLOYMENT.md) - Production deployment instructions
- [Architecture Overview](ARCHITECTURE.md) - System design and components
