# Deployment Guide

Guide for deploying the ContentAtlas API in production environments.

## Table of Contents

- [Overview](#overview)
- [Railway Deployment](#railway-deployment)
- [Docker Deployment](#docker-deployment)
- [Production Configuration](#production-configuration)
- [Monitoring and Logging](#monitoring-and-logging)
- [Security Considerations](#security-considerations)
- [Performance Optimization](#performance-optimization)
- [Backup and Recovery](#backup-and-recovery)

---

## Overview

This guide covers deploying the ContentAtlas API in production environments using Docker and Docker Compose.

### Deployment Options

1. **New Server Setup**: See the [Detailed Server Setup Tutorial](#detailed-server-setup-tutorial) below.
2. **Docker Compose** (Recommended): Complete stack with database
3. **Docker Container**: API only, external database
4. **Kubernetes**: Scalable container orchestration
5. **Cloud Platforms**: AWS, GCP, Azure deployment

---

## Detailed Server Setup Tutorial

This guide provides a step-by-step tutorial for deploying the Content Atlas full stack (Frontend, Backend, and Database) on a new server using Docker Compose.

### Prerequisites

- A clean server (Ubuntu 20.04/22.04 LTS recommended)
- Root or sudo access
- A domain name pointing to your server's IP address

### 1. Install Docker & Docker Compose

First, ensure your server has the latest version of Docker and Docker Compose installed.

```bash
# Update package index
sudo apt-get update

# Install prerequisites
sudo apt-get install -y ca-certificates curl gnupg

# Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Set up the repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Verify installation
sudo docker run hello-world
```

### 2. Clone the Repository

Clone the project to your server (e.g., in `/opt/content-atlas` or your home directory).

```bash
git clone https://github.com/thefoundry-app/content-atlas.git
cd content-atlas
```

### 3. Configure Environment Variables

Create a production `.env` file for the backend.

```bash
cp .env.example .env.production
nano .env.production
```

**Essential variables to configure:**

```bash
# Database Credentials
DB_USER=content_atlas_prod
DB_PASSWORD=your_secure_password
DATABASE_URL=postgresql://${DB_USER}:${DB_PASSWORD}@db:5432/datamapper

# Backblaze B2 Storage (Required for file storage)
STORAGE_PROVIDER=b2
STORAGE_ENDPOINT_URL=https://s3.us-west-004.backblazeb2.com
STORAGE_ACCESS_KEY_ID=your_key_id
STORAGE_SECRET_ACCESS_KEY=your_app_key
STORAGE_BUCKET_NAME=your_bucket_name

# LLM Integration (Optional but recommended)
ANTHROPIC_API_KEY=your_anthropic_key

# Security
SECRET_KEY=generate_a_long_random_string
ALLOWED_ORIGINS=https://your-domain.com
```

### 4. Create Production Docker Compose File

Create a file named `docker-compose.prod.yml`. This configuration defines the full stack: Nginx (proxy), Frontend, Backend (API), and Database.

```bash
nano docker-compose.prod.yml
```

Paste the following content:

```yaml
version: '3.8'

services:
  # Database Service
  db:
    image: postgres:15-alpine
    container_name: content-atlas-db
    restart: always
    environment:
      POSTGRES_DB: datamapper
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - app-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Backend API Service
  api:
    build: 
      context: .
      dockerfile: Dockerfile
    container_name: content-atlas-api
    restart: always
    env_file:
      - .env.production
    environment:
      DATABASE_URL: postgresql://${DB_USER}:${DB_PASSWORD}@db:5432/datamapper
    depends_on:
      db:
        condition: service_healthy
    networks:
      - app-network
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

  # Frontend Service
  frontend:
    build:
      context: .
      dockerfile: frontend/Dockerfile
    container_name: content-atlas-frontend
    restart: always
    environment:
      - VITE_API_URL=/api
    networks:
      - app-network

  # Nginx Reverse Proxy
  nginx:
    image: nginx:alpine
    container_name: content-atlas-nginx
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - api
      - frontend
    networks:
      - app-network

volumes:
  postgres_data:

networks:
  app-network:
    driver: bridge
```

### 5. Configure Nginx

Create the Nginx configuration file to route traffic correctly.

```bash
nano nginx.conf
```

Paste the following configuration:

```nginx
events {
    worker_connections 1024;
}

http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;
    
    # Optimization
    sendfile on;
    keepalive_timeout 65;
    client_max_body_size 100M;  # Allow large file uploads

    upstream api {
        server api:8000;
    }

    upstream frontend {
        server frontend:3000;
    }

    server {
        listen 80;
        server_name _;  # Catch all, or replace with your domain

        # Proxy /api requests to Backend
        location /api {
            # Rewrite /api/v1/... to /api/v1/... (no rewrite needed if app expects /api prefix)
            # However, the app mounts router_v1 at /api/v1.
            # If we forward /api/* to the backend, it matches the router path.
            
            proxy_pass http://api;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            
            # Timeouts for long-running imports
            proxy_connect_timeout 300s;
            proxy_send_timeout 300s;
            proxy_read_timeout 300s;
        }

        # Proxy Docs and OpenAPI to Backend
        location /docs {
            proxy_pass http://api;
            proxy_set_header Host $host;
        }
        
        location /openapi.json {
            proxy_pass http://api;
            proxy_set_header Host $host;
        }

        # Proxy everything else to Frontend
        location / {
            proxy_pass http://frontend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
```

> **Note on SSL**: For production, you should obtain an SSL certificate (e.g., via Certbot) and configure Nginx to listen on 443. See the [Production Configuration](#production-configuration) section below for details. For initial setup, the config above listens on port 80 (HTTP).

### 6. Create SSL Directory (Placeholder)

Create the directory for SSL certificates to avoid Docker errors, even if empty initially.

```bash
mkdir -p ssl
```

### 7. Deploy

Start the services using the production compose file.

```bash
sudo docker compose -f docker-compose.prod.yml up -d --build
```

Monitor the logs to ensure everything starts correctly:

```bash
sudo docker compose -f docker-compose.prod.yml logs -f
```

### 8. Final Setup

#### Create Admin User
1. Open your browser and navigate to `http://your-server-ip`.
2. You should see the login page.
3. Click "Register" to create a new account.
4. **Important**: The first account created on a fresh database is automatically assigned **Admin** privileges.

#### Verify System
1. Log in with your new admin account.
2. Go to the "Imports" section.
3. Upload a small test CSV to verify the database and file storage connection.

### Troubleshooting

- **502 Bad Gateway**: Usually means the API or Frontend container is not ready yet. Check logs with `docker compose logs -f`.
- **CORS Errors**: Ensure `ALLOWED_ORIGINS` in `.env.production` includes your domain (e.g., `https://your-domain.com`).
- **Database Connection Error**: Check `DB_USER` and `DB_PASSWORD` in `.env.production` match what is in `docker-compose.prod.yml`.

---

## Railway Deployment

Railway can host multiple services that share this monorepo (FastAPI backend + Refine frontend). By default it looks for a `Dockerfile` in the repository root, so you must point the frontend service at `frontend/Dockerfile` to avoid deploying the backend container twice.

### 1. Create/confirm services

1. In the Railway project, create two services that both reference this GitHub repository (or deploy once with `railway up` to create them automatically).
2. Keep the backend service attached to the root `Dockerfile`.
3. Use the second service for the frontend build (`frontend/Dockerfile`).

### 2. Override the Dockerfile path for the frontend service

Railway lets you override the Dockerfile location via the `RAILWAY_DOCKERFILE_PATH` environment variable. Set it only on the frontend service:

```bash
railway service frontend
railway env set RAILWAY_DOCKERFILE_PATH=frontend/Dockerfile
```

The same variable can be set in the Dashboard → Variables tab if you prefer the UI. You may also set `RAILWAY_DOCKERFILE_PATH=Dockerfile` on the backend service to make the intent explicit.

### 3. Run builds inside `frontend/`

Tell the frontend service to execute its build and start commands inside that directory so the GitHub integration does not try to run Node tooling from the root:

- **Build command:** `cd frontend && npm ci && npm run build`
- **Start command (if needed):** `cd frontend && npm run start` or the equivalent command in your Dockerfile.

### 4. CLI fallback when GitHub limits apply

If you prefer not to rely on GitHub-triggered deploys, run the CLI in each directory and push artifacts manually:

```bash
# Backend container
railway up

# Frontend container
cd frontend
railway up
```

`railway up` only uploads the current working directory, so the correct Dockerfile is used automatically.

Once the environment variable and commands are set, re-trigger a deployment. Railway will now build the backend from the root `Dockerfile` and the frontend from `frontend/Dockerfile` without splitting the repository.

---

## Docker Deployment

### Building the Docker Image

Build the application image:

```bash
docker build -t data-mapper:latest .
```

**Build with specific tag:**
```bash
docker build -t data-mapper:v1.0.1 .
```

**Build with build arguments:**
```bash
docker build \
  --build-arg PYTHON_VERSION=3.11 \
  -t data-mapper:latest .
```

### Running a Single Container

Run the API container with an external database:

```bash
docker run -d \
  --name data-mapper-api \
  -p 8000:8000 \
  -e DATABASE_URL="postgresql://user:pass@db-host:5432/datamapper" \
  -e STORAGE_PROVIDER="b2" \
  -e STORAGE_ENDPOINT_URL="https://s3.us-west-004.backblazeb2.com" \
  -e STORAGE_ACCESS_KEY_ID="your_key_id" \
  -e STORAGE_SECRET_ACCESS_KEY="your_key" \
  -e STORAGE_BUCKET_NAME="your_bucket" \
  -e STORAGE_REGION="us-west-004" \
  data-mapper:latest
```

### Docker Compose Deployment

#### Complete Stack

Deploy the full stack (API + Database):

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f api

# Stop services
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v
```

#### Production docker-compose.yml

Create a production-ready `docker-compose.prod.yml`:

```yaml
version: '3.8'

services:
  db:
    image: postgres:15-alpine
    container_name: datamapper-db
    restart: always
    environment:
      POSTGRES_DB: datamapper
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - datamapper-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${DB_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  api:
    image: data-mapper:latest
    container_name: datamapper-api
    restart: always
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://${DB_USER}:${DB_PASSWORD}@db:5432/datamapper
      B2_APPLICATION_KEY_ID: ${B2_APPLICATION_KEY_ID}
      B2_APPLICATION_KEY: ${B2_APPLICATION_KEY}
      B2_BUCKET_NAME: ${B2_BUCKET_NAME}
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    depends_on:
      db:
        condition: service_healthy
    networks:
      - datamapper-network
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4

  nginx:
    image: nginx:alpine
    container_name: datamapper-nginx
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - api
    networks:
      - datamapper-network

volumes:
  postgres_data:
    driver: local

networks:
  datamapper-network:
    driver: bridge
```

**Deploy with production config:**
```bash
docker-compose -f docker-compose.prod.yml up -d
```

---

## Production Configuration

### Environment Variables

Create a `.env.production` file:

```bash
# Database
DB_USER=datamapper_prod
DB_PASSWORD=strong_secure_password_here
DATABASE_URL=postgresql://${DB_USER}:${DB_PASSWORD}@db:5432/datamapper

# Backblaze B2
B2_APPLICATION_KEY_ID=your_production_key_id
B2_APPLICATION_KEY=your_production_key
B2_BUCKET_NAME=your_production_bucket

# LLM (Optional)
ANTHROPIC_API_KEY=your_production_api_key

# Application
WORKERS=4
LOG_LEVEL=info

```

**Load environment:**
```bash
docker-compose --env-file .env.production up -d
```

### Initial Admin Setup

Admin accounts are no longer provisioned automatically. When the database is
empty, the first account created through the `/register` page is granted the
`admin` role. After deploying a new environment, visit the UI and complete
registration to establish this initial admin before onboarding other users.

### Nginx Configuration

Create `nginx.conf` for reverse proxy:

```nginx
events {
    worker_connections 1024;
}

http {
    upstream api {
        server api:8000;
    }

    server {
        listen 80;
        server_name your-domain.com;

        # Redirect HTTP to HTTPS
        return 301 https://$server_name$request_uri;
    }

    server {
        listen 443 ssl http2;
        server_name your-domain.com;

        # SSL Configuration
        ssl_certificate /etc/nginx/ssl/cert.pem;
        ssl_certificate_key /etc/nginx/ssl/key.pem;
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;

        # Security Headers
        add_header X-Frame-Options "SAMEORIGIN" always;
        add_header X-Content-Type-Options "nosniff" always;
        add_header X-XSS-Protection "1; mode=block" always;

        # File Upload Size
        client_max_body_size 100M;

        # Proxy Settings
        location / {
            proxy_pass http://api;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            
            # Timeouts for large file uploads
            proxy_connect_timeout 300s;
            proxy_send_timeout 300s;
            proxy_read_timeout 300s;
        }

        # Health Check Endpoint
        location /health {
            access_log off;
            proxy_pass http://api/health;
        }
    }
}
```

### SSL/TLS Certificates

**Using Let's Encrypt:**

```bash
# Install certbot
apt-get install certbot python3-certbot-nginx

# Obtain certificate
certbot --nginx -d your-domain.com

# Auto-renewal (add to crontab)
0 0 * * * certbot renew --quiet
```

**Using self-signed certificates (development):**

```bash
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout ssl/key.pem \
  -out ssl/cert.pem
```

---

## Monitoring and Logging

### Application Logs

**View logs:**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f api

# Last 100 lines
docker-compose logs --tail=100 api

# Since specific time
docker-compose logs --since 2024-01-01T00:00:00 api
```

### Log Aggregation

**Using Docker logging driver:**

```yaml
# In docker-compose.yml
services:
  api:
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

**Using external logging (e.g., ELK Stack):**

```yaml
services:
  api:
    logging:
      driver: "syslog"
      options:
        syslog-address: "tcp://logstash:5000"
```

### Health Checks

Add health check endpoint to your application:

```python
# In app/main.py
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }
```

**Monitor with Docker:**
```bash
docker inspect --format='{{.State.Health.Status}}' datamapper-api
```

### Monitoring Tools

**Prometheus + Grafana:**

```yaml
# Add to docker-compose.yml
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

---

## Security Considerations

### Database Security

1. **Strong Passwords**: Use complex, unique passwords
2. **Network Isolation**: Keep database in private network
3. **Connection Limits**: Configure max connections
4. **Regular Updates**: Keep PostgreSQL updated

```sql
-- Create read-only user for reporting
CREATE USER readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE datamapper TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
```

### API Security

1. **HTTPS Only**: Force SSL/TLS encryption
2. **Rate Limiting**: Implement request rate limits
3. **Input Validation**: Validate all user inputs
4. **CORS Configuration**: Restrict allowed origins

**Add rate limiting:**
```python
# Using slowapi
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

@app.post("/map-data")
@limiter.limit("10/minute")
async def map_data(...):
    ...
```

### Secrets Management

**Using Docker Secrets:**

```yaml
services:
  api:
    secrets:
      - db_password
      - b2_key

secrets:
  db_password:
    file: ./secrets/db_password.txt
  b2_key:
    file: ./secrets/b2_key.txt
```

**Using environment variable files:**
```bash
# Store sensitive data separately
echo "DB_PASSWORD=secret" > .env.secret
chmod 600 .env.secret
```

### Firewall Configuration

```bash
# Allow only necessary ports
ufw allow 22/tcp   # SSH
ufw allow 80/tcp   # HTTP
ufw allow 443/tcp  # HTTPS
ufw enable
```

---

## Performance Optimization

### Application Tuning

**Worker Configuration:**
```bash
# Calculate workers: (2 x CPU cores) + 1
uvicorn app.main:app --workers 4 --host 0.0.0.0 --port 8000
```

**Connection Pooling:**
```python
# In app/database.py
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True
)
```

### Database Optimization

**Create indexes:**
```sql
-- Index frequently queried columns
CREATE INDEX idx_table_name ON your_table(column_name);

-- Composite indexes for multi-column queries
CREATE INDEX idx_composite ON your_table(col1, col2);
```

**Vacuum and analyze:**
```sql
-- Regular maintenance
VACUUM ANALYZE;

-- Auto-vacuum configuration
ALTER SYSTEM SET autovacuum = on;
```

### Caching

**Add Redis for caching:**

```yaml
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
```

```python
# In your application
from redis import Redis
cache = Redis(host='redis', port=6379)
```

---

## Backup and Recovery

### Database Backups

**Automated backups:**

```bash
#!/bin/bash
# backup.sh
BACKUP_DIR="/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/datamapper_$TIMESTAMP.sql"

docker exec datamapper-db pg_dump -U postgres datamapper > $BACKUP_FILE
gzip $BACKUP_FILE

# Keep only last 7 days
find $BACKUP_DIR -name "*.sql.gz" -mtime +7 -delete
```

**Schedule with cron:**
```bash
# Run daily at 2 AM
0 2 * * * /path/to/backup.sh
```

### Restore from Backup

```bash
# Decompress backup
gunzip datamapper_20240101_020000.sql.gz

# Restore to database
docker exec -i datamapper-db psql -U postgres datamapper < datamapper_20240101_020000.sql
```

### Disaster Recovery

1. **Regular Backups**: Automated daily backups
2. **Off-site Storage**: Store backups in different location
3. **Test Restores**: Regularly test backup restoration
4. **Documentation**: Maintain recovery procedures

---

## Scaling Strategies

### Horizontal Scaling

**Load Balancer Configuration:**

```yaml
services:
  api-1:
    image: data-mapper:latest
    # ... configuration

  api-2:
    image: data-mapper:latest
    # ... configuration

  nginx:
    # Load balance between api-1 and api-2
```

### Database Scaling

**Read Replicas:**
```yaml
services:
  db-primary:
    image: postgres:15-alpine
    # Primary database

  db-replica:
    image: postgres:15-alpine
    # Read replica configuration
```

---

## Troubleshooting Production Issues

### Common Issues

**Container won't start:**
```bash
# Check logs
docker-compose logs api

# Check container status
docker-compose ps

# Inspect container
docker inspect datamapper-api
```

**Database connection issues:**
```bash
# Test database connectivity
docker exec datamapper-api pg_isready -h db -U postgres

# Check network
docker network inspect datamapper-network
```

**High memory usage:**
```bash
# Monitor resource usage
docker stats

# Adjust worker count
# Reduce pool_size in database configuration
```

---

## Related Documentation

- [Setup Guide](SETUP.md) - Initial setup and configuration
- [Frontend Setup Guide](FRONTEND_SETUP.md) - Frontend build and deployment
- [API Reference](API_REFERENCE.md) - API endpoints
- [Architecture](ARCHITECTURE.md) - System design
- [Testing Guide](TESTING.md) - Testing procedures
