# Server Deployment Guide

This guide provides a step-by-step tutorial for deploying the Content Atlas full stack (Frontend, Backend, and Database) on a new server using Docker Compose.

## Prerequisites

- A clean server (Ubuntu 20.04/22.04 LTS recommended)
- Root or sudo access
- A domain name pointing to your server's IP address

## 1. Install Docker & Docker Compose

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

## 2. Clone the Repository

Clone the project to your server (e.g., in `/opt/content-atlas` or your home directory).

```bash
git clone https://github.com/thefoundry-app/content-atlas.git
cd content-atlas
```

## 3. Configure Environment Variables

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

## 4. Create Production Docker Compose File

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

## 5. Configure Nginx

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

> **Note on SSL**: For production, you should obtain an SSL certificate (e.g., via Certbot) and configure Nginx to listen on 443. See [DEPLOYMENT.md](DEPLOYMENT.md#ssl-tls-certificates) for details. For initial setup, the config above listens on port 80 (HTTP).

## 6. Create SSL Directory (Placeholder)

Create the directory for SSL certificates to avoid Docker errors, even if empty initially.

```bash
mkdir -p ssl
```

## 7. Deploy

Start the services using the production compose file.

```bash
sudo docker compose -f docker-compose.prod.yml up -d --build
```

Monitor the logs to ensure everything starts correctly:

```bash
sudo docker compose -f docker-compose.prod.yml logs -f
```

## 8. Final Setup

### Create Admin User
1. Open your browser and navigate to `http://your-server-ip`.
2. You should see the login page.
3. Click "Register" to create a new account.
4. **Important**: The first account created on a fresh database is automatically assigned **Admin** privileges.

### Verify System
1. Log in with your new admin account.
2. Go to the "Imports" section.
3. Upload a small test CSV to verify the database and file storage connection.

## Troubleshooting

- **502 Bad Gateway**: Usually means the API or Frontend container is not ready yet. Check logs with `docker compose logs -f`.
- **CORS Errors**: Ensure `ALLOWED_ORIGINS` in `.env.production` includes your domain (e.g., `https://your-domain.com`).
- **Database Connection Error**: Check `DB_USER` and `DB_PASSWORD` in `.env.production` match what is in `docker-compose.prod.yml`.

---
For advanced configuration, scaling, and backups, refer to [DEPLOYMENT.md](DEPLOYMENT.md).
