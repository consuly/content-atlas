# Frontend Setup Guide

Complete guide for setting up and deploying the Content Atlas frontend dashboard.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Local Development](#local-development)
- [Authentication Setup](#authentication-setup)
- [Deployment Options](#deployment-options)
- [Environment Configuration](#environment-configuration)
- [Troubleshooting](#troubleshooting)

---

## Overview

The Content Atlas frontend is built with:
- **Refine** - React framework for admin dashboards
- **Vite** - Fast build tool and dev server
- **Ant Design** - UI component library
- **TypeScript** - Type-safe JavaScript
- **React Router** - Client-side routing

### Key Features

- 🔐 JWT-based authentication
- 📊 Table browser and data viewer
- 📤 File upload and data import
- 💬 Natural language database queries
- 📜 Import history tracking
- 🎨 Dark/light mode support

---

## Prerequisites

### Required Software

- **Node.js** 18+ (LTS recommended)
- **npm** 9+ or **yarn** 1.22+
- **Git** (for cloning)

### Backend Requirements

The frontend requires the FastAPI backend to be running. See [SETUP.md](SETUP.md) for backend setup.

---

## Local Development

### 1. Navigate to Frontend Directory

```bash
cd frontend
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Configure Environment

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit `.env` and set your API URL:

```env
VITE_API_URL=http://localhost:8000
```

### 4. Start Development Server

```bash
npm run dev
```

The frontend will be available at `http://localhost:5173`

### 5. Build for Production

```bash
npm run build
```

Built files will be in the `dist/` directory.

---

## Authentication Setup

### Creating the First User

Before you can login, you need to create an admin user:

```bash
# From the project root
python create_admin_user.py
```

Follow the prompts to create your account:
- Enter email address
- Enter full name (optional)
- Enter password (minimum 8 characters)
- Confirm password

### Login

1. Navigate to `http://localhost:5173/login`
2. Enter your email and password
3. Click "Sign in"

You'll be redirected to the dashboard upon successful login.

### JWT Token Management

- Tokens are stored in `localStorage` as `refine-auth`
- Tokens expire after 24 hours
- Expired tokens automatically redirect to login
- Logout clears the token from storage

---

## Deployment Options

### Option 1: Static Hosting (Recommended for Simple Deployments)

Build the frontend and deploy to any static hosting service:

**Vercel:**
```bash
npm install -g vercel
vercel --prod
```

**Netlify:**
```bash
npm install -g netlify-cli
netlify deploy --prod --dir=dist
```

**GitHub Pages:**
```bash
npm run build
# Push dist/ folder to gh-pages branch
```

### Option 2: Docker with Nginx

Create `frontend/Dockerfile.prod`:

```dockerfile
FROM node:18-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

Create `frontend/nginx.conf`:

```nginx
server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    # API proxy (optional - if serving from same domain)
    location /api {
        proxy_pass http://backend:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Build and run:

```bash
docker build -f Dockerfile.prod -t content-atlas-frontend .
docker run -p 80:80 content-atlas-frontend
```

### Option 3: Serve from FastAPI

Build the frontend and serve it from FastAPI:

```bash
# Build frontend
cd frontend
npm run build

# Copy to FastAPI static directory
mkdir -p ../../app/static
cp -r dist/* ../../app/static/
```

Update `app/main.py`:

```python
from fastapi.staticfiles import StaticFiles

# Add after app initialization
app.mount("/", StaticFiles(directory="app/static", html=True), name="static")
```

Now the frontend is served from `http://localhost:8000/`

### Option 4: Complete Docker Compose

Update `docker-compose.yml` to include frontend:

```yaml
version: '3.8'

services:
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: datamapper
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  backend:
    build: .
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://postgres:postgres@db:5432/datamapper
      SECRET_KEY: your-secret-key-change-in-production
    depends_on:
      - db
    volumes:
      - .:/app

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile.prod
    ports:
      - "80:80"
    environment:
      VITE_API_URL: http://localhost:8000
    depends_on:
      - backend

volumes:
  postgres_data:
```

Start everything:

```bash
docker-compose up -d
```

---

## Environment Configuration

### Development (.env)

```env
VITE_API_URL=http://localhost:8000
```

### Production (.env.production)

```env
VITE_API_URL=https://api.yourdomain.com
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VITE_API_URL` | FastAPI backend URL | `http://localhost:8000` |

---

## Troubleshooting

### Issue: "Network Error" on Login

**Cause:** Frontend can't reach the backend API

**Solution:**
1. Verify backend is running: `curl http://localhost:8000`
2. Check `VITE_API_URL` in `.env`
3. Check browser console for CORS errors
4. Ensure FastAPI has CORS enabled (it should by default)

### Issue: "401 Unauthorized" After Login

**Cause:** Token not being sent or invalid

**Solution:**
1. Check browser localStorage for `refine-auth` token
2. Verify token in backend: `python -c "from jose import jwt; print(jwt.decode('YOUR_TOKEN', 'SECRET_KEY', algorithms=['HS256']))"`
3. Ensure `SECRET_KEY` matches between frontend requests and backend

### Issue: Build Fails with TypeScript Errors

**Cause:** Type mismatches or missing dependencies

**Solution:**
```bash
# Clear node_modules and reinstall
rm -rf node_modules package-lock.json
npm install

# Clear Vite cache
rm -rf node_modules/.vite
```

### Issue: Page Refreshes to 404

**Cause:** Server not configured for SPA routing

**Solution:**
- For Nginx: Use `try_files $uri $uri/ /index.html;`
- For Apache: Use `.htaccess` with rewrite rules
- For static hosts: Most handle this automatically

### Issue: API Calls Fail in Production

**Cause:** Incorrect `VITE_API_URL` or CORS issues

**Solution:**
1. Verify production API URL is correct
2. Check backend CORS settings allow your frontend domain
3. Use browser dev tools Network tab to inspect requests

---

## Next Steps

After setting up the frontend:

1. **Create User Account** - Run `python create_admin_user.py`
2. **Test Login** - Verify authentication works
3. **Import Data** - Use the import page to upload your first file
4. **Browse Tables** - View imported data in the tables page
5. **Query Database** - Try natural language queries

For detailed API documentation, see [API_REFERENCE.md](API_REFERENCE.md).

For backend setup, see [SETUP.md](SETUP.md).

---

## Development Tips

### Hot Module Replacement

Vite provides instant HMR. Changes to React components update without full page reload.

### TypeScript

The project uses strict TypeScript. Fix type errors before building:

```bash
npm run build
```

### Code Style

The project uses ESLint. Run linter:

```bash
npm run lint
```

### Component Development

Refine provides many built-in components. See:
- [Refine Documentation](https://refine.dev/docs/)
- [Ant Design Components](https://ant.design/components/overview/)

---

## Support

For issues or questions:
- Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
- Review [API_REFERENCE.md](API_REFERENCE.md)
- Check backend logs for API errors
- Use browser dev tools for frontend debugging
