# Repository Separation Guide

Content Atlas uses a monorepo for development but automatically syncs to separate deployment repositories for independent scaling and deployment.

## Repository Structure

### Development Repository (Monorepo)
- **Repository**: `content-atlas` (this repository)
- **Purpose**: Unified development environment
- **Contents**: Full codebase, tests, documentation, development tools

### Deployment Repositories

#### API Repository
- **Repository**: `content-atlas-api`
- **Purpose**: Backend API deployment
- **Contents**: FastAPI application, database code, API documentation
- **Deployment**: Railway, Render, or any container platform

#### Frontend Repository
- **Repository**: `content-atlas-frontend`
- **Purpose**: Frontend application deployment
- **Contents**: React application, static assets
- **Deployment**: Vercel, Netlify, or any static hosting platform

## Automated Sync Process

Changes pushed to the `main` branch of the monorepo automatically trigger GitHub Actions that sync relevant files to the deployment repositories.

### Backend Sync
- **Trigger**: Changes to `app/`, `docs/`, `tests/`, backend config files
- **Workflow**: `.github/workflows/sync-backend.yml`
- **Target**: `content-atlas-api` repository

### Frontend Sync
- **Trigger**: Changes to `frontend/` directory
- **Workflow**: `.github/workflows/sync-frontend.yml`
- **Target**: `content-atlas-frontend` repository

## Setup Instructions

### 1. Create Deployment Repositories

Run the setup script to create the deployment repositories:

```bash
# Requires GitHub CLI (gh) to be installed and authenticated
./setup-separate-repos.sh
```

This will:
- Create `content-atlas-api` repository
- Create `content-atlas-frontend` repository
- Provide instructions for setting up repository secrets

### 2. Configure Repository Secrets

In your main repository (`content-atlas`), go to Settings → Secrets and variables → Actions and add:

```
API_REPO_OWNER: your-github-username
API_REPO_TOKEN: your-personal-access-token
FRONTEND_REPO_OWNER: your-github-username
FRONTEND_REPO_TOKEN: your-personal-access-token
```

### 3. Create Personal Access Token

1. Go to [GitHub Settings → Developer settings → Personal access tokens](https://github.com/settings/tokens)
2. Generate new token (classic)
3. Select `repo` scope
4. Use the token for both `API_REPO_TOKEN` and `FRONTEND_REPO_TOKEN`

### 4. Initial Sync

After setting up secrets, push the workflow files to trigger the initial sync:

```bash
git add .
git commit -m "Add repository separation workflows"
git push origin main
```

## Development Workflow

### For Backend Changes
1. Make changes to `app/`, `docs/`, or backend files
2. Push to `main` branch
3. GitHub Actions automatically sync to `content-atlas-api`
4. Deployments in `content-atlas-api` will update automatically

### For Frontend Changes
1. Make changes to `frontend/` directory
2. Push to `main` branch
3. GitHub Actions automatically sync to `content-atlas-frontend`
4. Deployments in `content-atlas-frontend` will update automatically

### For Shared Changes
1. Update documentation, docker-compose, or shared configs
2. Backend sync will include documentation updates
3. Frontend sync ignores shared files (by design)

## Deployment Configuration

### Backend Deployment (Railway)
The `content-atlas-api` repository contains:
- `Dockerfile` for containerized deployment
- `railway.json` for Railway configuration
- All necessary backend files

### Frontend Deployment (Vercel/Netlify)
The `content-atlas-frontend` repository contains:
- `package.json` with build scripts
- `vite.config.ts` for build configuration
- All React application files

Set the `VITE_API_URL` environment variable to point to your deployed API.

## Benefits

1. **Independent Scaling**: Frontend and backend scale separately
2. **Technology Isolation**: Frontend changes don't trigger backend deployments
3. **Unified Development**: All development happens in one repository
4. **Flexible Deployment**: Choose different platforms for each service
5. **Version Control**: Single source of truth for all code

## Troubleshooting

### Workflows Not Triggering
- Check that secrets are properly configured
- Verify the Personal Access Token has `repo` scope
- Check GitHub Actions logs for errors

### Sync Issues
- Manual changes to deployment repositories will be overwritten
- All changes should be made in the monorepo
- Check the `paths` configuration in workflow files

### Deployment Issues
- Ensure deployment repositories have proper CI/CD setup
- Check that environment variables are configured in deployment platforms
- Verify API URLs are correctly set in frontend deployments

## Migration from Monorepo

If you were previously deploying from the monorepo:

1. Set up the automated sync as described above
2. Update your deployment configurations to use the new repositories
3. Test deployments from the separate repositories
4. Archive old deployment configurations in the monorepo

## Future Considerations

- Consider using GitHub's repository rules to protect deployment repositories from direct changes
- Set up automated testing in deployment repositories
- Configure branch protection rules for the monorepo
- Consider using GitHub Environments for better deployment management
