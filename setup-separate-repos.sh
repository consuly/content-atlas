#!/bin/bash

# Content Atlas - Setup Separate Repositories
# This script helps set up the automated sync to separate deployment repositories

set -e

echo "🚀 Content Atlas - Repository Separation Setup"
echo "=============================================="

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed. Please install it first:"
    echo "   https://cli.github.com/"
    exit 1
fi

# Check if user is logged in to GitHub
if ! gh auth status &> /dev/null; then
    echo "❌ Not logged in to GitHub. Please run 'gh auth login' first."
    exit 1
fi

# Get current repository info
REPO_INFO=$(gh repo view --json owner,name)
REPO_OWNER=$(echo $REPO_INFO | jq -r '.owner.login')
REPO_NAME=$(echo $REPO_INFO | jq -r '.name')

echo "📋 Current repository: $REPO_OWNER/$REPO_NAME"
echo ""

# Create API repository
echo "🔧 Creating API repository..."
if gh repo view $REPO_OWNER/content-atlas-api &> /dev/null; then
    echo "⚠️  API repository already exists: $REPO_OWNER/content-atlas-api"
else
    gh repo create content-atlas-api --public --description "Content Atlas API - FastAPI backend (auto-synced from monorepo)"
    echo "✅ Created API repository: $REPO_OWNER/content-atlas-api"
fi

# Create Frontend repository
echo "🔧 Creating Frontend repository..."
if gh repo view $REPO_OWNER/content-atlas-frontend &> /dev/null; then
    echo "⚠️  Frontend repository already exists: $REPO_OWNER/content-atlas-frontend"
else
    gh repo create content-atlas-frontend --public --description "Content Atlas Frontend - React admin interface (auto-synced from monorepo)"
    echo "✅ Created Frontend repository: $REPO_OWNER/content-atlas-frontend"
fi

echo ""
echo "🔑 Setting up repository secrets..."
echo "You'll need to create the following secrets in your main repository ($REPO_OWNER/$REPO_NAME):"
echo ""

# Generate and display secrets setup instructions
echo "1. Go to: https://github.com/$REPO_OWNER/$REPO_NAME/settings/secrets/actions"
echo ""
echo "2. Add these secrets:"
echo ""
echo "   API_REPO_OWNER: $REPO_OWNER"
echo "   API_REPO_TOKEN: [Create a Personal Access Token with repo permissions]"
echo "   FRONTEND_REPO_OWNER: $REPO_OWNER"
echo "   FRONTEND_REPO_TOKEN: [Create a Personal Access Token with repo permissions]"
echo ""
echo "3. To create a Personal Access Token:"
echo "   - Go to: https://github.com/settings/tokens"
echo "   - Generate new token (classic)"
echo "   - Select 'repo' scope"
echo "   - Copy the token and use it for both API_REPO_TOKEN and FRONTEND_REPO_TOKEN"
echo ""

echo "📝 Next steps:"
echo "1. Set up the repository secrets as shown above"
echo "2. Push these workflow files to your main repository"
echo "3. The workflows will automatically sync on the next push to main"
echo "4. Set up deployments for the separate repositories"
echo ""

echo "✨ Setup complete! Your repositories are ready for automated sync."
