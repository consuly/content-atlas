# This Dockerfile uses `serve` npm package to serve the static files with node process.
# You can find the Dockerfile for nginx in the following link:
# https://github.com/refinedev/dockerfiles/blob/main/vite/Dockerfile.nginx
FROM refinedev/node:20 AS base

FROM base as deps

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production=false

FROM base as builder

ENV NODE_ENV production

# Copy node_modules from deps stage
COPY --from=deps /app/node_modules ./node_modules

# Copy source code
COPY . .

# Build the application
RUN npm run build

FROM base as runner

ENV NODE_ENV production

# Install serve globally
RUN npm install -g serve

# Copy built files from builder stage
COPY --from=builder /app/dist ./dist

USER refine

# Use PORT environment variable if set, otherwise default to 3000
CMD serve -s dist -l ${PORT:-3000}
