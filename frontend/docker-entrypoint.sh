#!/bin/sh
set -e

# Generate runtime configuration based on container env vars
node /app/scripts/write-runtime-env.cjs

exec "$@"
