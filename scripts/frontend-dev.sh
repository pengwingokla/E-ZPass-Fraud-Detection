#!/usr/bin/env bash
# Run the frontend with npm (hot reload). Backend must be available at REACT_APP_API_URL (e.g. Docker app on :5001).
# From repo root: ./scripts/frontend-dev.sh

set -e
cd "$(dirname "$0")/../frontend"
echo "Starting frontend dev server (API from .env, usually http://localhost:5001)..."
npm start
