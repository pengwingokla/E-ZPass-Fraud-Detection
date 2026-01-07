# Multi-stage Dockerfile for all-in-one deployment
# Builds frontend and serves it from Flask backend

# Stage 1: Build React frontend
FROM node:18-alpine AS frontend-builder

WORKDIR /app

# Copy package files
COPY frontend/package*.json ./

# Install dependencies
RUN npm ci

# Copy frontend source
COPY frontend/ .

# Build React app (use empty API URL for relative paths)
ARG REACT_APP_API_URL=
ENV REACT_APP_API_URL=${REACT_APP_API_URL}
RUN npm run build

# Stage 2: Python backend with frontend
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy backend requirements
COPY backend/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Gunicorn
RUN pip install --no-cache-dir gunicorn

# Copy backend code
COPY backend/ .

# Copy built frontend from builder stage
COPY --from=frontend-builder /app/build ./static

# Cloud Run uses PORT environment variable
ENV PORT=8080

# Expose port
EXPOSE $PORT

# Run Gunicorn
CMD exec gunicorn --bind :$PORT --workers 2 --threads 4 --timeout 0 app:app
