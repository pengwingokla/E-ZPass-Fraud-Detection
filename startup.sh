#!/bin/bash

# E-ZPass Fraud Detection System - Startup Script
# This script starts all services: Airflow, Backend API, and Frontend

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}E-ZPass Fraud Detection - Startup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a port is in use
port_in_use() {
    lsof -Pi :$1 -sTCP:LISTEN -t >/dev/null 2>&1
}

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check Docker
if ! command_exists docker; then
    echo -e "${RED}✗ Docker is not installed or not in PATH${NC}"
    echo "Please install Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker daemon is not running${NC}"
    echo "Please start Docker Desktop and try again"
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# Check Docker Compose
if ! command_exists docker-compose && ! docker compose version >/dev/null 2>&1; then
    echo -e "${RED}✗ Docker Compose is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose is available${NC}"

# Check Python
if ! command_exists python3; then
    echo -e "${RED}✗ Python 3 is not installed${NC}"
    exit 1
fi
PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo -e "${GREEN}✓ Python $PYTHON_VERSION is installed${NC}"

# Check Node.js
if ! command_exists node; then
    echo -e "${RED}✗ Node.js is not installed${NC}"
    exit 1
fi
NODE_VERSION=$(node --version)
echo -e "${GREEN}✓ Node.js $NODE_VERSION is installed${NC}"

# Check npm
if ! command_exists npm; then
    echo -e "${RED}✗ npm is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ npm is installed${NC}"

echo ""

# Check environment files
echo -e "${YELLOW}Checking environment files...${NC}"

# Check root .env
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠ Root .env file not found. Creating from template...${NC}"
    if [ -f "env.template" ]; then
        cp env.template .env
        echo -e "${YELLOW}⚠ Please configure .env with your GCP settings${NC}"
    else
        echo -e "${RED}✗ env.template not found${NC}"
    fi
else
    echo -e "${GREEN}✓ Root .env exists${NC}"
fi

# Check backend BigQuery key file
if [ ! -f "backend/keys/bigquery.json" ]; then
    echo -e "${YELLOW}⚠ Backend BigQuery key file not found at backend/keys/bigquery.json${NC}"
    echo -e "${YELLOW}The backend requires this file to connect to BigQuery${NC}"
else
    echo -e "${GREEN}✓ Backend BigQuery key file exists${NC}"
fi

# Check backend .env (for BIGQUERY_TABLE and other settings)
if [ ! -f "backend/.env" ]; then
    echo -e "${YELLOW}⚠ Backend .env file not found. Creating from template...${NC}"
    if [ -f "backend/env.template" ]; then
        cp backend/env.template backend/.env
        sed -i.bak '/^BIGQUERY_KEY_JSON=/d' backend/.env 2>/dev/null || sed -i '' '/^BIGQUERY_KEY_JSON=/d' backend/.env
        echo -e "${YELLOW}⚠ Backend .env created (BigQuery key will be loaded from backend/keys/bigquery.json)${NC}"
    else
        echo -e "${RED}✗ backend/env.template not found${NC}"
    fi
else
    echo -e "${GREEN}✓ Backend .env exists${NC}"
fi

# Check frontend .env
if [ ! -f "frontend/.env" ]; then
    echo -e "${YELLOW}⚠ Frontend .env file not found. Creating from template...${NC}"
    if [ -f "frontend/env.template" ]; then
        cp frontend/env.template frontend/.env
    else
        echo -e "${RED}✗ frontend/env.template not found${NC}"
    fi
else
    echo -e "${GREEN}✓ Frontend .env exists${NC}"
fi

echo ""

# Check if ports are available
echo -e "${YELLOW}Checking if ports are available...${NC}"

if port_in_use 8080; then
    echo -e "${YELLOW}⚠ Port 8080 is already in use (Airflow)${NC}"
fi

if port_in_use 5001; then
    echo -e "${YELLOW}⚠ Port 5001 is already in use (Backend API)${NC}"
fi

if port_in_use 3000; then
    echo -e "${YELLOW}⚠ Port 3000 is already in use (Frontend)${NC}"
fi

echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down services...${NC}"
    # Stop Docker containers
    docker-compose down 2>/dev/null || true
    # Kill background processes
    kill $BACKEND_PID $FRONTEND_PID 2>/dev/null || true
    echo -e "${GREEN}Cleanup complete${NC}"
}

# Trap Ctrl+C and cleanup
trap cleanup INT TERM

# Start Airflow
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Starting Airflow (Data Pipeline)${NC}"
echo -e "${BLUE}========================================${NC}"

# Build Docker images if needed
if [ "$1" == "--build" ] || [ ! -f ".docker-built" ]; then
    echo -e "${YELLOW}Building Docker images (this may take a few minutes)...${NC}"
    docker-compose build
    touch .docker-built
fi

# Initialize Airflow if not already done
if [ ! -f ".airflow-init" ]; then
    echo -e "${YELLOW}Initializing Airflow database...${NC}"
    docker-compose up airflow-init
    touch .airflow-init
fi

# Start Airflow services
echo -e "${YELLOW}Starting Airflow services...${NC}"
docker-compose up -d

# Wait for Airflow to be ready
echo -e "${YELLOW}Waiting for Airflow to be ready...${NC}"
sleep 10
MAX_WAIT=60
WAIT_COUNT=0
while ! curl -s http://localhost:8080/health >/dev/null 2>&1; do
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo -e "${RED}✗ Airflow failed to start within $MAX_WAIT seconds${NC}"
        exit 1
    fi
    echo -n "."
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
done
echo ""
echo -e "${GREEN}✓ Airflow is running at http://localhost:8080${NC}"
echo -e "${GREEN}  Username: airflow | Password: airflow${NC}"

echo ""

# Start Backend
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Starting Backend API${NC}"
echo -e "${BLUE}========================================${NC}"

# Check for BigQuery key file and set environment variable
if [ ! -f "backend/keys/bigquery.json" ]; then
    echo -e "${RED}✗ BigQuery key file not found at backend/keys/bigquery.json${NC}"
    echo -e "${YELLOW}Please ensure the BigQuery service account key file exists${NC}"
    exit 1
fi

# Read the JSON file and set as environment variable
# Convert the JSON file to a compact single-line string
BIGQUERY_KEY_JSON=$(python3 -c "import json; print(json.dumps(json.load(open('backend/keys/bigquery.json')), separators=(',', ':')))")
export BIGQUERY_KEY_JSON
echo -e "${GREEN}✓ Loaded BigQuery credentials from backend/keys/bigquery.json${NC}"

cd backend

# Check if virtual environment exists
if [ -d "../venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source ../venv/bin/activate
elif [ -d "venv" ]; then
    echo -e "${YELLOW}Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${YELLOW}No virtual environment found. Using system Python...${NC}"
fi

# Install dependencies if needed
if [ ! -d "../venv" ] && [ ! -d "venv" ]; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip3 install -q -r requirements.txt
fi

# Start backend in background with the environment variable
echo -e "${YELLOW}Starting Flask backend server...${NC}"
BIGQUERY_KEY_JSON="$BIGQUERY_KEY_JSON" python3 app.py > ../logs/backend.log 2>&1 &
BACKEND_PID=$!
cd ..

# Wait for backend to be ready
echo -e "${YELLOW}Waiting for backend to be ready...${NC}"
sleep 3
MAX_WAIT=30
WAIT_COUNT=0
while ! curl -s http://localhost:5001/api/table-info >/dev/null 2>&1; do
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo -e "${RED}✗ Backend failed to start within $MAX_WAIT seconds${NC}"
        echo -e "${YELLOW}Check logs/backend.log for errors${NC}"
        exit 1
    fi
    echo -n "."
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done
echo ""
echo -e "${GREEN}✓ Backend API is running at http://localhost:5001${NC}"

echo ""

# Start Frontend
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Starting Frontend${NC}"
echo -e "${BLUE}========================================${NC}"

cd frontend

# Install dependencies if needed
if [ ! -d "node_modules" ]; then
    echo -e "${YELLOW}Installing Node.js dependencies (this may take a few minutes)...${NC}"
    npm install
fi

# Start frontend in background
echo -e "${YELLOW}Starting React development server...${NC}"
npm start > ../logs/frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..

# Wait for frontend to be ready
echo -e "${YELLOW}Waiting for frontend to be ready...${NC}"
sleep 5
MAX_WAIT=60
WAIT_COUNT=0
while ! curl -s http://localhost:3000 >/dev/null 2>&1; do
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo -e "${YELLOW}⚠ Frontend may still be starting. Check logs/frontend.log${NC}"
        break
    fi
    echo -n "."
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
done
echo ""
echo -e "${GREEN}✓ Frontend is running at http://localhost:3000${NC}"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All services are running!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Services:${NC}"
echo -e "  • Airflow UI:    ${BLUE}http://localhost:8080${NC} (airflow/airflow)"
echo -e "  • Backend API:   ${BLUE}http://localhost:5001${NC}"
echo -e "  • Frontend:      ${BLUE}http://localhost:3000${NC}"
echo ""
echo -e "${YELLOW}Logs:${NC}"
echo -e "  • Backend:  logs/backend.log"
echo -e "  • Frontend: logs/frontend.log"
echo -e "  • Airflow:  docker-compose logs -f"
echo ""
echo -e "${YELLOW}To stop all services, press Ctrl+C or run ./stop.sh${NC}"
echo ""

# Create logs directory if it doesn't exist
mkdir -p logs

# Keep script running and wait for background processes
# This allows Ctrl+C to trigger the cleanup trap
echo -e "${YELLOW}Services are running in the background.${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop all services...${NC}"
echo ""

# Wait for background processes
wait $BACKEND_PID $FRONTEND_PID 2>/dev/null || true

