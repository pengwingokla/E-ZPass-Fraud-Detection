#!/bin/bash

# E-ZPass Fraud Detection System - Stop Script
# This script stops all services: Airflow, Backend API, and Frontend

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
echo -e "${BLUE}E-ZPass Fraud Detection - Stopping Services${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to kill process on a port
kill_port() {
    local port=$1
    local service_name=$2
    
    # Find PIDs using the port
    local pids=$(lsof -ti :$port 2>/dev/null || true)
    
    if [ -z "$pids" ]; then
        echo -e "${YELLOW}⚠ $service_name (port $port) is not running${NC}"
        return 0
    fi
    
    echo -e "${YELLOW}Stopping $service_name (port $port)...${NC}"
    for pid in $pids; do
        # Get process info
        local process_name=$(ps -p $pid -o comm= 2>/dev/null || echo "")
        local process_path=$(ps -p $pid -o command= 2>/dev/null | awk '{print $1}' || echo "")
        
        # Skip Docker container processes (should be handled by docker-compose)
        if echo "$process_path" | grep -q "com.docker" || echo "$process_name" | grep -q "docker"; then
            echo -e "  Skipping Docker process $pid, should be stopped by docker-compose"
            continue
        fi
        
        # Skip macOS system processes
        if echo "$process_path" | grep -q "/System/Library" || echo "$process_path" | grep -q "/usr/libexec"; then
            echo -e "${YELLOW}  ⚠ Skipping system process $pid ($process_name) - port $port may be in use by macOS${NC}"
            echo -e "${YELLOW}     Consider changing the service port in docker-compose.yaml${NC}"
            continue
        fi
        
        # Check if it's a system process (be more careful)
        if [ -n "$process_name" ]; then
            echo -e "  Killing process $pid ($process_name)"
        else
            echo -e "  Killing process $pid"
        fi
        kill $pid 2>/dev/null || true
    done
    
    # Wait a bit and force kill if still running (but skip Docker processes)
    sleep 2
    local remaining_pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$remaining_pids" ]; then
        for pid in $remaining_pids; do
            local process_name=$(ps -p $pid -o comm= 2>/dev/null || echo "")
            if ! echo "$process_name" | grep -q "com.docker"; then
                echo -e "${YELLOW}  Force killing remaining process $pid...${NC}"
                kill -9 $pid 2>/dev/null || true
            fi
        done
    fi
    
    echo -e "${GREEN}✓ $service_name stopped${NC}"
}

# Stop Docker containers first (Airflow, MLflow) - this frees up ports 8080 and 5005
echo -e "${BLUE}Stopping Docker containers (Airflow, MLflow)...${NC}"
if command -v docker-compose >/dev/null 2>&1; then
    docker-compose down 2>/dev/null || true
elif docker compose version >/dev/null 2>&1; then
    docker compose down 2>/dev/null || true
else
    echo -e "${YELLOW}⚠ Docker Compose not found. Trying to stop containers manually...${NC}"
    docker stop $(docker ps -q --filter "ancestor=ezpass-airflow:latest") 2>/dev/null || true
fi

# Also stop any containers that might be using the ports
echo -e "${YELLOW}Checking for containers using ports 5005, 8080...${NC}"
for port in 5005 8080; do
    container_id=$(docker ps --format "{{.ID}}\t{{.Ports}}" | grep ":$port->" | awk '{print $1}' | head -1)
    if [ -n "$container_id" ]; then
        echo -e "  Stopping container $container_id using port $port"
        docker stop $container_id 2>/dev/null || true
    fi
done

# Remove any stopped containers from this project
docker-compose rm -f 2>/dev/null || docker compose rm -f 2>/dev/null || true

echo -e "${GREEN}✓ Docker containers stopped${NC}"

echo ""

# Stop Frontend (port 3000)
echo -e "${BLUE}Stopping Frontend...${NC}"
kill_port 3000 "Frontend"

echo ""

# Stop Backend (port 5001)
echo -e "${BLUE}Stopping Backend API...${NC}"
kill_port 5001 "Backend API"

echo ""

# Stop MLflow UI (port 5005) - in case it's still running
echo -e "${BLUE}Stopping MLflow UI...${NC}"
kill_port 5005 "MLflow UI"

echo ""

# Stop Airflow (port 8080) - in case it's still running
echo -e "${BLUE}Stopping Airflow...${NC}"
kill_port 8080 "Airflow"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All services stopped!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

