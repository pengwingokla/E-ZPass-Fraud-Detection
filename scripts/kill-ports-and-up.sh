#!/usr/bin/env bash
# Stop compose, kill processes on Airflow/MLflow/App ports, then compose up with repo .env.
# Run from repo root: ./scripts/kill-ports-and-up.sh

set -e
cd "$(dirname "$0")/.."

COMPOSE_CMD="docker compose --env-file .env -f deploy/docker-compose.yaml"
PORTS="8080 8081 5001 5003 5005 5007"

echo "Stopping compose stack..."
$COMPOSE_CMD down 2>/dev/null || true

echo "Killing processes on ports: $PORTS"
for port in $PORTS; do
  pids=$(lsof -ti :$port 2>/dev/null || true)
  if [ -n "$pids" ]; then
    echo "  Port $port: killing $pids"
    echo "$pids" | xargs kill -9 2>/dev/null || true
  fi
done

echo "Starting stack..."
$COMPOSE_CMD up -d

echo "Done. Airflow: http://localhost:8080  MLflow: http://localhost:5005  App: http://localhost:5001"
