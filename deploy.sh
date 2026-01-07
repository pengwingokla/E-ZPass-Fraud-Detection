#!/bin/bash

# Fixed deployment script that ensures Dockerfile is included

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}GCP Cloud Run Deployment ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if Dockerfile exists
if [ ! -f "Dockerfile" ]; then
    echo -e "${RED}✗ Dockerfile not found in current directory!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Dockerfile found${NC}"

# Get project ID
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}No project set. Please set one:${NC}"
    echo "gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi
echo -e "${GREEN}✓ Using project: ${PROJECT_ID}${NC}"
echo ""

# Enable APIs
echo -e "${YELLOW}Enabling required APIs...${NC}"
gcloud services enable cloudbuild.googleapis.com --quiet
gcloud services enable run.googleapis.com --quiet
gcloud services enable containerregistry.googleapis.com --quiet
echo -e "${GREEN}✓ APIs enabled${NC}"
echo ""

# Check for key file
KEY_FILE="backend/keys/backend-key.json"
if [ ! -f "$KEY_FILE" ]; then
    KEY_FILE="backend/keys/bigquery.json"
fi

# Build deployment command
echo -e "${YELLOW}Building and deploying to Cloud Run...${NC}"
echo -e "${YELLOW}(This may take 5-10 minutes)${NC}"
echo ""

# Use Cloud Build with explicit Dockerfile reference
if [ -f "cloudbuild.yaml" ]; then
    echo -e "${GREEN}✓ Using Cloud Build configuration${NC}"
    gcloud builds submit --config cloudbuild.yaml .
    DEPLOY_EXIT_CODE=$?
else
    # Fallback: use --source but verify Dockerfile is included
    echo -e "${YELLOW}Using --source method...${NC}"
    
    # Create a temporary archive to verify Dockerfile is included
    echo -e "${YELLOW}Verifying Dockerfile will be included...${NC}"
    
    DEPLOY_CMD="gcloud run deploy ezpass-app \
      --source . \
      --platform managed \
      --region us-central1 \
      --allow-unauthenticated \
      --set-env-vars \"BIGQUERY_DATASET=ezpass_data\" \
      --memory 1Gi \
      --timeout 300"
    
    # Add service account if it exists
    SERVICE_ACCOUNT="ezpass-backend@${PROJECT_ID}.iam.gserviceaccount.com"
    if gcloud iam service-accounts describe "$SERVICE_ACCOUNT" &>/dev/null; then
        DEPLOY_CMD="$DEPLOY_CMD --service-account $SERVICE_ACCOUNT"
    fi
    
    # Add BigQuery key if file exists
    if [ -f "$KEY_FILE" ]; then
        if command -v jq &> /dev/null; then
            KEY_JSON=$(cat "$KEY_FILE" | jq -c)
        else
            KEY_JSON=$(python3 -c "import json, sys; print(json.dumps(json.load(sys.stdin), separators=(',', ':')))" < "$KEY_FILE")
        fi
        DEPLOY_CMD="$DEPLOY_CMD --set-env-vars \"BIGQUERY_KEY_JSON=$KEY_JSON\""
    fi
    
    eval $DEPLOY_CMD
    DEPLOY_EXIT_CODE=$?
fi

if [ $DEPLOY_EXIT_CODE -ne 0 ]; then
    echo -e "${RED}✗ Deployment failed${NC}"
    echo -e "${YELLOW}Check build logs: https://console.cloud.google.com/cloud-build/builds${NC}"
    exit $DEPLOY_EXIT_CODE
fi

# Get the URL
APP_URL=$(gcloud run services describe ezpass-app \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)' \
  --project "$PROJECT_ID" 2>/dev/null)

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}✅ Deployment Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
if [ -n "$APP_URL" ]; then
    echo -e "${GREEN}Your app is live at:${NC}"
    echo -e "${GREEN}$APP_URL${NC}"
else
    echo -e "${YELLOW}Deployment completed. Get URL with:${NC}"
    echo "gcloud run services describe ezpass-app --region us-central1 --format 'value(status.url)'"
fi
echo ""

