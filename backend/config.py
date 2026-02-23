"""
Backend configuration from environment.
Loads .env (and root .env when running from backend/) and exposes
BigQuery client and table identifiers.
"""
import os
import tempfile

from dotenv import load_dotenv
from google.cloud import bigquery

# Load env: backend/.env then repo root .env
load_dotenv()
_root_env = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(_root_env)


def _get_project_id() -> str:
    project_id = (
        os.getenv("GCS_PROJECT_ID") or os.getenv("BIGQUERY_PROJECT_ID") or ""
    ).strip()
    if not project_id:
        raise ValueError(
            "GCS_PROJECT_ID or BIGQUERY_PROJECT_ID must be set to your GCP project ID."
        )
    return project_id


# BigQuery credentials:
# 1) Prefer BIGQUERY_KEY_JSON (used in Cloud Run and startup.sh)
# 2) Fallback to GOOGLE_APPLICATION_CREDENTIALS file
# 3) Finally, rely on Application Default Credentials (ADC)
_key_json = os.getenv("BIGQUERY_KEY_JSON")
_client: bigquery.Client

if _key_json and _key_json.strip():
    _key_path = os.path.join(tempfile.gettempdir(), "bigquery-key.json")
    with open(_key_path, "w") as f:
        f.write(_key_json.strip())
    _client = bigquery.Client.from_service_account_json(_key_path)
else:
    _creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if _creds_path and os.path.exists(_creds_path):
        _client = bigquery.Client.from_service_account_json(_creds_path)
    else:
        # Fall back to ADC (e.g., when running with gcloud auth application-default login)
        _client = bigquery.Client()

# BigQuery client
client = _client

# Project and dataset
project_id = _get_project_id()
dataset = os.getenv("BIGQUERY_DATASET", "ezpass_data").strip() or "ezpass_data"
table_name = os.getenv("BIGQUERY_TABLE", "master_viz").strip() or "master_viz"


def get_table() -> str:
    """Fully qualified BigQuery table id, e.g. `project.dataset.table`."""
    return f"`{project_id}.{dataset}.{table_name}`"
