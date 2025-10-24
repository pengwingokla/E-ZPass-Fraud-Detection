"""
GCS File Sensor implementation for detecting new files in the raw bucket.
This module provides functionality to detect new CSV files uploaded to GCS.
"""

import os
import logging
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_gcs_config() -> dict:
    """
    Load GCS configuration from environment variables.
    
    Returns:
        dict: Configuration dictionary with GCS settings
        
    Raises:
        ValueError: If required environment variables are missing
    """
    config = {
        'bucket_name': os.getenv("GCS_BUCKET_NAME"),
        'project_id': os.getenv("GCS_PROJECT_ID"),
        'credentials_path': os.getenv("GCS_CREDENTIALS_PATH"),
        'folder_prefix': os.getenv("GCS_FOLDER_PREFIX_RAW", "data/raw/"),
        'folder_prefix_raw': os.getenv("GCS_FOLDER_PREFIX_RAW", "data/raw/"),
        'default_content_type': os.getenv("GCS_DEFAULT_CONTENT_TYPE", "text/csv"),
        'check_interval': int(os.getenv("GCS_SENSOR_CHECK_INTERVAL", "60")),
        'timeout': int(os.getenv("GCS_SENSOR_TIMEOUT", "3600"))
    }
    
    # Validate required variables
    if not config['bucket_name']:
        raise ValueError("GCS_BUCKET_NAME environment variable is required")
    
    if not config['project_id']:
        raise ValueError("GCS_PROJECT_ID environment variable is required")
    
    return config


if __name__ == "__main__":
    """
    Standalone usage example for testing GCS configuration.
    """
    try:
        # Load configuration
        config = get_gcs_config()
        
        print("GCS Sensor Configuration Test")
        print("=" * 50)
        print(f"Bucket Name: {config['bucket_name']}")
        print(f"Project ID: {config['project_id']}")
        print(f"Folder Prefix: {config['folder_prefix']}")
        print(f"Check Interval: {config['check_interval']}s")
        print(f"Timeout: {config['timeout']}s")
        
        # Test GCS connection
        if config['credentials_path'] and os.path.exists(config['credentials_path']):
            credentials = service_account.Credentials.from_service_account_file(
                config['credentials_path']
            )
            client = storage.Client(credentials=credentials, project=config['project_id'])
            print(f"✓ GCS client initialized with service account: {config['credentials_path']}")
        else:
            client = storage.Client(project=config['project_id'])
            print("✓ GCS client initialized with default credentials")
        
        # Test bucket access
        bucket = client.bucket(config['bucket_name'])
        if bucket.exists():
            print(f"✓ Bucket '{config['bucket_name']}' exists and is accessible")
        else:
            print(f"✗ Bucket '{config['bucket_name']}' not found or not accessible")
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print(f"Please check your .env file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An error occurred: {e}")
