"""
Local file detection and upload functionality for E-ZPass Fraud Detection.
This module detects new CSV files in the local data/raw folder and uploads them to GCS.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Union, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

try:
    from .gcs_utils import get_gcs_config
except ImportError:
    # Fallback for when running as standalone script
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from gcs_utils import get_gcs_config

# Import Google Cloud Storage
from google.cloud import storage
from google.oauth2 import service_account
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def upload_files(file_paths: Union[str, Path, List[Union[str, Path]]],
                bucket_name: str,
                destination_names: Optional[Union[str, List[str]]] = None,
                folder_prefix: str = "",
                content_type: str = "text/csv",
                credentials_path: Optional[str] = None,
                project_id: Optional[str] = None) -> Union[bool, Dict[str, bool]]:
    """
    Upload CSV files to Google Cloud Storage.
    
    This function can handle multiple use cases:
    - Single file: pass file_paths as string/Path, destination_names as string (optional)
    - Multiple files: pass file_paths as list, destination_names as list (optional)
    
    Args:
        file_paths (str, Path, or List): Single file path or list of file paths
        bucket_name (str): Name of the GCS bucket
        destination_names (str or List[str], optional): Destination names in GCS
        folder_prefix (str): Optional folder prefix in GCS
        content_type (str): MIME type for the uploaded files
        credentials_path (str, optional): Path to service account JSON file
        project_id (str, optional): Google Cloud project ID
        
    Returns:
        For single file: bool (success status)
        For multiple files: Dict[str, bool] (file paths as keys, success status as values)
    """
    # Initialize GCS client
    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        client = storage.Client(credentials=credentials, project=project_id)
        logger.info(f"Initialized GCS client with service account: {credentials_path}")
    else:
        # Use default credentials (e.g., from environment or gcloud auth)
        client = storage.Client(project=project_id)
        logger.info("Initialized GCS client with default credentials")
    
    # Normalize input to list
    if isinstance(file_paths, (str, Path)):
        file_paths = [file_paths]
        single_file = True
    else:
        single_file = False
    
    # Normalize destination names
    if destination_names is None:
        destination_names = [None] * len(file_paths)
    elif isinstance(destination_names, str):
        destination_names = [destination_names]
    
    if len(destination_names) != len(file_paths):
        logger.error("Number of destination names must match number of files")
        return False if single_file else {}
    
    results = {}
    
    for i, file_path in enumerate(file_paths):
        try:
            local_path = Path(file_path)
            
            # Validate file exists
            if not local_path.exists():
                logger.error(f"File not found: {local_path}")
                results[str(file_path)] = False
                continue
            
            # Validate it's a CSV file
            if local_path.suffix.lower() != '.csv':
                logger.warning(f"File {local_path} doesn't have .csv extension")
            
            # Set destination blob name
            if destination_names[i] is None:
                destination_blob_name = local_path.name
            else:
                destination_blob_name = destination_names[i]
            
            # Add folder prefix if provided
            if folder_prefix:
                destination_blob_name = f"{folder_prefix}{destination_blob_name}"
            
            # Get bucket
            bucket = client.bucket(bucket_name)
            
            # Create blob
            blob = bucket.blob(destination_blob_name)
            
            # Upload file
            blob.upload_from_filename(str(local_path), content_type=content_type)
            
            logger.info(f"Successfully uploaded {local_path} to gs://{bucket_name}/{destination_blob_name}")
            results[str(file_path)] = True
            
        except Exception as e:
            logger.error(f"Failed to upload {file_path}: {str(e)}")
            results[str(file_path)] = False
    
    # Return appropriate format based on input
    if single_file:
        return results[str(file_paths[0])]
    else:
        return results


def detect_and_upload_local_files(
    local_raw_dir: str = None,
    processed_files_file: str = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Detect new CSV files in local directory and upload them to GCS.
    
    Args:
        local_raw_dir (str): Path to local raw data directory
        processed_files_file (str): Path to file tracking processed files
        dry_run (bool): If True, only detect files without uploading
        
    Returns:
        Dict[str, Any]: Results dictionary with upload information
    """
    # Default paths
    if local_raw_dir is None:
        local_raw_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw')
    if processed_files_file is None:
        processed_files_file = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'processed_files.txt')
    
    local_raw_path = Path(local_raw_dir)
    processed_files_path = Path(processed_files_file)
    
    print(f"Scanning directory: {local_raw_path}")
    print(f"Processed files tracking: {processed_files_path}")
    
    # Get list of CSV files in local directory
    csv_files = list(local_raw_path.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in local directory:")
    for file_path in csv_files:
        print(f"  - {file_path.name}")
    
    # Load previously processed files
    processed_files = set()
    if processed_files_path.exists():
        with open(processed_files_path, 'r') as f:
            processed_files = set(line.strip() for line in f if line.strip())
        print(f"Found {len(processed_files)} previously processed files")
    
    # Find new files (not yet processed)
    new_files = []
    for file_path in csv_files:
        if file_path.name not in processed_files:
            new_files.append(file_path)
    
    print(f"Found {len(new_files)} new files to process:")
    for file_path in new_files:
        print(f"  - {file_path.name}")
    
    if not new_files:
        print("No new files to process")
        return {
            'status': 'no_new_files',
            'uploaded_files': [],
            'upload_count': 0,
            'failed_files': [],
            'message': 'No new files to process'
        }
    
    if dry_run:
        print("DRY RUN: Would upload the following files:")
        for file_path in new_files:
            print(f"  - {file_path.name}")
        return {
            'status': 'dry_run',
            'uploaded_files': [f.name for f in new_files],
            'upload_count': len(new_files),
            'failed_files': [],
            'message': f'DRY RUN: Would upload {len(new_files)} files'
        }
    
    # Upload new files to GCS
    print("Starting upload to GCS...")
    try:
        config = get_gcs_config()
        
        upload_results = upload_files(
            file_paths=new_files,
            bucket_name=config['bucket_name'],
            folder_prefix=config['folder_prefix_raw'],
            credentials_path=config['credentials_path'],
            project_id=config['project_id']
        )
        
        # Track successful uploads
        uploaded_files = []
        failed_files = []
        
        for file_path, success in upload_results.items():
            if success:
                uploaded_files.append(file_path.name)
                # Mark file as processed
                processed_files.add(file_path.name)
                print(f"✓ Successfully uploaded: {file_path.name}")
            else:
                failed_files.append(file_path.name)
                print(f"✗ Failed to upload: {file_path.name}")
        
        # Save updated processed files list
        with open(processed_files_path, 'w') as f:
            for filename in processed_files:
                f.write(f"{filename}\n")
        
        return {
            'status': 'completed',
            'uploaded_files': uploaded_files,
            'upload_count': len(uploaded_files),
            'failed_files': failed_files,
            'message': f'Successfully uploaded {len(uploaded_files)} files'
        }
        
    except Exception as e:
        print(f"Error during upload: {str(e)}")
        return {
            'status': 'error',
            'uploaded_files': [],
            'upload_count': 0,
            'failed_files': [f.name for f in new_files],
            'message': f'Upload failed: {str(e)}'
        }


def detect_local_files_task(**context):
    """
    Airflow task function for detecting and uploading local files.
    This function is designed to be called by Airflow PythonOperator.
    
    Args:
        **context: Airflow task context
        
    Returns:
        str: Status message
    """
    try:
        # Get parameters from Airflow context or use defaults
        local_raw_dir = context.get('params', {}).get('local_raw_dir')
        processed_files_file = context.get('params', {}).get('processed_files_file')
        dry_run = context.get('params', {}).get('dry_run', False)
        
        # Call the main function
        result = detect_and_upload_local_files(
            local_raw_dir=local_raw_dir,
            processed_files_file=processed_files_file,
            dry_run=dry_run
        )
        
        # Store results in XCom for downstream tasks
        context['task_instance'].xcom_push(
            key='uploaded_files', 
            value=result['uploaded_files']
        )
        context['task_instance'].xcom_push(
            key='upload_count', 
            value=result['upload_count']
        )
        context['task_instance'].xcom_push(
            key='failed_files', 
            value=result['failed_files']
        )
        context['task_instance'].xcom_push(
            key='status', 
            value=result['status']
        )
        
        return result['message']
        
    except Exception as e:
        error_msg = f"Error in file detection and upload: {str(e)}"
        print(error_msg)
        raise


if __name__ == "__main__":
    """
    Standalone usage example for testing the file detection and upload.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Detect and upload local CSV files to GCS')
    parser.add_argument('--local-dir', help='Local raw data directory path')
    parser.add_argument('--processed-file', help='Processed files tracking file path')
    parser.add_argument('--dry-run', action='store_true', help='Only detect files without uploading')
    
    args = parser.parse_args()
    
    try:
        result = detect_and_upload_local_files(
            local_raw_dir=args.local_dir,
            processed_files_file=args.processed_file,
            dry_run=args.dry_run
        )
        
        print("\n" + "="*50)
        print("RESULTS:")
        print(f"Status: {result['status']}")
        print(f"Message: {result['message']}")
        print(f"Uploaded files: {result['uploaded_files']}")
        print(f"Failed files: {result['failed_files']}")
        print(f"Upload count: {result['upload_count']}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
