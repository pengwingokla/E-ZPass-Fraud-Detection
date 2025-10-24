"""
Apache Airflow DAG for E-ZPass File Upload and Detection.
This DAG uploads local CSV files to GCS and then detects their presence in GCS.
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner': 'ezpass-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': [os.getenv('NOTIFICATION_EMAIL')],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG configuration
dag = DAG(
    'ezpass_file_upload_and_detection',
    default_args=default_args,
    description='E-ZPass: Upload Local Files to GCS and Detect Presence',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['ezpass', 'local-files', 'gcs-upload', 'gcs-detection'],
)

# Configuration from environment variables
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")
GCS_CONN_ID = os.getenv("GCS_CONN_ID", "google_cloud_default")

# Validate required environment variables
required_vars = {
    'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
    'GCS_PROJECT_ID': GCS_PROJECT_ID,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")


def detect_local_files_task(**context):
    """
    Airflow task function that calls the separate detect_and_upload module.
    """
    import sys
    sys.path.append('/opt/airflow/dags/src/orchestration')
    
    from detect_and_upload import detect_local_files_task as detect_task
    
    return detect_task(**context)


# Task definitions
start_task = DummyOperator(
    task_id='start_detection',
    dag=dag,
)

# Local file detection and upload task
detect_and_upload_files = PythonOperator(
    task_id='detect_and_upload_files',
    python_callable=detect_local_files_task,
    dag=dag,
)

# GCS sensor to check if raw data files are uploaded
gcs_sensor_check = GCSObjectExistenceSensor(
    task_id='check_gcs_data_uploaded',
    bucket=GCS_BUCKET_NAME,
    object='data/raw/*.csv',
    google_cloud_conn_id=GCS_CONN_ID,
    mode='poke',
    poke_interval=60,
    timeout=3600,
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end_detection',
    dag=dag,
)

# Task dependencies - upload local files then detect in GCS
start_task >> detect_and_upload_files >> check_gcs_data_uploaded >> end_task
