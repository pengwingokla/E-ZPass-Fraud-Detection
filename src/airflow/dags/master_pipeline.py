from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import pendulum
from pathlib import Path

# Configuration
RAW_DATA_PATH = '/opt/airflow/data/raw/'

# Set timezone to Eastern Time
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def detect_csv_files(**context):
    """
    Detect new CSV files in data/raw/ directory
    Returns True if CSV files are found, False otherwise
    """
    raw_path = Path(RAW_DATA_PATH)
    
    if not raw_path.exists():
        print(f"Directory does not exist: {RAW_DATA_PATH}")
        return False
    
    # Get all CSV files in raw directory
    csv_files = list(raw_path.glob('*.csv'))
    
    if csv_files:
        print(f"✓ Found {len(csv_files)} CSV file(s) in {RAW_DATA_PATH}:")
        for csv_file in csv_files:
            print(f"  - {csv_file.name}")
        return True
    else:
        print(f"No CSV files found in {RAW_DATA_PATH}")
        return False

# Define the master DAG
with DAG(
    'master_pipeline',
    default_args=default_args,
    description='Master pipeline that orchestrates: File detection -> GCS upload -> BigQuery load -> dbt transformation',
    schedule_interval=None,
    catchup=False,
    tags=['master', 'orchestration', 'etl'],
) as dag:
    
    # Step 0: Detect new CSV files in data/raw/
    detect_files = PythonSensor(
        task_id='detect_csv_files',
        python_callable=detect_csv_files,
        poke_interval=10,  # Check every 10 seconds
        timeout=60,  # Check once, timeout after 60 seconds if no files
        mode='poke',  # Check for files
    )
    
    # Step 1: Trigger GCS upload raw data DAG
    trigger_gcs_upload = TriggerDagRunOperator(
        task_id='trigger_gcs_upload_raw',
        trigger_dag_id='gcs_upload_raw',
        wait_for_completion=True,
        reset_dag_run=True
    )
    
    # Step 2: Trigger GCS to BigQuery pipeline
    trigger_gcs_to_bigquery = TriggerDagRunOperator(
        task_id='trigger_gcs_to_bigquery',
        trigger_dag_id='gcs_to_bigquery_pipeline',
        wait_for_completion=True,
        reset_dag_run=True
    )
    
    # Step 3: Trigger dbt pipeline (which includes model training and viz)
    trigger_dbt_pipeline = TriggerDagRunOperator(
        task_id='trigger_dbt_pipeline',
        trigger_dag_id='dbt_pipeline',
        wait_for_completion=True,
        reset_dag_run=True
    )
    
    # Execution order: detect files -> gcs_upload_raw -> gcs_to_bigquery -> dbt_pipeline
    detect_files >> trigger_gcs_upload >> trigger_gcs_to_bigquery >> trigger_dbt_pipeline

