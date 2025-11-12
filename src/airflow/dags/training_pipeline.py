from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys
import pendulum

# Configuration from environment variables
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_PREFIX = os.getenv('GCS_FOLDER_PREFIX_RAW', 'data/raw/')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'ezpass_data')


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

# Path config
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags
ML_TRAINING_PATH = '/opt/airflow/ml_train'               # /opt/airflow/ml_train


# Add to Python path so we can import the training module
sys.path.insert(0, ML_TRAINING_PATH)

def create_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    dataset_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"✓ Dataset already exists: {dataset_id}")
        print(f"  Location: {dataset.location}")
        print(f"  Created: {dataset.created}")
    except Exception:
        print(f"Creating new dataset: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "EZ-Pass Transaction Data"
        
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✓ Created dataset: {dataset_id}")
        print(f"  Location: {dataset.location}")
    
    return dataset_id

def create_training_metrics_table():
    """Create table to track training metrics over time"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.model_training_metrics"
    
    schema = [
        bigquery.SchemaField("model_id", "STRING"),
        bigquery.SchemaField("model_type", "STRING"),
        bigquery.SchemaField("training_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("training_time_seconds", "FLOAT64"),
        bigquery.SchemaField("n_samples", "INTEGER"),
        bigquery.SchemaField("n_features", "INTEGER"),
        bigquery.SchemaField("n_estimators", "INTEGER"),
        bigquery.SchemaField("contamination", "FLOAT64"),
        bigquery.SchemaField("n_anomalies", "INTEGER"),
        bigquery.SchemaField("anomaly_rate", "FLOAT64"),
        bigquery.SchemaField("score_mean", "FLOAT64"),
        bigquery.SchemaField("score_std", "FLOAT64"),
        bigquery.SchemaField("score_min", "FLOAT64"),
        bigquery.SchemaField("score_max", "FLOAT64"),
        bigquery.SchemaField("score_median", "FLOAT64"),
    ]
    
    try:
        table = client.get_table(table_id)
        print(f"✓ Metrics table exists: {table_id}")
    except:
        print(f"Creating metrics table: {table_id}")
        table = bigquery.Table(table_id, schema=schema)
        table.description = "ML model training metrics over time"
        table = client.create_table(table)
        print(f"✓ Created metrics table: {table_id}")
    
    return table_id

def create_predictions_table():
    """Create fraud_predictions table if it doesn't exist"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.fraud_predictions"
    
    # SIMPLIFIED - Only store predictions, not all transaction data
    schema = [
        # ID columns
        bigquery.SchemaField("transaction_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tag_plate_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_updated", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"),

        # Flag columns
        bigquery.SchemaField("flag_is_weekend", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("flag_is_out_of_state", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("flag_is_vehicle_type_gt2", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("flag_is_holiday", "BOOLEAN", mode="NULLABLE"),

        # Prediction columns
        bigquery.SchemaField("is_anomaly", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("anomaly_score", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("prediction_timestamp", "TIMESTAMP", mode="REQUIRED"),  # When prediction was made
    ]
    
    try:
        table = client.get_table(table_id)
        print(f"✓ Table already exists: {table_id}")
        print(f"  Total rows: {table.num_rows:,}")
    except Exception:
        print(f"Creating new table: {table_id}")
        table = bigquery.Table(table_id, schema=schema)
        table.description = "ML fraud predictions for EZ-Pass transactions"
        
        # Partition by prediction_timestamp for better performance
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="prediction_timestamp"
        )
        
        table = client.create_table(table)
        print(f"✓ Created table: {table_id}")
    
    return table_id

def run_fraud_training():
    import logging
    logger = logging.getLogger(__name__)

    try:
        from vertex_ai.training_pipeline import FraudDetectionTrainer
        logger.info("Successfully imported FraudDetectionTrainer")
    except ImportError as e:
        logger.error(f"Import failed: {e}")
        raise
    
    trainer = FraudDetectionTrainer(
        project_id=GCS_PROJECT_ID,
        location="us-central1",
        bq_table=f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.silver"
    )
    
    trainer.run_training_pipeline()


# Define the DAG
with DAG(
    'model_training',
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval=None,
    catchup=False,
    tags=['ml', 'fraud-detection']
) as dag:
    
    create_dataset_task = PythonOperator(
        task_id='create_dataset',
        python_callable=create_dataset
    )

    create_training_metrics_table_task = PythonOperator(
        task_id='create_training_metrics_table',
        python_callable=create_training_metrics_table
    )
    create_predictions_table_task = PythonOperator(
        task_id='create_predictions_table',
        python_callable=create_predictions_table
    )
    
    train_fraud_model_task = PythonOperator(
        task_id='train_fraud_model',
        python_callable=run_fraud_training
    )
    
    create_dataset_task >> create_training_metrics_table_task >> create_predictions_table_task >> train_fraud_model_task