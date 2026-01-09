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
BIGQUERY_TRAIN = os.getenv('BIGQUERY_TRAIN', 'gold_train')


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

def delete_predictions_table():
    """Delete fraud_predictions table if it exists"""
    from google.cloud import bigquery
    from google.api_core import exceptions
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.fraud_predictions"
    
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"✓ Deleted table: {table_id}")
    except exceptions.Forbidden as e:
        # Permission denied - log warning but don't fail
        # The create_predictions_table function will handle existing tables
        print(f"⚠ Permission denied to delete table {table_id}: {e}")
        print(f"⚠ Continuing - create_predictions_table will handle existing table")
    except Exception as e:
        print(f"⚠ Could not delete table {table_id}: {e}")
        # Only raise for unexpected errors, not permission issues
        raise
    
    return table_id

def create_predictions_table():
    """Create fraud_predictions table if it doesn't exist"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.fraud_predictions"
    
    # Store predictions with key features for ML training
    schema = [
        # Core ID
        bigquery.SchemaField("transaction_id", "STRING", mode="NULLABLE"),
        
        # ===== FINANCIAL =====
        bigquery.SchemaField("amount", "FLOAT64", mode="NULLABLE"),
        
        # ===== VELOCITY & TRAVEL FEATURES =====
        bigquery.SchemaField("distance_miles", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("travel_time_minutes", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("speed_mph", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("overlapping_journey_duration_minutes", "FLOAT64", mode="NULLABLE"),
        
        # ===== DRIVER ROLLING STATS =====
        bigquery.SchemaField("driver_amount_last_30txn_avg", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("driver_amount_last_30txn_std", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("driver_amount_last_30txn_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("driver_daily_txn_count", "INTEGER", mode="NULLABLE"),
        
        # ===== GOLD LAYER: DRIVER FEATURES =====
        bigquery.SchemaField("driver_amount_modified_z_score", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("amount_deviation_from_avg_pct", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("amount_deviation_from_median_pct", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("driver_today_spend", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("driver_avg_daily_spend_30d", "FLOAT64", mode="NULLABLE"),
        
        # ===== GOLD LAYER: ROUTE FEATURES =====
        bigquery.SchemaField("route_amount_z_score", "FLOAT64", mode="NULLABLE"),
        
        # ===== TIME & CONTEXT =====
        bigquery.SchemaField("exit_hour", "INTEGER", mode="NULLABLE"),
        
        # ===== VEHICLE =====
        bigquery.SchemaField("vehicle_type_code", "STRING", mode="NULLABLE"),
        
        # ===== ENCODED CATEGORICAL FEATURES =====
        bigquery.SchemaField("route_name_freq_encoded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("entry_plaza_freq_encoded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("exit_plaza_freq_encoded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("vehicle_type_freq_encoded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("agency_freq_encoded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("travel_time_of_day_freq_encoded", "INTEGER", mode="NULLABLE"),
        
        # ===== ML PREDICTION COLUMNS =====
        bigquery.SchemaField("is_anomaly", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("ml_anomaly_score", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("prediction_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    try:
        table = client.get_table(table_id)
        print(f"✓ Table already exists: {table_id}")
        print(f"  Total rows: {table.num_rows:,}")
        print(f"⚠ WARNING: If schema doesn't match, manually delete the table in BigQuery console")
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
        bq_table=f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TRAIN}"
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
    
    delete_predictions_table_task = PythonOperator(
        task_id='delete_predictions_table',
        python_callable=delete_predictions_table
    )
    
    create_predictions_table_task = PythonOperator(
        task_id='create_predictions_table',
        python_callable=create_predictions_table
    )
    
    train_fraud_model_task = PythonOperator(
        task_id='train_fraud_model',
        python_callable=run_fraud_training
    )
    
    delete_predictions_table_task >> create_dataset_task >> create_training_metrics_table_task >> create_predictions_table_task >> train_fraud_model_task