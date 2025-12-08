from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
import pendulum

# Configuration from environment variables
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
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

# Path to dbt project (mounted in docker-compose)
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
DBT_PROFILES_DIR = '/opt/airflow/config'

# Define the DAG
with DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='Run dbt models for data transformation, feature engineering and automated training pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'bronze', 'silver', 'gold', 'data-transformation'],
) as dag:
    
    # Task 1: Verify dbt project directory exists and run dbt deps
    dbt_deps = BashOperator(
        task_id='dbt_install_dependencies',
        bash_command=f'''
            export PATH="$PATH:/home/airflow/.local/bin"
            if [ ! -d "{DBT_PROJECT_DIR}" ]; then
                echo "ERROR: dbt project directory not found: {DBT_PROJECT_DIR}"
                echo "Please ensure the volume is mounted in docker-compose.yaml"
                exit 1
            fi
            cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}
        ''',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Task 2: Run bronze models (disabled for now)
    # dbt_run_bronze = BashOperator(
    #     task_id='dbt_run_bronze',
    #     bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select bronze.* --profiles-dir {DBT_PROFILES_DIR}',
    #     env={
    #         'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
    #         'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
    #         'BIGQUERY_DATASET': BIGQUERY_DATASET,
    #         'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
    #     }
    # )
    
    # Task 3: Run silver models
    dbt_run_silver = BashOperator(
        task_id='dbt_silver_feature_engineering',
        bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select silver.* --profiles-dir {DBT_PROFILES_DIR}',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
            'BIGQUERY_DATASET': BIGQUERY_DATASET,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Task 4: Run gold_rulebased and gold_train models
    dbt_run_gold_train = BashOperator(
        task_id='dbt_gold_extract_train_features',
        bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select gold_rulebased gold_train --profiles-dir {DBT_PROFILES_DIR}',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
            'BIGQUERY_DATASET': BIGQUERY_DATASET,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Task 5: Run remaining gold models
    dbt_run_gold = BashOperator(
        task_id='dbt_gold_master_table',
        bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select gold.* --exclude gold_rulebased gold_train --profiles-dir {DBT_PROFILES_DIR}',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
            'BIGQUERY_DATASET': BIGQUERY_DATASET,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Task 6: Trigger model training DAG (model_training)
    trigger_model_training = TriggerDagRunOperator(
        task_id='trigger_model_training',
        trigger_dag_id='model_training',
        wait_for_completion=True,
        reset_dag_run=True
    )
    
    # Task 7: Run pred_viz models
    dbt_run_pred_viz = BashOperator(
        task_id='dbt_prediction_results_table',
        bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select pred_viz --profiles-dir {DBT_PROFILES_DIR}',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
            'BIGQUERY_DATASET': BIGQUERY_DATASET,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Task 8: Run master_viz models
    dbt_run_master_viz = BashOperator(
        task_id='dbt_master_table',
        bash_command=f'export PATH="$PATH:/home/airflow/.local/bin" && cd {DBT_PROJECT_DIR} && dbt run --select master_viz --profiles-dir {DBT_PROFILES_DIR}',
        env={
            'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp-key.json',
            'GCS_PROJECT_ID': GCS_PROJECT_ID or '',
            'BIGQUERY_DATASET': BIGQUERY_DATASET,
            'PATH': '/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
        }
    )
    
    # Execution order: deps -> silver -> gold_rulebased/gold_train -> remaining gold -> model_training DAG -> pred_viz -> master_viz
    dbt_deps >> dbt_run_silver >> dbt_run_gold_train >> dbt_run_gold >> trigger_model_training >> dbt_run_pred_viz >> dbt_run_master_viz

