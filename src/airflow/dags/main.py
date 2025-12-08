from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import shutil
import re
import sys
from pathlib import Path
import pendulum
import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================
RAW_DATA_PATH = '/opt/airflow/data/raw/'
INTERIM_PATH = '/opt/airflow/data/interim/'
NORMALIZED_PATH = '/opt/airflow/data/interim/normalized/'
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_PREFIX = os.getenv('GCS_FOLDER_PREFIX_RAW', 'data/raw/')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'ezpass_data')
BIGQUERY_RAW_TABLE = os.getenv('BIGQUERY_RAW_TABLE', 'bronze')
BIGQUERY_TRAIN = os.getenv('BIGQUERY_TRAIN', 'gold_train')
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
DBT_PROFILES_DIR = '/opt/airflow/config'
ML_TRAINING_PATH = '/opt/airflow/ml_train'

# Column mapping from raw to normalized names
COLUMN_MAPPING = {
    'POSTING DATE': 'posting_date',
    'TRANSACTION DATE': 'transaction_date',
    'TAG/PLATE NUMBER': 'tag_plate_number',
    'AGENCY': 'agency',
    'DESCRIPTION': 'description',
    'ENTRY TIME': 'entry_time',
    'ENTRY PLAZA': 'entry_plaza',
    'ENTRY LANE': 'entry_lane',
    'EXIT TIME': 'exit_time',
    'EXIT PLAZA': 'exit_plaza',
    'EXIT LANE': 'exit_lane',
    'VEHICLE TYPE CODE': 'vehicle_type_code',
    'AMOUNT': 'amount',
    'PREPAID': 'prepaid',
    'PLAN/RATE': 'plan_rate',
    'FARE TYPE': 'fare_type',
    'BALANCE': 'balance'
}

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

# Add to Python path so we can import the training module
sys.path.insert(0, ML_TRAINING_PATH)

# ============================================================================
# GCS UPLOAD FUNCTIONS (from gcs_upload_raw)
# ============================================================================
def detect_files(**context):
    """
    Step 1: Detect new CSV files in data/raw/
    Returns list of detected file paths
    """
    raw_path = Path(RAW_DATA_PATH)
    
    # Get all CSV files in raw directory
    csv_files = list(raw_path.glob('*.csv'))
    
    if not csv_files:
        return []
    
    detected_files = [str(f) for f in csv_files]
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='detected_files', value=detected_files)
    
    return detected_files

def normalize_columns(**context):
    """
    Step 2: Normalize column names from UPPER CASE to snake_case
    Reads files, normalizes columns, converts date formats, and saves to interim/normalized/
    """
    ti = context['ti']
    detected_files = ti.xcom_pull(key='detected_files', task_ids='detect_files')
    
    if not detected_files:
        return []
    
    normalized_path = Path(NORMALIZED_PATH)
    normalized_path.mkdir(parents=True, exist_ok=True)
    
    normalized_files = []
    
    for file_path in detected_files:
        csv_file = Path(file_path)
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            original_cols = list(df.columns)
            
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            
            # Normalize column names
            df.rename(columns=COLUMN_MAPPING, inplace=True)
            
            # Convert date/timestamp columns to proper formats for BigQuery
            # Handle dates (MM/DD/YYYY format)
            date_columns = ['posting_date', 'transaction_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            
            # Handle timestamps - entry_time and exit_time are time-only (HH:MM:SS)
            # Combine them with transaction_date to create full timestamps
            if 'transaction_date' in df.columns:
                for time_col in ['entry_time', 'exit_time']:
                    if time_col in df.columns:
                        # Replace '-' with NaN for empty time values
                        df[time_col] = df[time_col].replace('-', pd.NA)
                        
                        # Combine transaction_date with time to create timestamp
                        combined = df['transaction_date'].astype(str) + ' ' + df[time_col].astype(str)
                        
                        # Parse as datetime
                        df[time_col] = pd.to_datetime(combined, errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert numeric columns
            numeric_columns = ['amount', 'balance']
            for col in numeric_columns:
                if col in df.columns:
                    # Convert to string
                    df[col] = df[col].astype(str).str.strip()

                    # Remove $ signs and commas
                    df[col] = df[col].str.replace('$', '', regex=False).str.replace(',', '', regex=False)

                    # Handle parentheses 
                    # ($60.56) -> -60.56
                    mask = df[col].str.contains(r'\(.*\)', regex=True, na=False)
                    df.loc[mask, col] = '-' + df.loc[mask, col].str.replace('(', '', regex=False).str.replace(')', '', regex=False)

                    # Convert to numeric
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add metadata columns
            df['loaded_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            df['source_file'] = csv_file.name
            
            normalized_cols = list(df.columns)
            
            # Save normalized file (keep original filename for now)
            normalized_file = normalized_path / csv_file.name
            df.to_csv(normalized_file, index=False)
            
            normalized_files.append(str(normalized_file))
            
        except Exception as e:
            print(f"  ✗ Error normalizing {csv_file.name}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='normalized_files', value=normalized_files)
    
    return normalized_files

def rename_files(**context):
    """
    Step 3: Rename files to transaction_{year}_{month}.csv format
    Extracts date from filename and renames accordingly
    """
    ti = context['ti']
    normalized_files = ti.xcom_pull(key='normalized_files', task_ids='normalize_columns')
    
    if not normalized_files:
        return []
    
    interim_path = Path(INTERIM_PATH)
    interim_path.mkdir(parents=True, exist_ok=True)
    
    renamed_files = []
    
    for file_path in normalized_files:
        normalized_file = Path(file_path)
        original_name = normalized_file.name
        
        # Extract date from filename
        date_match = re.search(r'(\d{4})[-_]?(\d{2})', original_name)
        
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
        else:
            # Try to extract month name and year
            month_names = {
                'january': 'january', 'jan': 'january',
                'february': 'february', 'feb': 'february', 
                'march': 'march', 'mar': 'march',
                'april': 'april', 'apr': 'april',
                'may': 'may',
                'june': 'june', 'jun': 'june',
                'july': 'july', 'jul': 'july',
                'august': 'august', 'aug': 'august',
                'september': 'september', 'sep': 'september', 'sept': 'september',
                'october': 'october', 'oct': 'october',
                'november': 'november', 'nov': 'november',
                'december': 'december', 'dec': 'december'
            }
            
            # Look for month name and year pattern (case insensitive)
            month_year_match = re.search(r'(?:transactions?|transaction)\s+(\w+)\s+(\d{4})', original_name.lower())
            
            if month_year_match:
                month_name = month_year_match.group(1)
                year = month_year_match.group(2)
                normalized_month = month_names.get(month_name, None)
                
                if normalized_month:
                    month = normalized_month
                else:
                    # Use current date if month name not recognized
                    now = datetime.now()
                    year = now.strftime('%Y')
                    month = now.strftime('%B').lower()
            else:
                # Use current date if no date found in filename
                now = datetime.now()
                year = now.strftime('%Y')
                month = now.strftime('%B').lower()
        
        # Create new filename
        new_filename = f'transaction_{year}_{month}.csv'
        new_filepath = interim_path / new_filename
        
        # Move file to interim with new name
        shutil.move(str(normalized_file), str(new_filepath))
        renamed_files.append(str(new_filepath))
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='renamed_files', value=renamed_files)
    
    return renamed_files

def upload_to_gcs(**context):
    """
    Step 4: Upload renamed files to Google Cloud Storage
    Checks if file exists in GCS before uploading - skips if already exists
    """
    from google.cloud import storage
    
    ti = context['ti']
    renamed_files = ti.xcom_pull(key='renamed_files', task_ids='rename_files')
    
    if not renamed_files:
        return 0
    
    # Validate environment variables
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set")
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID environment variable is not set")
    
    # Initialize GCS client
    client = storage.Client(project=GCS_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    uploaded_count = 0
    skipped_count = 0
    for local_file in renamed_files:
        filename = Path(local_file).name
        blob_name = f"{GCS_PREFIX.rstrip('/')}/{filename}"
        
        try:
            blob = bucket.blob(blob_name)
            
            # Check if file already exists in GCS
            if blob.exists():
                skipped_count += 1
                continue
            
            # File doesn't exist, proceed with upload
            blob.upload_from_filename(local_file)
            uploaded_count += 1
            
        except Exception as e:
            print(f"  ✗ Error uploading {filename}: {str(e)}")
            continue
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='uploaded_count', value=uploaded_count)
    context['ti'].xcom_push(key='skipped_count', value=skipped_count)
    
    return uploaded_count

def verify_gcs_upload(**context):
    """
    Step 5: Verify that all uploaded files exist in GCS
    """
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    
    ti = context['ti']
    renamed_files = ti.xcom_pull(key='renamed_files', task_ids='rename_files')
    uploaded_count = ti.xcom_pull(key='uploaded_count', task_ids='upload_to_gcs')
    
    if not renamed_files:
        return True
    
    # Validate environment variables
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set")
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID environment variable is not set")
    
    # Initialize GCS client
    client = storage.Client(project=GCS_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    verification_results = []
    all_verified = True
    
    for local_file in renamed_files:
        filename = Path(local_file).name
        blob_name = f"{GCS_PREFIX.rstrip('/')}/{filename}"
        
        try:
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                blob.reload()
                file_size = blob.size
                created_time = blob.time_created
                
                verification_results.append({
                    'filename': filename,
                    'blob_name': blob_name,
                    'exists': True,
                    'size': file_size,
                    'created': created_time
                })
            else:
                verification_results.append({
                    'filename': filename,
                    'blob_name': blob_name,
                    'exists': False
                })
                all_verified = False
                
        except NotFound:
            verification_results.append({
                'filename': filename,
                'blob_name': blob_name,
                'exists': False,
                'error': 'NotFound'
            })
            all_verified = False
            
        except Exception as e:
            verification_results.append({
                'filename': filename,
                'blob_name': blob_name,
                'exists': False,
                'error': str(e)
            })
            all_verified = False
    
    if not all_verified:
        verified_count = sum(1 for r in verification_results if r.get('exists', False))
        total_count = len(verification_results)
        error_msg = f"Upload verification failed: {total_count - verified_count} out of {total_count} files not found in GCS"
        raise ValueError(error_msg)
    
    # Push results to XCom
    context['ti'].xcom_push(key='verification_results', value=verification_results)
    
    return all_verified

# ============================================================================
# BIGQUERY LOAD FUNCTIONS (from gcs_to_bigquery)
# ============================================================================
def detect_new_files(**context):
    """
    Step 1: Detect files in GCS that haven't been loaded to BigQuery yet
    Compares GCS files against source_file column in BigQuery bronze table
    """
    from google.cloud import storage
    from google.cloud import bigquery
    
    if not GCS_BUCKET or not GCS_PROJECT_ID:
        raise ValueError("GCS_BUCKET_NAME and GCS_PROJECT_ID must be set")
    
    # Initialize clients
    storage_client = storage.Client(project=GCS_PROJECT_ID)
    bq_client = bigquery.Client(project=GCS_PROJECT_ID)
    
    # Step 1: Get all CSV files from GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))
    
    gcs_files = []
    for blob in blobs:
        if blob.name.endswith('.csv') and blob.name != GCS_PREFIX and not blob.name.endswith('/'):
            filename = blob.name.split('/')[-1]  # Extract just the filename
            gcs_files.append({
                'full_path': blob.name,
                'filename': filename,
                'size': blob.size,
                'updated': blob.updated
            })
    
    # Step 2: Get already loaded files from BigQuery
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_RAW_TABLE}"
    loaded_files = set()
    
    try:
        # Query to get distinct source files already in BigQuery
        query = f"""
        SELECT DISTINCT source_file
        FROM `{table_id}`
        WHERE source_file IS NOT NULL
        """
        
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            loaded_files.add(row.source_file)
            
    except Exception as e:
        # Table doesn't exist or is empty - all files are new
        pass
    
    # Step 3: Find new files (in GCS but not in BigQuery)
    new_files = []
    for file_info in gcs_files:
        if file_info['filename'] not in loaded_files:
            new_files.append(file_info['full_path'])
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='gcs_files', value=new_files)
    context['ti'].xcom_push(key='new_files_count', value=len(new_files))
    
    return new_files

def delete_bronze_table(**context):
    """
    Step 2: Delete the bronze table if it exists
    """
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_RAW_TABLE}"
    
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"✓ Deleted bronze table: {table_id}")
    except Exception as e:
        print(f"⚠️ Could not delete table {table_id}: {str(e)}")
        pass
    
    return table_id

def create_bq_dataset(**context):
    """
    Step 3: Create BigQuery dataset if it doesn't exist
    """
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    # Initialize BigQuery client
    client = bigquery.Client(project=GCS_PROJECT_ID)
    dataset_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        dataset = client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "EZ-Pass Transaction Data Bronze (raw data)"
        
        dataset = client.create_dataset(dataset, timeout=30)
        pass
    
    # Push dataset info to XCom
    context['ti'].xcom_push(key='dataset_id', value=dataset_id)
    
    return dataset_id

def load_gcs_to_bigquery(**context):
    """
    Step 4: Load new CSV files from GCS into BigQuery bronze table
    Only loads files that don't already exist in BigQuery
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    gcs_files = ti.xcom_pull(key='gcs_files', task_ids='detect_new_files')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='create_bq_dataset')
    new_files_count = ti.xcom_pull(key='new_files_count', task_ids='detect_new_files')
    
    if not gcs_files or new_files_count == 0:
        print("No new files to load")
        context['ti'].xcom_push(key='loaded_files', value=[])
        context['ti'].xcom_push(key='failed_files', value=[])
        context['ti'].xcom_push(key='total_rows_loaded', value=0)
        return 0
    
    print("\n=== Loading Data to BigQuery ===")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=GCS_PROJECT_ID)
    
    # Define table schema matching normalized column names
    schema = [
        bigquery.SchemaField("posting_date", "STRING"),
        bigquery.SchemaField("transaction_date", "STRING"),
        bigquery.SchemaField("tag_plate_number", "STRING"),
        bigquery.SchemaField("agency", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("entry_time", "STRING"),
        bigquery.SchemaField("entry_plaza", "STRING"),
        bigquery.SchemaField("entry_lane", "STRING"),
        bigquery.SchemaField("exit_time", "STRING"),
        bigquery.SchemaField("exit_plaza", "STRING"),
        bigquery.SchemaField("exit_lane", "STRING"),
        bigquery.SchemaField("vehicle_type_code", "STRING"),
        bigquery.SchemaField("amount", "STRING"),
        bigquery.SchemaField("prepaid", "STRING"),
        bigquery.SchemaField("plan_rate", "STRING"),
        bigquery.SchemaField("fare_type", "STRING"),
        bigquery.SchemaField("balance", "STRING"),
        bigquery.SchemaField("loaded_at", "STRING"),
        bigquery.SchemaField("source_file", "STRING"),
    ]
    
    table_id = f"{dataset_id}.{BIGQUERY_RAW_TABLE}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        max_bad_records=100,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        field_delimiter=',',
        quote_character='"',
    )
    
    loaded_files = []
    failed_files = []
    total_rows_loaded = 0
    
    for gcs_file in gcs_files:
        uri = f"gs://{GCS_BUCKET}/{gcs_file}"
        filename = gcs_file.split('/')[-1]
        
        print(f"\n--- Loading: {filename} ---")
        print(f"Source: {uri}")
        print(f"Destination: {table_id}")
        
        try:
            # Start load job
            load_job = client.load_table_from_uri(
                uri,
                table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result()
            
            # Check for errors
            if load_job.errors:
                print(f"✗ FAILED with errors:")
                for error in load_job.errors[:5]:
                    print(f"  - {error}")
                raise Exception(f"Load job had {len(load_job.errors)} errors")
            
            # Get updated table info
            destination_table = client.get_table(table_id)
            rows_loaded = load_job.output_rows or 0
            total_rows_loaded += rows_loaded
            
            print(f"✓ SUCCESS")
            print(f"  Rows loaded: {rows_loaded:,}")
            print(f"  Total rows in table: {destination_table.num_rows:,}")
            print(f"  Job ID: {load_job.job_id}")
            
            loaded_files.append({
                'file': filename,
                'rows': rows_loaded,
                'status': 'success'
            })
            
        except Exception as e:
            error_msg = str(e)
            print(f"✗ FAILED")
            print(f"  Error: {error_msg}")
            
            failed_files.append({
                'file': filename,
                'error': error_msg,
                'status': 'failed'
            })
            
            continue
    
    print(f"\nLOAD SUMMARY")
    print(f"Total files processed: {len(gcs_files)}")
    print(f"Successfully loaded: {len(loaded_files)}")
    print(f"Failed: {len(failed_files)}")
    print(f"Total rows loaded: {total_rows_loaded:,}")
    print(f"Destination table: {table_id}")
    
    # Push results to XCom
    context['ti'].xcom_push(key='loaded_files', value=loaded_files)
    context['ti'].xcom_push(key='failed_files', value=failed_files)
    context['ti'].xcom_push(key='total_rows_loaded', value=total_rows_loaded)
    
    # Fail task if any files failed
    if failed_files:
        raise ValueError(f"Failed to load {len(failed_files)} out of {len(gcs_files)} files")
    
    return total_rows_loaded

def verify_bigquery_load(**context):
    """
    Step 5: Verify data was loaded correctly into BigQuery
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='create_bq_dataset')
    total_rows_loaded = ti.xcom_pull(key='total_rows_loaded', task_ids='load_to_bigquery')
    loaded_files = ti.xcom_pull(key='loaded_files', task_ids='load_to_bigquery')
    
    if not dataset_id:
        print("⚠️ No dataset_id found - skipping verification")
        return True
    
    if total_rows_loaded == 0:
        print("✓ No new rows loaded this run, skipping verification")
        return True
    
    # Initialize BigQuery client
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{dataset_id}.{BIGQUERY_RAW_TABLE}"
    
    # Get table info
    table = client.get_table(table_id)
    
    print(f"Table: {table_id}")
    print(f"Total rows: {table.num_rows:,}")
    print(f"Size: {table.num_bytes:,} bytes")
    print(f"Created: {table.created}")
    print(f"Modified: {table.modified}")
    
    # Run a simple query to verify data
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT source_file) as unique_files,
        MIN(transaction_date) as earliest_transaction,
        MAX(transaction_date) as latest_transaction,
        SUM(SAFE_CAST(amount AS FLOAT64)) as total_amount
    FROM `{table_id}`
    """
    
    print("\nRunning verification query...")
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"\n✓ Data Verification:")
            print(f"  Total rows: {row.total_rows:,}")
            print(f"  Unique files: {row.unique_files}")
            print(f"  Date range: {row.earliest_transaction} to {row.latest_transaction}")
            print(f"  Total amount: ${row.total_amount:,.2f}" if row.total_amount else "  Total amount: N/A")
        
        print(f"\n✓ Files loaded this run: {len(loaded_files) if loaded_files else 0}")
        if loaded_files:
            for file_info in loaded_files:
                print(f"  - {file_info['file']}: {file_info['rows']:,} rows")
        
        print("\n✓ All verifications passed!")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Verification failed: {str(e)}")
        raise

# ============================================================================
# ML TRAINING FUNCTIONS (from training_pipeline)
# ============================================================================
def create_ml_dataset(**context):
    """Create BigQuery dataset if it doesn't exist (for ML training)"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    dataset_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"✓ Dataset already exists: {dataset_id}")
    except Exception:
        print(f"Creating new dataset: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "EZ-Pass Transaction Data"
        
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✓ Created dataset: {dataset_id}")
    
    return dataset_id

def create_training_metrics_table(**context):
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

def delete_predictions_table(**context):
    """Delete fraud_predictions table if it exists"""
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    
    client = bigquery.Client(project=GCS_PROJECT_ID)
    table_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}.fraud_predictions"
    
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"✓ Deleted table: {table_id}")
    except Exception as e:
        print(f"⚠ Could not delete table {table_id}: {e}")
        raise
    
    return table_id

def create_predictions_table(**context):
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

def run_fraud_training(**context):
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

# ============================================================================
# DAG DEFINITION
# ============================================================================
with DAG(
    'main',
    default_args=default_args,
    description='Main pipeline: File detection -> GCS upload -> BigQuery load -> dbt transformation -> ML training',
    schedule_interval=None,
    catchup=False,
    tags=['main', 'orchestration', 'etl', 'ml'],
) as dag:
    
    # ========================================================================
    # PHASE 1: FILE DETECTION & GCS UPLOAD PIPELINE
    # ========================================================================
    detect_files_task = PythonOperator(
        task_id='detect_files',
        python_callable=detect_files,
        provide_context=True,
    )
    
    normalize = PythonOperator(
        task_id='normalize_columns',
        python_callable=normalize_columns,
        provide_context=True,
    )
    
    rename_cols = PythonOperator(
        task_id='rename_files',
        python_callable=rename_files,
        provide_context=True,
    )
    
    upload_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
    )
    
    verify_upload = PythonOperator(
        task_id='verify_gcs_upload',
        python_callable=verify_gcs_upload,
        provide_context=True,
    )
    
    # ========================================================================
    # PHASE 3: BIGQUERY LOAD PIPELINE
    # ========================================================================
    detect_new_files_gcs = PythonOperator(
        task_id='detect_new_files',
        python_callable=detect_new_files,
        provide_context=True,
    )
    
    delete_BQ_bronze = PythonOperator(
        task_id='delete_bronze_table',
        python_callable=delete_bronze_table,
        provide_context=True,
    )
    
    create_BQ_dataset = PythonOperator(
        task_id='create_bq_dataset',
        python_callable=create_bq_dataset,
        provide_context=True,
    )
    
    moveto_BQ = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_gcs_to_bigquery,
        provide_context=True,
    )
    
    verify_BQ = PythonOperator(
        task_id='verify_bigquery_load',
        python_callable=verify_bigquery_load,
        provide_context=True,
    )
    
    # ========================================================================
    # PHASE 4: DBT TRANSFORMATION PIPELINE
    # ========================================================================
    dbt_install_deps = BashOperator(
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
    
    # ========================================================================
    # PHASE 5: ML TRAINING PIPELINE
    # ========================================================================
    create_ml_dataset_task = PythonOperator(
        task_id='create_ml_dataset',
        python_callable=create_ml_dataset
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
    
    # ========================================================================
    # PHASE 6: DBT POST-TRAINING PIPELINE
    # ========================================================================
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
    
    # ========================================================================
    # TASK DEPENDENCIES
    # ========================================================================
    # Phase 1: File detection & GCS upload pipeline
    detect_files_task >> normalize >> rename_cols >> upload_gcs >> verify_upload
    
    # Phase 3: BigQuery load pipeline
    verify_upload >> detect_new_files_gcs >> delete_BQ_bronze >> create_BQ_dataset >> moveto_BQ >> verify_BQ
    
    # Phase 4: DBT transformation pipeline
    verify_BQ >> dbt_install_deps >> dbt_run_silver >> dbt_run_gold_train >> dbt_run_gold
    
    # Phase 5: ML training pipeline
    dbt_run_gold >> create_ml_dataset_task >> create_training_metrics_table_task >> delete_predictions_table_task >> create_predictions_table_task >> train_fraud_model_task
    
    # Phase 6: DBT post-training pipeline
    train_fraud_model_task >> dbt_run_pred_viz >> dbt_run_master_viz

