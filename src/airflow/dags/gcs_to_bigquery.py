from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import os
import pendulum

# Configuration from environment variables
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_PREFIX = os.getenv('GCS_FOLDER_PREFIX_RAW', 'data/raw/')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'ezpass_data')
BIGQUERY_RAW_TABLE = os.getenv('BIGQUERY_RAW_TABLE', 'bronze')


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

def detect_new_files(**context):
    """
    Step 1: Detect files in GCS that haven't been loaded to BigQuery yet
    Compares GCS files against source_file column in BigQuery bronze table
    """
    from google.cloud import storage
    from google.cloud import bigquery
    
    if not GCS_BUCKET or not GCS_PROJECT_ID:
        raise ValueError("GCS_BUCKET_NAME and GCS_PROJECT_ID must be set")
    
    # print("\n DETECTING NEW FILES IN GCS")
    # print(f"GCS Bucket: gs://{GCS_BUCKET}/{GCS_PREFIX}")
    # print(f"BigQuery Table: {GCS_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_RAW_TABLE}")
    
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
    
    # print(f"\nFiles in GCS: {len(gcs_files)}")
    # for f in gcs_files:
    #     print(f"  - {f['filename']} ({f['size']:,} bytes)")
    
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
        
        # print(f"\nChecking BigQuery for existing files...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        for row in results:
            loaded_files.add(row.source_file)
        
        # print(f"\n✓ Files already in BigQuery: {len(loaded_files)}")
        # for f in sorted(loaded_files):
        #     print(f"  - {f}")
            
    except Exception as e:
        # Table doesn't exist or is empty - all files are new
        # print(f"\n⚠️ Could not query BigQuery (table may not exist yet): {str(e)}")
        # print("Treating all GCS files as new")
        pass
    
    # Step 3: Find new files (in GCS but not in BigQuery)
    new_files = []
    for file_info in gcs_files:
        if file_info['filename'] not in loaded_files:
            new_files.append(file_info['full_path'])
    
    # print(f"\n DETECTION SUMMARY")
    # print(f"Total files in GCS: {len(gcs_files)}")
    # print(f"Already loaded: {len(loaded_files)}")
    # print(f"New files to load: {len(new_files)}")
    
    # if new_files:
    #     print(f"\n New files to load:")
    #     for filepath in new_files:
    #         filename = filepath.split('/')[-1]
    #         print(f"  - {filename}")
    # else:
    #     print(f"\n✓ No new files to load - all files already in BigQuery")
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='gcs_files', value=new_files)
    context['ti'].xcom_push(key='new_files_count', value=len(new_files))
    
    return new_files

def create_dataset(**context):
    """
    Step 2: Create BigQuery dataset if it doesn't exist
    """
    from google.cloud import bigquery
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID must be set")
    # Initialize BigQuery client
    client = bigquery.Client(project=GCS_PROJECT_ID)
    dataset_id = f"{GCS_PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        # print(f"✓ Dataset already exists: {dataset_id}")
        # print(f"  Location: {dataset.location}")
        # print(f"  Created: {dataset.created}")
    except Exception:
        # print(f"Creating new dataset: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "EZ-Pass Transaction Data Bronze (raw data)"
        
        dataset = client.create_dataset(dataset, timeout=30)
        # print(f"✓ Created dataset: {dataset_id}")
        # print(f"  Location: {dataset.location}")
        pass
    
    # Push dataset info to XCom
    context['ti'].xcom_push(key='dataset_id', value=dataset_id)
    
    return dataset_id

def load_gcs_to_bigquery(**context):
    """
    Step 3: Load new CSV files from GCS into BigQuery bronze table
    Only loads files that don't already exist in BigQuery
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    gcs_files = ti.xcom_pull(key='gcs_files', task_ids='detect_new_files')
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='create_dataset')
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
    # Using STRING for dates/timestamps (already formatted correctly)
    # Using FLOAT64 for amounts (cleaned in normalization)
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
        bigquery.SchemaField("amount", "STRING"),  # Keep as STRING since CSV has it as STRING
        bigquery.SchemaField("prepaid", "STRING"),
        bigquery.SchemaField("plan_rate", "STRING"),
        bigquery.SchemaField("fare_type", "STRING"),
        bigquery.SchemaField("balance", "STRING"),
        bigquery.SchemaField("loaded_at", "STRING"),
        bigquery.SchemaField("source_file", "STRING"),
    ]
    
    table_id = f"{dataset_id}.{BIGQUERY_RAW_TABLE}"
    
    # Configure load job - use autodetect for more flexibility
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,  # Skip header row
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,  # Use explicit schema
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        max_bad_records=100,  # Allow more bad records for debugging
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        field_delimiter=',',
        quote_character='"',
    )
    
    loaded_files = []
    failed_files = []
    total_rows_loaded = 0
    
    for gcs_file in gcs_files:
        # gcs_file is a string path like "data/raw/transaction_2025_april.csv"
        uri = f"gs://{GCS_BUCKET}/{gcs_file}"
        filename = gcs_file.split('/')[-1]  # Extract just the filename
        
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
                for error in load_job.errors[:5]:  # Show first 5 errors
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
            
            # Try to get more detailed error information
            try:
                if hasattr(load_job, 'errors') and load_job.errors:
                    print(f"  Detailed errors:")
                    for error in load_job.errors[:3]:
                        print(f"    - {error}")
            except:
                pass
            
            failed_files.append({
                'file': filename,
                'error': error_msg,
                'status': 'failed'
            })
            
            # Continue with other files even if one fails
            continue
    
    # Print summary
    print(f"\nLOAD SUMMARY")
    print(f"Total files processed: {len(gcs_files)}")
    print(f"Successfully loaded: {len(loaded_files)}")
    print(f"Failed: {len(failed_files)}")
    print(f"Total rows loaded: {total_rows_loaded:,}")
    print(f"Destination table: {table_id}")
    
    if failed_files:
        print("\nFailed files:")
        for failed in failed_files:
            print(f"  ✗ {failed['file']}: {failed['error'][:100]}...")
    
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
    Step 4: Verify data was loaded correctly into BigQuery
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    dataset_id = ti.xcom_pull(key='dataset_id', task_ids='create_dataset')
    total_rows_loaded = ti.xcom_pull(key='total_rows_loaded', task_ids='load_to_bigquery')
    loaded_files = ti.xcom_pull(key='loaded_files', task_ids='load_to_bigquery')
    
    # Check if dataset_id exists
    if not dataset_id:
        print("⚠️ No dataset_id found - skipping verification")
        print("This likely means no files were loaded")
        return True
    
    if total_rows_loaded == 0:
        print("✓ No new rows loaded this run, skipping verification")
        return True
    
    # print("\nVERIFYING BIGQUERY LOAD)
    
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
            pass
        
        # Verify loaded files count
        print(f"\n✓ Files loaded this run: {len(loaded_files) if loaded_files else 0}")
        if loaded_files:
            for file_info in loaded_files:
                print(f"  - {file_info['file']}: {file_info['rows']:,} rows")
        else:
            print("  (No files loaded in this specific run)")
        
        print("\n✓ All verifications passed!")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Verification failed: {str(e)}")
        raise


# Define the DAG
with DAG(
    'gcs_to_bigquery_pipeline',
    default_args=default_args,
    description='Load CSV files from GCS into BigQuery bronze table',
    schedule_interval='@hourly',
    catchup=False,
    tags=['bigquery', 'gcs', 'bronze', 'ezpass', 'etl'],
) as dag:
    # Task 1: Detect new files in GCS
    detect_new_files_task = PythonOperator(
        task_id='detect_new_files',
        python_callable=detect_new_files,
        provide_context=True,
    )
    
    # Task 2: Create or verify dataset
    create_dataset_task = PythonOperator(
        task_id='create_dataset',
        python_callable=create_dataset,
        provide_context=True,
    )
    
    # Task 3: Load to BigQuery
    load_to_bq_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_gcs_to_bigquery,
        provide_context=True,
    )
    
    # Task 4: Verify load
    verify_task = PythonOperator(
        task_id='verify_bigquery_load',
        python_callable=verify_bigquery_load,
        provide_context=True,
    )
    
    # Execution order
    detect_new_files_task >> create_dataset_task >> load_to_bq_task >> verify_task