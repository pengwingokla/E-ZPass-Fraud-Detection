from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
import re
from pathlib import Path
import pendulum
import pandas as pd

# Configuration from environment variables
RAW_DATA_PATH = '/opt/airflow/data/raw/'
INTERIM_PATH = '/opt/airflow/data/interim/'
NORMALIZED_PATH = '/opt/airflow/data/interim/normalized/'
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME')
GCS_PROJECT_ID = os.getenv('GCS_PROJECT_ID')
GCS_PREFIX = os.getenv('GCS_FOLDER_PREFIX_RAW', 'data/raw/')

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

def detect_files(**context):
    """
    Step 1: Detect new CSV files in data/raw/
    Returns list of detected file paths
    """
    raw_path = Path(RAW_DATA_PATH)
    
    # Get all CSV files in raw directory
    csv_files = list(raw_path.glob('*.csv'))
    
    if not csv_files:
        # print("No CSV files found in raw directory")
        return []
    
    detected_files = [str(f) for f in csv_files]
    
    # print(f"\nDETECTION SUMMARY")
    # print(f"Found {len(detected_files)} CSV file(s):")
    # for csv_file in csv_files:
    #     print(f"  - {csv_file.name}")
    
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
        # print("No files to normalize")
        return []
    
    normalized_path = Path(NORMALIZED_PATH)
    normalized_path.mkdir(parents=True, exist_ok=True)
    
    normalized_files = []
    
    # print("\nNORMALIZING COLUMN NAMES")
    
    for file_path in detected_files:
        csv_file = Path(file_path)
        # print(f"\nProcessing: {csv_file.name}")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            original_cols = list(df.columns)
            
            # print(f"  Original columns ({len(original_cols)}): {original_cols[:3]}...")
            # print(f"  Rows: {len(df):,}")
            
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
            
            # Handle timestamps (MM/DD/YYYY HH:MM:SS format)
            timestamp_columns = ['entry_time', 'exit_time']
            for col in timestamp_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert numeric columns
            numeric_columns = ['amount', 'prepaid', 'balance']
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
            # print(f"  Normalized columns ({len(normalized_cols)}): {normalized_cols[:5]}...")
            # print(f"  Sample dates converted:")
            # if 'transaction_date' 
            # ction_date: {df['transaction_date'].iloc[0] if len(df) > 0 else 'N/A'}")
            # if 'entry_time' in df.columns:
            #     print(f"    entry_time: {df['entry_time'].iloc[0] if len(df) > 0 else 'N/A'}")
            
            # Save normalized file (keep original filename for now)
            normalized_file = normalized_path / csv_file.name
            df.to_csv(normalized_file, index=False)
            
            normalized_files.append(str(normalized_file))
            
            # print(f"  ✓ Saved to: {normalized_file.name}")
            
        except Exception as e:
            # print(f"  ✗ Error normalizing {csv_file.name}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    # print(f"\nNORMALIZATION SUMMARY")
    # print(f"Successfully normalized: {len(normalized_files)}/{len(detected_files)} files")
    
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
        # print("No files to rename")
        return []
    
    interim_path = Path(INTERIM_PATH)
    interim_path.mkdir(parents=True, exist_ok=True)
    
    renamed_files = []
    
    # print("\n=== Renaming Files ===")
    
    for file_path in normalized_files:
        normalized_file = Path(file_path)
        original_name = normalized_file.name
        
        # print(f"\nProcessing: {original_name}")
        
        # Extract date from filename
        date_match = re.search(r'(\d{4})[-_]?(\d{2})', original_name)
        
        if date_match:
            year = date_match.group(1)
            month = date_match.group(2)
            # print(f"  Found numeric date: {year}-{month}")
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
                    # print(f"  Found month name: {month_name} {year} → {year}_{normalized_month}")
                    month = normalized_month
                else:
                    # Use current date if month name not recognized
                    now = datetime.now()
                    year = now.strftime('%Y')
                    month = now.strftime('%B').lower()
                    # print(f"  Unrecognized month '{month_name}', using current: {year}_{month}")
            else:
                # Use current date if no date found in filename
                now = datetime.now()
                year = now.strftime('%Y')
                month = now.strftime('%B').lower()
                # print(f"  No date pattern found, using current: {year}_{month}")
        
        # Create new filename
        new_filename = f'transaction_{year}_{month}.csv'
        new_filepath = interim_path / new_filename
        
        # Move file to interim with new name
        shutil.move(str(normalized_file), str(new_filepath))
        renamed_files.append(str(new_filepath))
        
        # print(f"  ✓ Renamed to: {new_filename}")
    
    # print(f"\RENAME SUMMARY")
    # print(f"Successfully renamed: {len(renamed_files)} file(s)")
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='renamed_files', value=renamed_files)
    
    return renamed_files

def upload_to_gcs(**context):
    """
    Step 4: Upload renamed files to Google Cloud Storage
    """
    from google.cloud import storage
    
    ti = context['ti']
    renamed_files = ti.xcom_pull(key='renamed_files', task_ids='rename_files')
    
    if not renamed_files:
        # print("No files to upload")
        return 0
    
    # Validate environment variables
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set")
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID environment variable is not set")
    
    # print("\nUPLOAD TO GCS")
    
    # Initialize GCS client
    client = storage.Client(project=GCS_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    uploaded_count = 0
    for local_file in renamed_files:
        filename = Path(local_file).name
        blob_name = f"{GCS_PREFIX.rstrip('/')}/{filename}"
        
        # print(f"\nUploading: {filename}")
        
        try:
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_file)
            
            # print(f"  ✓ Uploaded to: gs://{GCS_BUCKET}/{blob_name}")
            uploaded_count += 1
            
        except Exception as e:
            # print(f"  ✗ Error uploading {filename}: {str(e)}")
            continue
    
    # print(f"\nUPLOAD SUMMARY")
    # print(f"Successfully uploaded: {uploaded_count}/{len(renamed_files)} file(s)")
    # print(f"Bucket: gs://{GCS_BUCKET}")
    # print(f"Prefix: {GCS_PREFIX}")
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='uploaded_count', value=uploaded_count)
    
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
        # print("No files to verify")
        return True
    
    # Validate environment variables
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET_NAME environment variable is not set")
    
    if not GCS_PROJECT_ID:
        raise ValueError("GCS_PROJECT_ID environment variable is not set")
    
    # print("\nVERIFYING GCS UPLOADS")
    
    # Initialize GCS client
    client = storage.Client(project=GCS_PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    verification_results = []
    all_verified = True
    
    for local_file in renamed_files:
        filename = Path(local_file).name
        blob_name = f"{GCS_PREFIX.rstrip('/')}/{filename}"
        
        # print(f"\nVerifying: {filename}")
        
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
                
                # print(f"  ✓ VERIFIED")
                # print(f"    Location: gs://{GCS_BUCKET}/{blob_name}")
                # print(f"    Size: {file_size:,} bytes")
                # print(f"    Created: {created_time}")
            else:
                verification_results.append({
                    'filename': filename,
                    'blob_name': blob_name,
                    'exists': False
                })
                # print(f"  ✗ NOT FOUND")
                # print(f"    Expected: gs://{GCS_BUCKET}/{blob_name}")
                all_verified = False
                
        except NotFound:
            verification_results.append({
                'filename': filename,
                'blob_name': blob_name,
                'exists': False,
                'error': 'NotFound'
            })
            # print(f"  ✗ ERROR: Not Found")
            all_verified = False
            
        except Exception as e:
            verification_results.append({
                'filename': filename,
                'blob_name': blob_name,
                'exists': False,
                'error': str(e)
            })
            # print(f"  ✗ ERROR: {str(e)}")
            all_verified = False
    
    # Print summary
    verified_count = sum(1 for r in verification_results if r.get('exists', False))
    total_count = len(verification_results)
    
    # print(f"\nVERIFICATION SUMMARY")
    # print(f"Total files: {total_count}")
    # print(f"Verified: {verified_count}")
    # print(f"Failed: {total_count - verified_count}")
    
    if all_verified:
        # print("\n✓ All files successfully verified in GCS!")
        pass
    else:
        error_msg = f"Upload verification failed: {total_count - verified_count} out of {total_count} files not found in GCS"
        # print(f"\n✗ {error_msg}")
        raise ValueError(error_msg)
    
    # Push results to XCom
    context['ti'].xcom_push(key='verification_results', value=verification_results)
    
    return all_verified


with DAG(
    'gcs_upload_raw_pipeline',
    default_args=default_args,
    description='Detect, normalize, rename, and upload CSV files to GCS',
    schedule_interval='@hourly',
    catchup=False,
    tags=['gcs', 'csv', 'upload', 'ezpass', 'etl'],
) as dag:
    
    # Task 1: Detect CSV files
    detect_task = PythonOperator(
        task_id='detect_files',
        python_callable=detect_files,
        provide_context=True,
    )
    
    # Task 2: Normalize column names
    normalize_task = PythonOperator(
        task_id='normalize_columns',
        python_callable=normalize_columns,
        provide_context=True,
    )
    
    # Task 3: Rename files to standard format
    rename_task = PythonOperator(
        task_id='rename_files',
        python_callable=rename_files,
        provide_context=True,
    )
    
    # Task 4: Upload to GCS
    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        provide_context=True,
    )
    
    # Task 5: Verify upload
    verify_task = PythonOperator(
        task_id='verify_gcs_upload',
        python_callable=verify_gcs_upload,
        provide_context=True,
    )
    
    # Execution order: Linear pipeline
    detect_task >> normalize_task >> rename_task >> upload_task >> verify_task