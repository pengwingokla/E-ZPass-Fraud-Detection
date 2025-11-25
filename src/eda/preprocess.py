import os
import logging
import re
from typing import Optional, Union, Dict, List, Tuple
from pathlib import Path
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env file
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
        'folder_prefix_raw': os.getenv("GCS_FOLDER_PREFIX_RAW", "data/raw/"),
        'default_content_type': os.getenv("GCS_DEFAULT_CONTENT_TYPE", "text/csv")
    }
    
    # Validate required variables
    if not config['bucket_name']:
        raise ValueError("GCS_BUCKET_NAME environment variable is required")
    
    if not config['project_id']:
        raise ValueError("GCS_PROJECT_ID environment variable is required")
    
    return config


class GCSDownloader:
    """
    A class to handle downloading CSV files from Google Cloud Storage.
    """
    
    def __init__(self, 
                 bucket_name: str,
                 credentials_path: Optional[str] = None,
                 project_id: Optional[str] = None):
        """
        Initialize the GCS downloader.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            credentials_path (str, optional): Path to service account JSON file
            project_id (str, optional): Google Cloud project ID
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        
        # Initialize GCS client
        if credentials_path and os.path.exists(credentials_path):
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.client = storage.Client(credentials=credentials, project=project_id)
            logger.info(f"Initialized GCS client with service account: {credentials_path}")
        else:
            # Use default credentials (e.g., from environment or gcloud auth)
            self.client = storage.Client(project=project_id)
            logger.info("Initialized GCS client with default credentials")
    
    def list_files(self, prefix: str = "") -> List[str]:
        """
        List files in the GCS bucket.
        
        Args:
            prefix (str): Optional prefix to filter files
            
        Returns:
            List[str]: List of blob names
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list files: {str(e)}")
            return []
    
    def download_files(self,
                       blob_names: Union[str, List[str]],
                       local_path: Optional[Union[str, Path]] = None,
                       return_dataframes: bool = False) -> Union[bool, Optional[pd.DataFrame], Dict[str, Union[bool, Optional[pd.DataFrame]]]]:
        """
        Download CSV files from Google Cloud Storage.
        
        This function can handle multiple use cases:
        - Single file to local path: pass blob_names as string, local_path as file path
        - Single file as DataFrame: pass blob_names as string, return_dataframes=True
        - Multiple files to directory: pass blob_names as list, local_path as directory
        - Multiple files as DataFrames: pass blob_names as list, return_dataframes=True
        
        Args:
            blob_names (str or List[str]): Single blob name or list of blob names
            local_path (str or Path, optional): Local path to save files (file path for single file, directory for multiple)
            return_dataframes (bool): If True, return DataFrames instead of saving to files
            
        Returns:
            For single file:
                - bool: Success status if saving to file
                - pd.DataFrame or None: DataFrame if return_dataframes=True
            For multiple files:
                - Dict[str, bool]: Success status for each file if saving to files
                - Dict[str, Optional[pd.DataFrame]]: DataFrames for each file if return_dataframes=True
        """
        # Normalize input to list
        if isinstance(blob_names, str):
            blob_names = [blob_names]
            single_file = True
        else:
            single_file = False
        
        results = {}
        
        for blob_name in blob_names:
            try:
                bucket = self.client.bucket(self.bucket_name)
                blob = bucket.blob(blob_name)
                
                if return_dataframes:
                    # Download as DataFrame
                    content = blob.download_as_text()
                    from io import StringIO
                    df = pd.read_csv(StringIO(content))
                    logger.info(f"Successfully loaded {blob_name} as DataFrame with {len(df)} rows")
                    results[blob_name] = df
                else:
                    # Download to file
                    if single_file:
                        # Single file - use provided path
                        local_file_path = Path(local_path)
                    else:
                        # Multiple files - create path in directory
                        local_dir = Path(local_path)
                        local_dir.mkdir(parents=True, exist_ok=True)
                        filename = Path(blob_name).name
                        local_file_path = local_dir / filename
                    
                    local_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    logger.info(f"Downloading gs://{self.bucket_name}/{blob_name} to {local_file_path}")
                    blob.download_to_filename(str(local_file_path))
                    
                    logger.info(f"Successfully downloaded {blob_name}")
                    results[blob_name] = True
                    
            except Exception as e:
                logger.error(f"Failed to download {blob_name}: {str(e)}")
                results[blob_name] = None if return_dataframes else False
        
        # Return appropriate format based on input
        if single_file:
            return results[blob_names[0]]
        else:
            return results


class ColumnNormalizer:
    """
    A class to normalize column names for E-ZPass transaction data.
    """
    
    def __init__(self):
        """
        Initialize the column normalizer with predefined mappings.
        """
        # Define column name mappings for different data sources
        self.column_mappings = {
            # These are the original uppercase column names from the raw data after cleaning:
            'posting_date': 'posting_date',
            'transaction_date': 'transaction_date',
            # TAG/PLATE NUMBER -> tagplate_number
            'tagplate_number': 'tag_plate_number',  
            'agency': 'agency',
            'description': 'description',
            'entry_time': 'entry_time',
            'entry_plaza': 'entry_plaza',
            'entry_lane': 'entry_lane',
            'exit_time': 'exit_time',
            'exit_plaza': 'exit_plaza',
            'exit_lane': 'exit_lane',
            'vehicle_type_code': 'vehicle_type_code',
            'amount': 'amount',
            'prepaid': 'prepaid',
            # PLAN/RATE -> planrate
            'planrate': 'plan_rate', 
            'fare_type': 'fare_type',
            'balance': 'balance',
        }
    
    def normalize_column_name(self, column_name: str) -> str:
        """
        Normalize a single column name.
        """
        # Clean the column name
        cleaned = column_name.strip().lower()
        
        # Remove extra spaces and replace with underscores
        cleaned = re.sub(r'\s+', '_', cleaned)
        
        # Remove special characters except underscores
        cleaned = re.sub(r'[^\w_]', '', cleaned)
        
        # Map to standard column name
        return self.column_mappings.get(cleaned, cleaned)
    
    def normalize_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all column names in a DataFrame.
        """
        # Create mapping of old to new column names
        column_mapping = {}
        
        for col in df.columns:
            normalized_col = self.normalize_column_name(col)
            column_mapping[col] = normalized_col
        
        # Rename columns
        df_normalized = df.rename(columns=column_mapping)
        
        # Log the changes
        # changes = {old: new for old, new in column_mapping.items() if old != new}
        # if changes:
        #     logger.info(f"Column name changes: {changes}")
        
        return df_normalized
    
    def validate_required_columns(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that required columns are present in the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            
        Returns:
            Tuple[bool, List[str]]: (is_valid, missing_columns)
        """
        required_columns = [
            'posting_date', 'transaction_date', 'tag_plate_number', 
            'agency', 'amount'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns: {missing_columns}")
            return False, missing_columns
        
        return True, []


class DataRetriever:
    """
    Main class to retrieve and normalize E-ZPass transaction data from GCS.
    """
    
    def __init__(self, 
                 bucket_name: str,
                 credentials_path: Optional[str] = None,
                 project_id: Optional[str] = None):
        """
        Initialize the data retriever.
        
        Args:
            bucket_name (str): Name of the GCS bucket
            credentials_path (str, optional): Path to service account JSON file
            project_id (str, optional): Google Cloud project ID
        """
        self.downloader = GCSDownloader(
            bucket_name=bucket_name,
            credentials_path=credentials_path,
            project_id=project_id
        )
        self.normalizer = ColumnNormalizer()
    
    def retrieve_files(self, 
                       blob_names: Optional[List[str]] = None,
                       folder_prefix: Optional[str] = None,
                       local_directory: Optional[Union[str, Path]] = None) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Retrieve CSV files from GCS and normalize their columns.
        
        This function can handle multiple use cases:
        - Single file: pass blob_names as a list with one item
        - Multiple files: pass blob_names as a list with multiple items
        - All files in folder: pass folder_prefix to retrieve all CSV files in that folder
        
        Args:
            blob_names (List[str], optional): List of specific blob names to retrieve
            folder_prefix (str, optional): Prefix for folder in GCS to retrieve all CSV files from
            local_directory (str or Path, optional): Local directory to save files
            
        Returns:
            Dict[str, Optional[pd.DataFrame]]: Dictionary with blob names as keys and DataFrames as values
            
        Raises:
            ValueError: If neither blob_names nor folder_prefix is provided
        """
        # Determine which files to retrieve
        if blob_names is not None:
            files_to_process = blob_names
            logger.info(f"Processing {len(files_to_process)} specified files")
        elif folder_prefix is not None:
            # List all files in the specified folder
            all_files = self.downloader.list_files(prefix=folder_prefix)
            files_to_process = [f for f in all_files if f.endswith('.csv')]
            
            if not files_to_process:
                logger.warning(f"No CSV files found in folder: {folder_prefix}")
                return {}
            
            logger.info(f"Found {len(files_to_process)} CSV files in folder: {folder_prefix}")
        else:
            raise ValueError("Either blob_names or folder_prefix must be provided")
        
        # Process each file
        results = {}
        for blob_name in files_to_process:
            logger.info(f"Processing {blob_name}...")
            
            # Download the file as DataFrame
            df = self.downloader.download_files(blob_name, return_dataframes=True)
            
            if df is None:
                logger.error(f"Failed to download {blob_name}")
                results[blob_name] = None
                continue
            
            # Normalize column names
            df_normalized = self.normalizer.normalize_dataframe_columns(df)
            
            # Validate required columns
            is_valid, missing_cols = self.normalizer.validate_required_columns(df_normalized)
            
            if not is_valid:
                logger.error(f"File {blob_name} missing required columns: {missing_cols}")
                results[blob_name] = None
                continue
            
            logger.info(f"Successfully retrieved and normalized {blob_name}")
            results[blob_name] = df_normalized
        
        # Log summary
        successful = sum(1 for df in results.values() if df is not None)
        logger.info(f"Successfully processed {successful}/{len(files_to_process)} files")
        
        return results


def download_and_normalize_data(bucket_name: str,
                              folder_prefix: str = "data/raw/",
                              credentials_path: Optional[str] = None,
                              project_id: Optional[str] = None,
                              local_directory: Union[str, Path] = None) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Convenience function to download and normalize E-ZPass transaction data.
    
    The normalization is done in the DataRetriever.retrieve_files() method, which:
    1. Downloads each CSV file as a DataFrame
    2. Uses ColumnNormalizer to standardize column names
    3. Validates that required columns are present
    
    Args:
        bucket_name (str): Name of the GCS bucket
        folder_prefix (str): Prefix for the folder in GCS
        credentials_path (str, optional): Path to service account JSON file
        project_id (str, optional): Google Cloud project ID
        local_directory (str or Path): Local directory to save files (optional)
        
    Returns:
        Dict[str, Optional[pd.DataFrame]]: Dictionary with blob names as keys and normalized DataFrames as values
    """
    retriever = DataRetriever(
        bucket_name=bucket_name,
        credentials_path=credentials_path,
        project_id=project_id
    )
    
    return retriever.retrieve_files(
        folder_prefix=folder_prefix,
        local_directory=local_directory
    )


# Example usage
if __name__ == "__main__":
    try:
        # Load GCS configuration
        config = get_gcs_config()
        
        # Initialize data retriever
        retriever = DataRetriever(
            bucket_name=config['bucket_name'],
            credentials_path=config['credentials_path'],
            project_id=config['project_id']
        )
        
        # Set up local directory for saving files
        script_dir = Path(__file__).parent
        local_data_dir = script_dir / "../../data/interim"
        
        print("Retrieving and normalizing raw transaction data...")
        
        # Retrieve all raw files
        results = retriever.retrieve_files(
            folder_prefix=config['folder_prefix_raw'],
            local_directory=local_data_dir
        )
        
        # Display results
        print(f"\nProcessed {len(results)} files:")
        for blob_name, df in results.items():
            if df is not None:
                print(f"  ✓ {blob_name}: {len(df)} rows, {len(df.columns)} columns")
                # print(f"    Columns: {list(df.columns)}")
            else:
                print(f"  ✗ {blob_name}: Failed to process")
        
        # Save processed data to processed folder
        processed_dir = script_dir / "../../data/processed"
        processed_dir.mkdir(exist_ok=True)
        
        print(f"\nSaving processed data to {processed_dir}...")
        for blob_name, df in results.items():
            if df is not None:
                filename = Path(blob_name).name
                output_path = processed_dir / f"processed_{filename}"
                df.to_csv(output_path, index=False)
                print(f"  Saved: {output_path}")
        
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        print(f"Please check your .env file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"An error occurred: {e}")
