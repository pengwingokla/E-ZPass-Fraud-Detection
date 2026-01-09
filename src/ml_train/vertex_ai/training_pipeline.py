from google.cloud import aiplatform
from google.cloud import bigquery
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import joblib
import os
from datetime import datetime
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import sys

# Add parent directory to path to import mlflow_config
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from mlflow_config import setup_mlflow_tracking, log_model_to_mlflow
except ImportError:
    # Fallback if mlflow_config not found
    setup_mlflow_tracking = None
    log_model_to_mlflow = None

class FraudDetectionTrainer:
    def __init__(self, project_id, location, bq_table, use_mlflow=True, mlflow_experiment_name="ezpass-fraud-detection"):
        self.project_id = project_id
        self.location = location
        self.bq_table = bq_table
        self.use_mlflow = use_mlflow
        self.mlflow_experiment_name = mlflow_experiment_name
        
        # Initialize Vertex AI
        aiplatform.init(
            project=project_id,
            location=location
        )
        
        # Set up MLflow tracking if enabled
        if self.use_mlflow:
            if setup_mlflow_tracking is None:
                print("⚠ MLflow config module not found, disabling MLflow")
                self.use_mlflow = False
            else:
                gcs_bucket = os.getenv("GCS_BUCKET_NAME")
                tracking_uri = os.getenv("MLFLOW_TRACKING_URI")  # Optional: set to MLflow server URI
                self.mlflow_client, self.mlflow_experiment_id = setup_mlflow_tracking(
                    tracking_uri=tracking_uri,
                    experiment_name=mlflow_experiment_name,
                    gcs_bucket=gcs_bucket,
                    project_id=project_id
                )
                print(f"✓ MLflow tracking initialized (experiment: {mlflow_experiment_name})")
        
    def load_features_from_bigquery(self):
        client = bigquery.Client(project=self.project_id)
        
        query = f"""
        SELECT 
            *
        FROM 
            `{self.bq_table}`
        
        """
        # LIMIT 1000000
        
        print("Loading features from BigQuery...")
        df = client.query(query).to_dataframe()
        print(f"Loaded {len(df)} rows from BigQuery")
        
        return df
    
    def preprocess_features(self, df):
        # Identify columns to exclude from training (IDs and flags)
        exclude_cols = []
        if 'transaction_id' in df.columns:
            exclude_cols.append('transaction_id')
        if 'tag_plate_number' in df.columns:
            exclude_cols.append('tag_plate_number')
        if 'last_updated' in df.columns:
            exclude_cols.append('last_updated')
        if 'source_file' in df.columns:
            exclude_cols.append('source_file')
        
        # Exclude flag columns from training
        flag_cols = [col for col in df.columns if col.startswith('flag_')]
        exclude_cols.extend(flag_cols)
        
        # Get feature columns (everything except excluded columns)
        feature_cols = [col for col in df.columns if col not in exclude_cols]

        print(f"Excluded columns (IDs + Flags): {exclude_cols}")
        print(f"Training feature columns: {feature_cols}")

        # Extract features
        X = df[feature_cols].copy()

        # Select only numeric columns before any operations
        numeric_cols = X.select_dtypes(include=['int64', 'float64', 'int32', 'float32']).columns
        X_numeric = X[numeric_cols]

        # Handle missing values - use median for numeric columns
        if len(numeric_cols) == 0:
            raise ValueError("No numeric columns found in the dataset")
        
        X_numeric = X_numeric.fillna(X_numeric.median())
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_numeric)

        # Create IDs dataframe
        id_cols = ['transaction_id', 'tag_plate_number']
        if all(col in df.columns for col in id_cols):
            df_ids = df[id_cols].copy()
        else:
            # If no ID columns, create a simple index
            df_ids = pd.DataFrame({
                'transaction_id': [f"txn_{i}" for i in range(len(df))],
                'tag_plate_number': ['unknown'] * len(df),
            })
        
        print(f"DF IDs: {df_ids.head()}")
        
        # Return the original dataframe as well for predictions
        return X_scaled, scaler, feature_cols, df_ids, df
    
    def train_isolation_forest(self, X_scaled, contamination=0.01, n_estimators=100, max_samples='auto', n_jobs=2):
        """Train Isolation Forest for anomaly detection"""
        import time
        import numpy as np
        
        print("Training Isolation Forest...")
        start_time = time.time()
        
        # Model parameters
        model_params = {
            'contamination': contamination,
            'random_state': 42,
            'n_estimators': n_estimators,
            'max_samples': max_samples,
            'n_jobs': n_jobs
        }
        
        # Log parameters to MLflow if enabled (run should already be active from run_training_pipeline)
        if self.use_mlflow:
            # Don't start a new run here - it should already be active from run_training_pipeline()
            # Just verify we have an active run, and if not, start one (shouldn't happen normally)
            try:
                if mlflow.active_run() is None:
                    # This shouldn't happen, but handle it gracefully
                    print("⚠ Warning: No active MLflow run found, starting one...")
                    mlflow.start_run()
            except Exception:
                # If we can't check or there's an issue, just continue - the run should exist
                pass
            # Log parameters
            mlflow.log_params({
                'model_type': 'isolation_forest',
                'contamination': contamination,
                'n_estimators': n_estimators,
                'max_samples': str(max_samples),
                'n_jobs': n_jobs,
                'random_state': 42
            })
            mlflow.set_tag('project', 'ezpass-fraud-detection')
            mlflow.set_tag('model_type', 'isolation_forest')
        
        model = IsolationForest(**model_params)
        
        model.fit(X_scaled)
        training_time = time.time() - start_time
        
        # Get anomaly scores from Isolation Forest
        # Isolation Forest returns:
        # - Negative scores (< 0): Anomalies
        # - Positive scores (> 0): Normal transactions
        # We keep the original scores (no negation)
        anomaly_scores = model.decision_function(X_scaled)
        
        predictions = model.predict(X_scaled)  
        
        # Calculate metrics
        n_anomalies = sum(predictions == -1)
        anomaly_rate = n_anomalies / len(predictions)
        score_percentiles = np.percentile(anomaly_scores, [1, 5, 25, 50, 75, 95, 99])
        
        metrics = {
            'training_time_seconds': training_time,
            'n_samples': len(X_scaled),
            'n_features': X_scaled.shape[1],
            'n_estimators': model.n_estimators,
            'contamination': contamination,
            'n_anomalies': int(n_anomalies),
            'anomaly_rate': float(anomaly_rate),
            'score_mean': float(anomaly_scores.mean()),
            'score_std': float(anomaly_scores.std()),
            'score_min': float(anomaly_scores.min()),
            'score_max': float(anomaly_scores.max()),
            'score_median': float(score_percentiles[3]),
        }
        
        # Log metrics to MLflow
        if self.use_mlflow:
            mlflow.log_metrics(metrics)
            mlflow.set_tag('run_type', 'training')
            mlflow.set_tag('dataset', self.bq_table)
        
        print(f"✓ Training completed in {training_time:.2f}s")
        print(f"  Samples: {len(X_scaled):,}")
        print(f"  Anomalies: {n_anomalies:,} ({anomaly_rate*100:.2f}%)")
        print(f"  Score range: [{anomaly_scores.min():.3f}, {anomaly_scores.max():.3f}]")
        print(f"  Score interpretation: Values lower than 0 = More anomalous")
        
        return model, anomaly_scores, predictions, metrics
    
    def train_dbscan(self, X_scaled, eps=0.5, min_samples=5):
        """Train DBSCAN for clustering-based anomaly detection"""
        pass
    
    def save_model_artifacts(self, model, scaler, feature_cols, model_type):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        artifacts_dir = f"./artifacts/{model_type}_{timestamp}"
        os.makedirs(artifacts_dir, exist_ok=True)
        
        # Save model
        joblib.dump(model, f"{artifacts_dir}/model.joblib")
        
        # Save scaler
        joblib.dump(scaler, f"{artifacts_dir}/scaler.joblib")
        
        # Save feature columns
        joblib.dump(feature_cols, f"{artifacts_dir}/feature_cols.joblib")
        
        print(f"Model artifacts saved to {artifacts_dir}")
        return artifacts_dir
    
    def upload_to_vertex_ai(self, artifacts_dir, model_type):
        display_name = f"ezpass-fraud-{model_type}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        model = aiplatform.Model.upload(
            display_name=display_name,
            artifact_uri=artifacts_dir,
            serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"
        )
        
        print(f"Model uploaded to Vertex AI: {model.resource_name}")
        return model
        
    def write_predictions_to_bigquery(self, df_original, predictions, anomaly_scores, output_table):
        """Write predictions back to BigQuery with all features"""
        from google.cloud import bigquery
        from datetime import datetime
        import pandas as pd
        
        # Start with original data that has all features
        results_df = df_original.copy()
        
        # Add prediction columns
        results_df['is_anomaly'] = (predictions == -1).astype(int)
        results_df['ml_anomaly_score'] = anomaly_scores.astype(float)
        results_df['prediction_timestamp'] = datetime.now()
        
        # Define expected columns based on BigQuery schema (order doesn't matter)
        # NOTE: flag_rush_hour and flag_is_weekend are excluded from training and predictions
        expected_cols = [
            'transaction_id',
            # Financial
            'amount',
            # Velocity & Travel Features
            'distance_miles', 'travel_time_minutes', 'speed_mph',
            'overlapping_journey_duration_minutes',
            # Driver Rolling Stats
            'driver_amount_last_30txn_avg', 'driver_amount_last_30txn_std',
            'driver_amount_last_30txn_count', 'driver_daily_txn_count',
            # Gold Layer: Driver Features
            'driver_amount_modified_z_score', 'amount_deviation_from_avg_pct',
            'amount_deviation_from_median_pct', 'driver_today_spend', 'driver_avg_daily_spend_30d',
            # Gold Layer: Route Features
            'route_amount_z_score',
            # Time & Context
            'exit_hour',
            # Vehicle
            'vehicle_type_code',
            # Encoded Categorical Features
            'route_name_freq_encoded', 'entry_plaza_freq_encoded',
            'exit_plaza_freq_encoded', 'vehicle_type_freq_encoded', 'agency_freq_encoded',
            'travel_time_of_day_freq_encoded',
            # ML Prediction Columns
            'is_anomaly', 'ml_anomaly_score', 'prediction_timestamp'
        ]
        
        # Only keep columns that exist in both the dataframe and expected schema
        available_cols = [col for col in expected_cols if col in results_df.columns]
        final_df = results_df[available_cols].copy()
        
        print(f"Predictions dataframe (before upload):")
        print(f"  Shape: {final_df.shape}")
        print(f"  Columns: {final_df.columns.tolist()}")
        print(f"  Expected columns: {len(expected_cols)}, Available: {len(available_cols)}")
        if len(available_cols) < len(expected_cols):
            missing = set(expected_cols) - set(available_cols)
            print(f"  WARNING: Missing columns: {missing}")
        print(f"  Sample:\n{final_df.head(3)}")
        
        # Write to BigQuery
        from google.api_core import exceptions
        
        client = bigquery.Client(project=self.project_id)
        
        try:
            # Check if table exists first
            try:
                client.get_table(output_table)
            except Exception:
                # Table doesn't exist - try to create it
                print(f"⚠ Table {output_table} does not exist, attempting to create...")
                # Define schema based on expected columns
                schema = []
                for col in expected_cols:
                    if col == 'transaction_id':
                        schema.append(bigquery.SchemaField(col, "STRING", mode="NULLABLE"))
                    elif col in ['is_anomaly']:
                        schema.append(bigquery.SchemaField(col, "INTEGER", mode="REQUIRED"))
                    elif col in ['ml_anomaly_score', 'prediction_timestamp']:
                        if col == 'prediction_timestamp':
                            schema.append(bigquery.SchemaField(col, "TIMESTAMP", mode="REQUIRED"))
                        else:
                            schema.append(bigquery.SchemaField(col, "FLOAT64", mode="REQUIRED"))
                    elif col in ['exit_hour', 'driver_amount_last_30txn_count', 'driver_daily_txn_count',
                                 'route_name_freq_encoded', 'entry_plaza_freq_encoded', 'exit_plaza_freq_encoded',
                                 'vehicle_type_freq_encoded', 'agency_freq_encoded', 'travel_time_of_day_freq_encoded']:
                        schema.append(bigquery.SchemaField(col, "INTEGER", mode="NULLABLE"))
                    elif col == 'vehicle_type_code':
                        schema.append(bigquery.SchemaField(col, "STRING", mode="NULLABLE"))
                    else:
                        schema.append(bigquery.SchemaField(col, "FLOAT64", mode="NULLABLE"))
                
                table = bigquery.Table(output_table, schema=schema)
                table.description = "ML fraud predictions for E-ZPass transactions"
                
                # Add time partitioning
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="prediction_timestamp"
                )
                
                try:
                    client.create_table(table)
                    print(f"✓ Created predictions table: {output_table}")
                except exceptions.Forbidden as e:
                    print(f"⚠ Permission denied: Cannot create table {output_table}")
                    print(f"⚠ Error: {e}")
                    print(f"⚠ Skipping predictions write - table must be created manually or permissions granted")
                    print(f"⚠ Training completed successfully, but predictions were not saved to BigQuery")
                    print(f"  Total rows processed: {len(final_df):,}")
                    print(f"  Anomalies detected: {sum(final_df['is_anomaly']):,}")
                    print(f"  Anomaly rate: {sum(final_df['is_anomaly']) / len(final_df) * 100:.2f}%")
                    return
            
            # Table exists, try to append data
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_NEVER"  # Don't try to create if missing
            )
            
            job = client.load_table_from_dataframe(
                final_df, output_table, job_config=job_config
            )
            job.result()
            
            print(f"✓ Predictions written to {output_table}")
            print(f"  Total rows: {len(final_df):,}")
            print(f"  Anomalies detected: {sum(final_df['is_anomaly']):,}")
            print(f"  Anomaly rate: {sum(final_df['is_anomaly']) / len(final_df) * 100:.2f}%")
            
        except exceptions.Forbidden as e:
            print(f"⚠ Permission denied: Cannot write to table {output_table}")
            print(f"⚠ Error: {e}")
            print(f"⚠ Skipping predictions write - ensure service account has BigQuery Data Editor role")
            print(f"⚠ Training completed successfully, but predictions were not saved to BigQuery")
            print(f"  Total rows processed: {len(final_df):,}")
            print(f"  Anomalies detected: {sum(final_df['is_anomaly']):,}")
            print(f"  Anomaly rate: {sum(final_df['is_anomaly']) / len(final_df) * 100:.2f}%")
        except Exception as e:
            print(f"⚠ Error writing predictions to BigQuery: {e}")
            print(f"⚠ Skipping predictions write - training will continue")
            print(f"  Total rows processed: {len(final_df):,}")
            print(f"  Anomalies detected: {sum(final_df['is_anomaly']):,}")
            print(f"  Anomaly rate: {sum(final_df['is_anomaly']) / len(final_df) * 100:.2f}%")
    
    def log_training_metrics(self, metrics, model_type="isolation_forest"):
        """Save training metrics to BigQuery"""
        from google.cloud import bigquery
        from google.api_core import exceptions
        from datetime import datetime
        import pandas as pd
        
        client = bigquery.Client(project=self.project_id)
        table_id = f"{self.project_id}.ezpass_data.model_training_metrics"
        
        model_id = f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        metrics_df = pd.DataFrame([{
            'model_id': model_id,
            'model_type': model_type,
            'training_timestamp': datetime.now(),
            **metrics
        }])
        
        try:
            # Check if table exists first
            try:
                client.get_table(table_id)
            except Exception:
                # Table doesn't exist - try to create it
                print(f"⚠ Table {table_id} does not exist, attempting to create...")
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
                table = bigquery.Table(table_id, schema=schema)
                table.description = "ML model training metrics over time"
                try:
                    client.create_table(table)
                    print(f"✓ Created metrics table: {table_id}")
                except exceptions.Forbidden as e:
                    print(f"⚠ Permission denied: Cannot create table {table_id}")
                    print(f"⚠ Skipping metrics logging - table must be created manually or permissions granted")
                    return
            
            # Table exists, try to append data
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_NEVER"  # Don't try to create if missing
            )
            
            client.load_table_from_dataframe(
                metrics_df, 
                table_id,
                job_config=job_config
            ).result()
            
            print(f"✓ Metrics logged to {table_id}")
            
        except exceptions.Forbidden as e:
            print(f"⚠ Permission denied: Cannot write to table {table_id}")
            print(f"⚠ Error: {e}")
            print(f"⚠ Skipping metrics logging - ensure service account has BigQuery Data Editor role")
        except Exception as e:
            print(f"⚠ Error logging metrics to BigQuery: {e}")
            print(f"⚠ Skipping metrics logging - training will continue")

    def run_training_pipeline(self):
        """Execute full training pipeline"""
        print("=" * 60)
        print("STARTING ML TRAINING PIPELINE")
        print("=" * 60)

        # Start MLflow run at the beginning if enabled
        if self.use_mlflow:
            # End any existing active run first (use try/except for safety)
            try:
                active_run = mlflow.active_run()
                if active_run is not None:
                    mlflow.end_run()
            except Exception:
                # If there's an error checking, try to end anyway
                try:
                    mlflow.end_run()
                except Exception:
                    pass
            # Start a fresh run
            mlflow.start_run()

        try:
            # 1. Load features from BigQuery
            print("\n[1/7] Loading features from BigQuery")
            df = self.load_features_from_bigquery()
            
            if self.use_mlflow:
                mlflow.log_param('dataset_size', len(df))
                mlflow.log_param('source_table', self.bq_table)
            
            # 2. Preprocess
            print("\n[2/7] Preprocessing features")
            X_scaled, scaler, feature_cols, df_ids, df_original = self.preprocess_features(df)
            
            if self.use_mlflow:
                mlflow.log_param('n_features', X_scaled.shape[1])
                mlflow.log_param('feature_columns', ','.join(feature_cols))
            
            # 3. Train Isolation Forest
            print("\n[3/7] Training Isolation Forest")
            isoforest_model, anomaly_scores, predictions, metrics = self.train_isolation_forest(X_scaled)
            
            # 4. Log model to MLflow
            if self.use_mlflow:
                print("\n[4/7] Logging model to MLflow")
                model_uri = mlflow.sklearn.log_model(
                    isoforest_model,
                    artifact_path="model",
                    registered_model_name="ezpass-fraud-isolation-forest"
                )
                print(f"✓ Model logged to MLflow: {model_uri}")
                
                # Log scaler and feature columns as additional artifacts
                import tempfile
                import json
                with tempfile.TemporaryDirectory() as tmpdir:
                    # Save scaler
                    scaler_path = os.path.join(tmpdir, "scaler.joblib")
                    joblib.dump(scaler, scaler_path)
                    mlflow.log_artifact(scaler_path, artifact_path="preprocessing")
                    
                    # Save feature columns
                    feature_path = os.path.join(tmpdir, "feature_columns.json")
                    with open(feature_path, "w") as f:
                        json.dump(feature_cols, f)
                    mlflow.log_artifact(feature_path, artifact_path="preprocessing")
            
            # 5. Log metrics to BigQuery (keep existing functionality)
            print("\n[5/7] Logging training metrics to BigQuery")
            self.log_training_metrics(metrics, "isolation_forest")
            
            # 6. Save artifacts (for backward compatibility)
            print("\n[6/7] Saving model artifacts locally")
            artifacts_dir = self.save_model_artifacts(
                isoforest_model, scaler, feature_cols, "isolation_forest"
            )
            
            if self.use_mlflow:
                mlflow.log_param('local_artifacts_dir', artifacts_dir)
            
            # 7. Write predictions to BigQuery
            print("\n[7/7] Writing predictions to BigQuery")
            output_table = f"{self.project_id}.ezpass_data.fraud_predictions"
            self.write_predictions_to_bigquery(df_original, predictions, anomaly_scores, output_table)
            
            if self.use_mlflow:
                mlflow.log_param('predictions_table', output_table)
                mlflow.set_tag('status', 'completed')
                run_id = mlflow.active_run().info.run_id
                print(f"✓ MLflow run ID: {run_id}")
                print(f"  View run: mlflow ui (then navigate to run {run_id})")
            
            # Optional: Upload to Vertex AI
            # vertex_model = self.upload_to_vertex_ai(artifacts_dir, "isolation_forest")
            
            print("\n" + "=" * 60)
            print("✓ ML TRAINING PIPELINE COMPLETED SUCCESSFULLY")
            print("=" * 60)
            
            return isoforest_model, anomaly_scores, metrics
            
        except Exception as e:
            if self.use_mlflow and mlflow.active_run():
                mlflow.set_tag('status', 'failed')
                mlflow.log_param('error', str(e))
            raise
        finally:
            if self.use_mlflow and mlflow.active_run():
                mlflow.end_run()

if __name__ == "__main__":
    trainer = FraudDetectionTrainer(
        project_id=os.environ.get("GCP_PROJECT_ID"),
        location="us-central1",
        bq_table=f"{os.environ.get('GCP_PROJECT_ID')}.ezpass_data.gold_train"
    )
    
    model, scores = trainer.run_training_pipeline()