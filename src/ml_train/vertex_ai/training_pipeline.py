from google.cloud import aiplatform
from google.cloud import bigquery
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import joblib
import os
from datetime import datetime

class FraudDetectionTrainer:
    def __init__(self, project_id, location, bq_table):
        self.project_id = project_id
        self.location = location
        self.bq_table = bq_table
        
        # Initialize Vertex AI
        aiplatform.init(
            project=project_id,
            location=location
        )
        
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
    
    def train_isolation_forest(self, X_scaled, contamination=0.01):
        """Train Isolation Forest for anomaly detection"""
        import time
        import numpy as np
        
        print("Training Isolation Forest...")
        start_time = time.time()
        
        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100,
            max_samples='auto',
            n_jobs=2
        )
        
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
        client = bigquery.Client(project=self.project_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
        
        job = client.load_table_from_dataframe(
            final_df, output_table, job_config=job_config
        )
        job.result()
        
        print(f"✓ Predictions written to {output_table}")
        print(f"  Total rows: {len(final_df):,}")
        print(f"  Anomalies detected: {sum(final_df['is_anomaly']):,}")
        print(f"  Anomaly rate: {sum(final_df['is_anomaly']) / len(final_df) * 100:.2f}%")
    
    def log_training_metrics(self, metrics, model_type="isolation_forest"):
        """Save training metrics to BigQuery"""
        from google.cloud import bigquery
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
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        client.load_table_from_dataframe(
            metrics_df, 
            table_id,
            job_config=job_config
        ).result()
        
        print(f"✓ Metrics logged to {table_id}")

    def run_training_pipeline(self):
        """Execute full training pipeline"""
        print("=" * 60)
        print("STARTING ML TRAINING PIPELINE")
        print("=" * 60)

        # 1. Load features from BigQuery
        print("\n[1/6] Loading features from BigQuery")
        df = self.load_features_from_bigquery()
        
        # 2. Preprocess
        print("\n[2/6] Preprocessing features")
        X_scaled, scaler, feature_cols, df_ids, df_original = self.preprocess_features(df)
        
        # 3. Train Isolation Forest
        print("\n[3/6] Training Isolation Forest")
        isoforest_model, anomaly_scores, predictions, metrics = self.train_isolation_forest(X_scaled)
        
        # 4. Log metrics to BigQuery
        print("\n[4/6] Logging training metrics")
        self.log_training_metrics(metrics, "isolation_forest")
        
        # 5. Save artifacts
        print("\n[5/6] Saving model artifacts")
        artifacts_dir = self.save_model_artifacts(
            isoforest_model, scaler, feature_cols, "isolation_forest"
        )
        
        # 6. Write predictions to BigQuery
        print("\n[6/6] Writing predictions to BigQuery")
        output_table = f"{self.project_id}.ezpass_data.fraud_predictions"
        self.write_predictions_to_bigquery(df_original, predictions, anomaly_scores, output_table)
        
        # Optional: Upload to Vertex AI
        # vertex_model = self.upload_to_vertex_ai(artifacts_dir, "isolation_forest")
        
        print("\n" + "=" * 60)
        print("✓ ML TRAINING PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
        return isoforest_model, anomaly_scores, metrics

if __name__ == "__main__":
    trainer = FraudDetectionTrainer(
        project_id=os.environ.get("GCP_PROJECT_ID"),
        location="us-central1",
        bq_table=f"{os.environ.get('GCP_PROJECT_ID')}.ezpass_data.gold_train"
    )
    
    model, scores = trainer.run_training_pipeline()