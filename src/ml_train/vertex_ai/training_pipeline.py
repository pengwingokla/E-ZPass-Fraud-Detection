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
        LIMIT 10000
        """
        
        print("Loading features from BigQuery...")
        df = client.query(query).to_dataframe()
        print(f"Loaded {len(df)} rows from BigQuery")
        
        return df
    
    def preprocess_features(self, df):
        # Identify ID columns (non-features)
        id_cols = []
        if 'transaction_id' in df.columns:
            id_cols.append('transaction_id')
        if 'tag_plate_number' in df.columns:
            id_cols.append('tag_plate_number')
        if 'last_updated' in df.columns:
            id_cols.append('last_updated')
        if 'source_file' in df.columns:
            id_cols.append('source_file')
        feature_cols = [col for col in df.columns if col not in id_cols]

        flag_cols = [col for col in df.columns if col.startswith('flag_')]

        # Get all columns except IDs
        nonid_feature_cols = [col for col in df.columns if col not in id_cols]

        print(f"Flag columns: {flag_cols}")
        print(f"ID columns: {id_cols}")
        print(f"Non-ID feature columns: {nonid_feature_cols}")

        # Extract features
        X = df[nonid_feature_cols].copy()

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

        # Return IDs
        if id_cols:
            df_ids = df[id_cols].copy()
            
        else:
            # If no ID columns, create a simple index
            df_ids = pd.DataFrame({
                'transaction_id': [f"txn_{i}" for i in range(len(df))],
                'tag_plate_number': ['unknown'] * len(df),
            })

        if flag_cols:
            for flag_col in flag_cols:
                df_ids[flag_col] = df[flag_col].values
        
        print(f"DF IDs: {df[id_cols].head()}")
        
        return X_scaled, scaler, feature_cols, df_ids
    
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
            n_jobs=-1
        )
        
        model.fit(X_scaled)
        training_time = time.time() - start_time
        
        # Get anomaly scores
        anomaly_scores = model.decision_function(X_scaled)
        # < -0.1: Highly anomalous
        # -0.1 to 0: Suspicious
        # 0 to 0.1: Normal
        # > 0.1: Very typical
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
        
    def write_predictions_to_bigquery(self, df_ids, predictions, anomaly_scores, output_table):
        """Write predictions back to BigQuery"""
        from google.cloud import bigquery
        from datetime import datetime
        import pandas as pd
        
        results_df = df_ids.copy()
        results_df['is_anomaly'] = (predictions == -1).astype(int)
        results_df['anomaly_score'] = anomaly_scores.astype(float)
        results_df['prediction_timestamp'] = datetime.now()
        
        # Keep only needed columns
        columns = ['transaction_id', 'tag_plate_number',
                'last_updated', 'source_file',
                'flag_is_weekend', 'flag_is_out_of_state', 'flag_is_vehicle_type_gt2', 'flag_is_holiday',
                'is_anomaly', 'anomaly_score', 'prediction_timestamp']
        final_df = results_df[[col for col in columns if col in results_df.columns]]
        
        
        print(f"Predictions dataframe:")
        print(f"  Shape: {results_df.shape}")
        print(f"  Columns: {results_df.columns.tolist()}")
        print(f"  Dtypes:\n{results_df.dtypes}")
        print(f"  Sample:\n{results_df.head(3)}")
        
        # Write to BigQuery
        client = bigquery.Client(project=self.project_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
        
        job = client.load_table_from_dataframe(
            results_df, output_table, job_config=job_config
        )
        job.result()
        
        print(f"✓ Predictions written to {output_table}")
        print(f"  Total rows: {len(results_df):,}")
        print(f"  Anomalies detected: {sum(results_df['is_anomaly']):,}")
        print(f"  Anomaly rate: {sum(results_df['is_anomaly']) / len(results_df) * 100:.2f}%")
    
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
        X_scaled, scaler, feature_cols, df_ids = self.preprocess_features(df)
        
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
        self.write_predictions_to_bigquery(df_ids, predictions, anomaly_scores, output_table)
        
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
        bq_table=f"{os.environ.get('GCP_PROJECT_ID')}.ezpass_data.silver"
    )
    
    model, scores = trainer.run_training_pipeline()