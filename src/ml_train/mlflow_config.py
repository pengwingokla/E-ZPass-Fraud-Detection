"""
MLflow configuration and setup utilities for E-ZPass Fraud Detection
"""
import os
import mlflow
from mlflow.tracking import MlflowClient
from google.cloud import storage
from pathlib import Path


def setup_mlflow_tracking(
    tracking_uri=None,
    experiment_name="ezpass-fraud-detection",
    artifact_location=None,
    gcs_bucket=None,
    project_id=None
):
    """
    Set up MLflow tracking with GCS backend for artifacts.
    
    Args:
        tracking_uri: MLflow tracking server URI. If None, uses file-based tracking.
        experiment_name: Name of the MLflow experiment
        artifact_location: GCS path for artifacts (e.g., gs://bucket/mlflow-artifacts)
        gcs_bucket: GCS bucket name (will construct artifact_location if not provided)
        project_id: GCP project ID
    
    Returns:
        MlflowClient instance
    """
    # Set tracking URI
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    else:
        # Use file-based tracking (local SQLite)
        # In production, you'd use a proper database or MLflow server
        tracking_dir = os.getenv("MLFLOW_TRACKING_DIR", "/opt/airflow/mlflow-tracking")
        try:
            os.makedirs(tracking_dir, exist_ok=True, mode=0o777)
            # Try to fix permissions if directory exists but has wrong permissions
            import stat
            if os.path.exists(tracking_dir):
                os.chmod(tracking_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        except PermissionError as e:
            print(f"⚠ Warning: Cannot create/fix permissions for {tracking_dir}: {e}")
            print(f"⚠ Attempting to continue - MLflow may fail if permissions are not fixed")
        except Exception as e:
            print(f"⚠ Warning: Error setting up tracking directory {tracking_dir}: {e}")
        mlflow.set_tracking_uri(f"file://{tracking_dir}")
    
    # Set up artifact store (GCS or local fallback)
    artifact_uri = None
    if artifact_location:
        artifact_uri = artifact_location
    elif gcs_bucket:
        # Try to use GCS, but fallback to local if permissions are missing
        gcs_uri = f"gs://{gcs_bucket}/mlflow-artifacts"
        # Test GCS write permissions by trying to create a test object
        try:
            from google.cloud import storage
            client = storage.Client(project=project_id)
            bucket = client.bucket(gcs_bucket)
            # Try to check if we can write (this will fail if no permissions)
            test_blob = bucket.blob("mlflow-artifacts/.test_write_permission")
            try:
                test_blob.upload_from_string("test", content_type="text/plain")
                test_blob.delete()  # Clean up test file
                artifact_uri = gcs_uri
                print(f"✓ GCS write permissions verified, using GCS for artifacts: {artifact_uri}")
            except Exception as e:
                if "403" in str(e) or "Permission" in str(e) or "Forbidden" in str(e):
                    print(f"⚠ GCS write permissions not available, falling back to local storage")
                    artifact_uri = None  # Will trigger fallback below
                else:
                    raise
        except Exception as e:
            print(f"⚠ Could not verify GCS permissions ({e}), falling back to local storage")
            artifact_uri = None  # Will trigger fallback below
    
    # Fallback to local storage if GCS not available or permissions missing
    if artifact_uri is None:
        artifact_dir = os.getenv("MLFLOW_ARTIFACT_DIR", "/opt/airflow/mlflow-artifacts")
        try:
            os.makedirs(artifact_dir, exist_ok=True, mode=0o777)
            # Try to fix permissions if directory exists but has wrong permissions
            import stat
            if os.path.exists(artifact_dir):
                os.chmod(artifact_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
            artifact_uri = artifact_dir
            print(f"✓ Using local storage for MLflow artifacts: {artifact_uri}")
        except PermissionError as e:
            print(f"⚠ Warning: Cannot create/fix permissions for {artifact_dir}: {e}")
            print(f"⚠ Attempting to continue - MLflow may fail if permissions are not fixed")
            artifact_uri = artifact_dir
        except Exception as e:
            print(f"⚠ Warning: Error setting up artifact directory {artifact_dir}: {e}")
            artifact_uri = artifact_dir
    
    # Create or get experiment
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            experiment_id = mlflow.create_experiment(
                name=experiment_name,
                artifact_location=artifact_uri
            )
            print(f"✓ Created MLflow experiment: {experiment_name} (ID: {experiment_id})")
        else:
            experiment_id = experiment.experiment_id
            # Check if existing experiment has GCS location but we're using local storage
            # This happens when experiment was created with GCS but we don't have permissions
            existing_artifact_location = experiment.artifact_location if hasattr(experiment, 'artifact_location') else None
            lifecycle_stage = getattr(experiment, 'lifecycle_stage', None)
            
            # Check if experiment is deleted or has GCS location when we need local
            if (lifecycle_stage == 'deleted') or (existing_artifact_location and existing_artifact_location.startswith("gs://") and artifact_uri and not artifact_uri.startswith("gs://")):
                if lifecycle_stage == 'deleted':
                    print(f"⚠ Experiment is in deleted state")
                else:
                    print(f"⚠ Existing experiment has GCS artifact location ({existing_artifact_location})")
                    print(f"⚠ But we're using local storage ({artifact_uri}) due to missing permissions")
                
                # Use a different experiment name to avoid conflicts
                import time
                new_name = f"{experiment_name}_local_{int(time.time())}"
                try:
                    experiment_id = mlflow.create_experiment(
                        name=new_name,
                        artifact_location=artifact_uri
                    )
                    experiment_name = new_name  # Update the name for set_experiment
                    print(f"✓ Created new MLflow experiment with local storage: {new_name} (ID: {experiment_id})")
                except Exception as create_error:
                    print(f"⚠ Could not create new experiment: {create_error}")
                    # Fallback: try to restore deleted experiment if it was deleted
                    if lifecycle_stage == 'deleted':
                        try:
                            client = MlflowClient()
                            client.restore_experiment(experiment_id)
                            print(f"⚠ Restored deleted experiment, but it may still have GCS location")
                            print(f"⚠ Artifacts may fail to upload due to GCS permissions")
                        except Exception:
                            print(f"⚠ Could not restore experiment, will use default")
                            experiment_id = "0"
                            experiment_name = "Default"
                    else:
                        print(f"⚠ Will try to continue with existing experiment")
                        print(f"⚠ Artifacts may fail to upload due to GCS permissions")
            else:
                print(f"✓ Using existing MLflow experiment: {experiment_name} (ID: {experiment_id})")
    except Exception as e:
        print(f"⚠ Error setting up experiment: {e}")
        # Fallback: use default experiment
        experiment_id = "0"
    
    mlflow.set_experiment(experiment_name)
    
    # Ensure no active run is left from set_experiment (it shouldn't create one, but be safe)
    try:
        if mlflow.active_run():
            mlflow.end_run()
    except Exception:
        pass  # Ignore if no active run
    
    # Initialize client
    client = MlflowClient(tracking_uri=mlflow.get_tracking_uri())
    
    return client, experiment_id


def log_model_to_mlflow(
    model,
    model_name="isolation_forest",
    scaler=None,
    feature_cols=None,
    model_type="sklearn",
    registered_model_name=None,
    tags=None
):
    """
    Log model artifacts to MLflow.
    
    Args:
        model: Trained model object
        model_name: Name for the model artifact
        scaler: Preprocessing scaler (optional)
        feature_cols: List of feature column names (optional)
        model_type: Type of model (sklearn, etc.)
        registered_model_name: Name for model registry (optional)
        tags: Dictionary of tags to add
    
    Returns:
        Model URI in MLflow
    """
    import joblib
    import tempfile
    
    # Create temporary directory for artifacts
    with tempfile.TemporaryDirectory() as tmpdir:
        artifacts = {}
        
        # Save model
        model_path = os.path.join(tmpdir, "model.joblib")
        joblib.dump(model, model_path)
        artifacts["model"] = model_path
        
        # Save scaler if provided
        if scaler is not None:
            scaler_path = os.path.join(tmpdir, "scaler.joblib")
            joblib.dump(scaler, scaler_path)
            artifacts["scaler"] = scaler_path
        
        # Save feature columns if provided
        if feature_cols is not None:
            import json
            feature_path = os.path.join(tmpdir, "feature_cols.json")
            with open(feature_path, "w") as f:
                json.dump(feature_cols, f)
            artifacts["feature_cols"] = feature_path
        
        # Log model based on type
        if model_type == "sklearn":
            mlflow.sklearn.log_model(
                model,
                artifact_path=model_name,
                registered_model_name=registered_model_name
            )
        else:
            # Generic model logging
            mlflow.log_artifacts(tmpdir, artifact_path=model_name)
    
    # Add tags if provided
    if tags:
        for key, value in tags.items():
            mlflow.set_tag(key, str(value))
    
    model_uri = mlflow.get_artifact_uri(artifact_path=model_name)
    print(f"✓ Model logged to MLflow: {model_uri}")
    
    return model_uri


def load_model_from_mlflow(model_uri, model_name="model"):
    """
    Load a model from MLflow.
    
    Args:
        model_uri: MLflow model URI
        model_name: Name of the model artifact
    
    Returns:
        Loaded model object
    """
    import joblib
    
    # Download artifacts
    artifacts_path = mlflow.artifacts.download_artifacts(artifact_uri=model_uri)
    model_path = os.path.join(artifacts_path, f"{model_name}.joblib")
    
    model = joblib.load(model_path)
    return model



