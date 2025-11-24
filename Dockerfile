FROM apache/airflow:2.7.0-python3.10

# Install ML packages
RUN pip install --no-cache-dir \
    google-cloud-aiplatform>=1.38.0 \
    google-cloud-bigquery>=3.11.0 \
    scikit-learn>=1.3.0 \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    joblib>=1.3.0