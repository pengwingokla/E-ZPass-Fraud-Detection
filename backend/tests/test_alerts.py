import pytest
from unittest.mock import MagicMock

def test_alerts_master_viz_empty(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to "master_viz"
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    # Mock BigQuery result to be empty
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/alerts")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert data["data"] == []

def test_alerts_gold_automation_with_data(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to "gold_automation"
    monkeypatch.setattr("app.TABLE_NAME", "gold_automation")

    # Mock BigQuery returning one fake row
    fake_row = {"transaction_id": "123", "flag_fraud": True}
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([fake_row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/alerts")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert len(data["data"]) == 1
    assert data["data"][0]["transaction_id"] == "123"

def test_alerts_unsupported_table(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to an unsupported value
    monkeypatch.setattr("app.TABLE_NAME", "unsupported_table")

    response = client.get("/api/transactions/alerts")
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data
    assert data["error"] == "Unsupported table"
