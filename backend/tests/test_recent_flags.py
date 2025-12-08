import pytest
from unittest.mock import MagicMock

def test_recent_flagged_master_viz_empty(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to master_viz
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    # Mock BigQuery result to be empty
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/recent-flagged")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert data["data"] == []

def test_recent_flagged_gold_automation_with_data(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to gold_automation
    monkeypatch.setattr("app.TABLE_NAME", "gold_automation")

    # Fake row returned by BigQuery
    fake_row = {
        "transaction_id": "456",
        "transaction_date": "2025-12-05",
        "tag_plate_number": "XYZ123",
        "agency": "Test Agency",
        "amount": 100.0,
        "status": "Needs Review",
        "threat_severity": "High Risk",
        "flag_fraud": True
    }

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([fake_row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/recent-flagged")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data["data"]) == 1
    row = data["data"][0]
    assert row["transaction_id"] == "456"
    assert row["amount"] == 100.0
    assert row["category"] == "High Risk"
    assert row["is_anomaly"] is True

def test_recent_flagged_unsupported_table(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "unsupported_table")
    response = client.get("/api/transactions/recent-flagged")
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data
    assert data["error"] == "Unsupported table"
