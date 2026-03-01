from unittest.mock import MagicMock

def test_alerts_empty(mock_bigquery, client):
    """Alerts from master_viz; empty result."""
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/alerts")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert data["data"] == []


def test_alerts_with_data(mock_bigquery, client):
    """Alerts from master_viz; one row."""
    fake_row = {"transaction_id": "123", "is_anomaly": 1}
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([fake_row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/alerts")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert len(data["data"]) == 1
    assert data["data"][0]["transaction_id"] == "123"
