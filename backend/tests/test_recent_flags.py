from unittest.mock import MagicMock

def test_recent_flagged_empty(mock_bigquery, client):
    """Recent flagged from master_viz; empty."""
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/transactions/recent-flagged")
    assert response.status_code == 200
    data = response.get_json()
    assert "data" in data
    assert data["data"] == []


def test_recent_flagged_with_data(mock_bigquery, client):
    """Recent flagged from master_viz; one row with ml_predicted_category, is_anomaly."""
    fake_row = {
        "transaction_id": "456",
        "transaction_date": "2025-12-05",
        "tag_plate_number": "XYZ123",
        "agency": "Test Agency",
        "amount": 100.0,
        "status": "Needs Review",
        "ml_predicted_category": "High Risk",
        "is_anomaly": 1,
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
