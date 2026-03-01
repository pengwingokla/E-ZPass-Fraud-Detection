from unittest.mock import MagicMock

def test_metrics_empty_table(mock_bigquery, client):
    """Metrics from _metrics_loss; empty table returns zeros."""
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/metrics")
    assert response.status_code == 200
    data = response.get_json()
    assert data["total_transactions"] == 0
    assert data["total_amount_all_time"] == 0.0
    assert data["total_alert_all_time"] == 0
    assert data["loss_all_time"] == 0.0


def test_metrics_from_table(mock_bigquery, client):
    """Metrics from _metrics_loss single row."""
    row = {
        "as_of_date": None,
        "total_transactions": 1000,
        "total_amount_all_time": 5000.0,
        "loss_all_time": 1200.0,
        "total_alert_all_time": 12,
    }
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/metrics")
    assert response.status_code == 200
    data = response.get_json()
    assert data["total_transactions"] == 1000
    assert data["total_amount_all_time"] == 5000.0
    assert data["total_alert_all_time"] == 12
    assert data["loss_all_time"] == 1200.0
