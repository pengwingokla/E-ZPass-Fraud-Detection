from unittest.mock import MagicMock

def test_metrics_master_viz_empty(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to master_viz
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    # Mock BigQuery result to return a single empty row
    fake_row = {
        "total_transactions": 0,
        "total_flagged": 0,
        "total_amount": 0.0,
        "total_alerts_ytd": 0,
        "detected_frauds_current_month": 0,
        "potential_loss_ytd": 0.0
    }

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([fake_row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/metrics")
    assert response.status_code == 200
    data = response.get_json()
    assert data["total_transactions"] == 0
    assert data["total_flagged"] == 0
    assert data["total_amount"] == 0.0
    assert data["total_alerts_ytd"] == 0
    assert data["detected_frauds_current_month"] == 0
    assert data["potential_loss_ytd"] == 0.0

def test_metrics_gold_automation_with_data(mock_bigquery, client, monkeypatch):
    # Force TABLE_NAME to gold_automation
    monkeypatch.setattr("app.TABLE_NAME", "gold_automation")

    fake_row = {
        "total_transactions": 100,
        "total_flagged": 5,
        "total_amount": 2500.0,
        "total_alerts_ytd": 3,
        "detected_frauds_current_month": 2,
        "potential_loss_ytd": 1500.0
    }

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter([fake_row])
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/metrics")
    assert response.status_code == 200
    data = response.get_json()
    assert data["total_transactions"] == 100
    assert data["total_flagged"] == 5
    assert data["total_amount"] == 2500.0
    assert data["total_alerts_ytd"] == 3
    assert data["detected_frauds_current_month"] == 2
    assert data["potential_loss_ytd"] == 1500.0

def test_metrics_unsupported_table(client, monkeypatch):
    # Force TABLE_NAME to unsupported
    monkeypatch.setattr("app.TABLE_NAME", "unsupported_table")

    response = client.get("/api/metrics")
    assert response.status_code == 400
    data = response.get_json()
    assert "error" in data
    assert data["error"] == "Unsupported table"
