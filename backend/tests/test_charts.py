from unittest.mock import MagicMock

def test_category_chart_master_viz(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    # Mock BigQuery result
    fake_results = [
        {"category": "Rush Hour", "count": 5},
        {"category": "Holiday", "count": 2},
    ]
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter(fake_results)
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/charts/category")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data["data"]) == 2
    assert data["data"][0]["category"] == "Rush Hour"
    assert data["data"][0]["count"] == 5

def test_category_chart_gold_automation(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "gold_automation")

    fake_results = [
        {"category": "Vehicle Type", "count": 3},
        {"category": "Amount > 29", "count": 1},
    ]
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter(fake_results)
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/charts/category")
    assert response.status_code == 200
    data = response.get_json()
    assert data["data"][0]["category"] == "Vehicle Type"
    assert data["data"][0]["count"] == 3

def test_category_chart_unsupported_table(client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "unsupported_table")

    response = client.get("/api/charts/category")
    assert response.status_code == 400
    data = response.get_json()
    assert data["error"] == "Unsupported table"
