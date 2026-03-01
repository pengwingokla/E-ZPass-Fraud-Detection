from unittest.mock import MagicMock

def test_timeseries_chart(mock_bigquery, client):
    """Timeseries chart from master_viz."""
    fake_results = [{"hour": 8, "fraud_count": 5}, {"hour": 9, "fraud_count": 3}]
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = iter(fake_results)
    mock_bigquery.query.return_value = mock_query_job

    response = client.get("/api/charts/timeseries")
    assert response.status_code == 200
    data = response.get_json()
    assert len(data["data"]) == 2
    assert data["data"][0]["hour"] == 8
    assert data["data"][0]["fraud_count"] == 5
