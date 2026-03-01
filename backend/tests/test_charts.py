from unittest.mock import MagicMock

def test_category_chart(mock_bigquery, client):
    """Category chart from master_viz."""
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
