from unittest.mock import MagicMock

def test_transactions_error(mock_bigquery, client):
    # Force BigQuery to raise an error
    mock_bigquery.query.side_effect = Exception("Boom!")

    response = client.get("/api/transactions")
    assert response.status_code == 500
    
    data = response.get_json()
    assert "error" in data
