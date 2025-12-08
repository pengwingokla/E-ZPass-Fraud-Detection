def test_transactions_returns_rows(mock_bigquery, client):
    # Mock a fake BigQuery response row
    mock_bigquery.query.return_value.result.return_value = [
        {"transaction_id": 123, "amount": 50.0},
        {"transaction_id": 456, "amount": 75.0}
    ]

    response = client.get("/api/transactions?page=1&limit=2")
    assert response.status_code == 200

    data = response.get_json()
    assert len(data["data"]) == 2
    assert data["data"][0]["transaction_id"] == 123
