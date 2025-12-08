from unittest.mock import MagicMock

def test_update_status_success(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    # Mock the query result for selecting current status
    mock_select_job = MagicMock()
    mock_select_job.result.return_value = iter([{"status": "Needs Review"}])
    mock_bigquery.query.return_value = mock_select_job

    # Mock the update query
    mock_update_job = MagicMock()
    mock_update_job.result.return_value = iter([{}])
    mock_bigquery.query.return_value = mock_update_job

    response = client.post("/api/transactions/update-status", json={
        "transactionId": "txn123",
        "newStatus": "Investigating"
    })

    assert response.status_code == 200
    assert response.get_json()["success"] is True

def test_update_status_invalid_transition(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    mock_select_job = MagicMock()
    mock_select_job.result.return_value = iter([{"status": "Resolved - Fraud"}])
    mock_bigquery.query.return_value = mock_select_job

    response = client.post("/api/transactions/update-status", json={
        "transactionId": "txn123",
        "newStatus": "Investigating"
    })

    assert response.status_code == 400
    data = response.get_json()
    assert "Invalid status transition" in data["error"]

def test_update_status_missing_fields(client):
    response = client.post("/api/transactions/update-status", json={
        "transactionId": "txn123"
    })
    assert response.status_code == 400
    data = response.get_json()
    assert "Missing transactionId or newStatus" in data["error"]

def test_update_status_transaction_not_found(mock_bigquery, client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    mock_select_job = MagicMock()
    mock_select_job.result.return_value = iter([])  # No rows found
    mock_bigquery.query.return_value = mock_select_job

    response = client.post("/api/transactions/update-status", json={
        "transactionId": "txn999",
        "newStatus": "Investigating"
    })

    assert response.status_code == 404
    data = response.get_json()
    assert "Transaction not found" in data["error"]
