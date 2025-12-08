import pytest
from unittest.mock import MagicMock, patch

@pytest.fixture
def mock_bigquery():
    """
    Patch BigQuery client so no real queries run.
    """
    with patch("app.client") as mock_client:
        # Mock a RowIterator with one row containing total count
        mock_row = {"total": 0}  # Change the number to whatever you want to test
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = iter([mock_row])
        mock_client.query.return_value = mock_query_job
        yield mock_client

def test_transactions_count_empty(mock_bigquery, client):
    """
    Test /api/transactions/count when there are no rows
    """
    response = client.get("/api/transactions/count")
    assert response.status_code == 200
    data = response.get_json()
    assert "total" in data
    assert data["total"] == 0

def test_transactions_count_with_filters(mock_bigquery, client):
    """
    Test /api/transactions/count with query parameters
    """
    response = client.get("/api/transactions/count?search=test&status=Needs+Review&category=Critical+Risk")
    assert response.status_code == 200
    data = response.get_json()
    assert "total" in data
    assert data["total"] == 0  # Our mock always returns 0
