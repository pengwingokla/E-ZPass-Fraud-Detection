import pytest
from unittest.mock import MagicMock, patch
from app import app

@pytest.fixture
def client():
    """Flask test client."""
    app.testing = True
    return app.test_client()

@pytest.fixture
def mock_bigquery():
    """
    Patch app.client so the BigQuery client inside the app
    is replaced with our mock.
    """
    with patch("app.client") as mock_client:
        # This ensures mock_client.query().result() exists
        mock_client.query.return_value.result.return_value = iter([])

        yield mock_client

