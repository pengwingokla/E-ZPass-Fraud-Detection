def test_table_info(client):
    """Table info always returns master_viz."""
    response = client.get("/api/table-info")
    assert response.status_code == 200
    data = response.get_json()
    assert data["table_name"] == "master_viz"
    assert data["is_master_viz"] is True
    assert "is_gold_automation" not in data
