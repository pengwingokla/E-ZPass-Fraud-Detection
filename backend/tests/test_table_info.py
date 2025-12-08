def test_table_info_master_viz(client, monkeypatch):
    # Temporarily set TABLE_NAME to master_viz
    monkeypatch.setattr("app.TABLE_NAME", "master_viz")

    response = client.get("/api/table-info")
    assert response.status_code == 200
    data = response.get_json()
    assert data["table_name"] == "master_viz"
    assert data["is_master_viz"] is True
    assert data["is_gold_automation"] is False

def test_table_info_gold_automation(client, monkeypatch):
    # Temporarily set TABLE_NAME to gold_automation
    monkeypatch.setattr("app.TABLE_NAME", "gold_automation")

    response = client.get("/api/table-info")
    assert response.status_code == 200
    data = response.get_json()
    assert data["table_name"] == "gold_automation"
    assert data["is_master_viz"] is False
    assert data["is_gold_automation"] is True

def test_table_info_unknown(client, monkeypatch):
    # Temporarily set TABLE_NAME to something else
    monkeypatch.setattr("app.TABLE_NAME", "unknown_table")

    response = client.get("/api/table-info")
    assert response.status_code == 200
    data = response.get_json()
    assert data["table_name"] == "unknown_table"
    assert data
