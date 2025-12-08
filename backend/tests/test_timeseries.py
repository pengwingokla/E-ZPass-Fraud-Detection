def test_timeseries_chart_unsupported_table(client, monkeypatch):
    monkeypatch.setattr("app.TABLE_NAME", "unsupported_table")
    response = client.get("/api/charts/timeseries")
    assert response.status_code == 400
    data = response.get_json()
    assert data["error"] == "Unsupported table"
