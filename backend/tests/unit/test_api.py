from lindley import __version__


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["version"] == __version__


def test_settings_get_and_put(client, tmp_path):
    current = client.get("/api/settings").json()
    current["move_files"] = True
    r = client.put("/api/settings", json=current)
    assert r.status_code == 200
    assert client.get("/api/settings").json()["move_files"] is True
    assert (tmp_path / "settings.json").exists()


def test_stub_endpoints_return_501(client):
    for path in ("/api/documents", "/api/search", "/api/chat"):
        assert client.get(path).status_code == 501
