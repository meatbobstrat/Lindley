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


def test_settings_say_when_each_ai_may_run_and_how_much_it_has_today(client, settings):
    current = client.get("/api/settings").json()
    assert current["ai"]["providers"]["local"]["allow"] == "ask"
    current["ai"]["providers"]["local"] |= {"allow": "auto", "daily_limit": 50}
    assert client.put("/api/settings", json=current).status_code == 200
    usage = client.get("/api/settings/ai-calls").json()["providers"]["local"]
    assert usage == {
        "allow": "auto",
        "daily_limit": 50,
        "monthly_limit": None,
        "per_minute": None,
        "at_once": 2,
        "automatic_today": 0,
        "oked_today": 0,
        "automatic_month": 0,
        "oked_month": 0,
        "automatic_left": 50,
        "spent_today": 0,
        "spent_month": 0,
        "key_hint": None,
    }
    current["ai"]["providers"]["local"]["daily_limit"] = 0
    assert client.put("/api/settings", json=current).status_code == 422  # at least 1, or none
