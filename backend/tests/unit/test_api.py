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


def test_a_new_database_place_must_hold_lindleys_database(client, tmp_path):
    current = client.get("/api/settings").json()
    for where in (tmp_path / "typo" / "lindley.db", tmp_path / "nothing.db"):
        current["db_path"] = str(where)
        r = client.put("/api/settings", json=current)
        assert r.status_code == 422
        assert "Move Lindley's database file there first" in r.json()["detail"][0]
        assert not where.exists()  # nothing made there
    (tmp_path / "notes.db").write_text("not a database")
    current["db_path"] = str(tmp_path / "notes.db")
    assert "can open" in client.put("/api/settings", json=current).json()["detail"][0]
    # Settings and the library are as they were
    assert client.get("/api/settings").json()["db_path"] == str(tmp_path / "lindley.db")
    assert client.get("/api/inbox").status_code == 200


def test_a_database_moved_there_first_is_used(client, tmp_path):
    import shutil

    moved = tmp_path / "elsewhere" / "lindley.db"
    moved.parent.mkdir()
    shutil.copy(tmp_path / "lindley.db", moved)
    current = client.get("/api/settings").json()
    current["db_path"] = str(moved)
    assert client.put("/api/settings", json=current).status_code == 200
    assert client.get("/api/settings").json()["db_path"] == str(moved)
    assert client.get("/api/inbox").status_code == 200


def test_folders_to_watch_say_whether_theyre_there(client, tmp_path, monkeypatch):
    from lindley.api import settings as settings_api

    inbox = tmp_path / "Documents" / "Lindley" / "Inbox"  # made the first time it's watched
    monkeypatch.setattr(settings_api, "default_inbox", lambda: inbox)
    (tmp_path / "scans").mkdir()
    asked = [str(tmp_path / "scans"), str(tmp_path / "scnas"), "Scans", str(inbox)]
    r = client.get("/api/settings/folders", params={"path": asked})
    assert [f["found"] for f in r.json()["folders"]] == [True, False, False, True]
    # The overview says which watched folders aren't there
    assert client.get("/api/overview").json()["missing_folders"] == [str(tmp_path / "inbox")]


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
