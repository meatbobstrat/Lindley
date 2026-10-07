"""python -m lindley: the server, with Lindley's own messages in its log."""

import json

from lindley import __main__ as entry


def test_lindleys_messages_are_logged_beside_uvicorns(monkeypatch, tmp_path):
    ran = {}
    monkeypatch.setattr(entry.uvicorn, "Config", lambda app, **kw: dict(app=app, **kw))
    monkeypatch.setattr(entry.uvicorn, "Server", lambda config: config)
    monkeypatch.setattr(entry.launcher, "already_running", lambda url: False)
    monkeypatch.setattr(
        entry.launcher, "run", lambda config, url, **kw: ran.update(config, url=url, **kw)
    )
    settings = tmp_path / "settings.json"
    settings.write_text(json.dumps({"db_path": str(tmp_path / "data" / "lindley.db")}))
    entry.main(["--settings", str(settings), "--port", "9000"])
    assert ran["port"] == 9000 and ran["url"] == "http://127.0.0.1:9000/"
    assert ran["browser"] and ran["tray"]  # as the menu starts it
    loggers = ran["log_config"]["loggers"]
    assert loggers["lindley"] == {
        "handlers": ["default", "file"],
        "level": "INFO",
        "propagate": False,
    }
    assert "uvicorn.access" in loggers  # uvicorn's own, as they were
    # Kept beside the database too: started from the menu, there's no console
    assert ran["log_config"]["handlers"]["file"]["filename"] == str(
        tmp_path / "data" / "logs" / "lindley.log"
    )
    assert ran["app"].state.quit  # Quit Lindley, in the app


def test_started_again_it_opens_the_one_running(monkeypatch, tmp_path, capsys):
    opened = []
    monkeypatch.setattr(entry.launcher, "already_running", lambda url: True)
    monkeypatch.setattr(entry.launcher.webbrowser, "open", opened.append)
    monkeypatch.setattr(entry.launcher, "run", lambda *a, **kw: opened.append("a second Lindley"))
    entry.main(["--settings", str(tmp_path / "settings.json")])
    assert opened == ["http://127.0.0.1:8765/"] and "already running" in capsys.readouterr().out


def test_reload_names_the_app_and_passes_the_settings_on(monkeypatch, tmp_path):
    ran = {}
    monkeypatch.setattr(entry.uvicorn, "run", lambda app, **kw: ran.update(app=app, **kw))
    monkeypatch.setenv(entry.SETTINGS_ENV_VAR, "before")  # put back after the test
    path = tmp_path / "settings.json"
    entry.main(["--reload", "--settings", str(path)])
    assert ran["app"] == "lindley.app:create_app" and ran["factory"] and ran["reload"]
    assert ran["reload_dirs"] == [str(entry.SOURCE)] and (entry.SOURCE / "lindley").is_dir()
    assert entry.os.environ[entry.SETTINGS_ENV_VAR] == str(path)
    assert "lindley" in ran["log_config"]["loggers"]
