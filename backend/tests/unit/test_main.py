"""python -m lindley: the server, with Lindley's own messages in its log."""

import json

import pytest

from lindley import __main__ as entry
from lindley import logs


def _settings(tmp_path):
    path = tmp_path / "settings.json"
    path.write_text(json.dumps({"db_path": str(tmp_path / "data" / "lindley.db")}))
    return path


def _started(monkeypatch):
    ran = {}
    monkeypatch.setattr(entry.uvicorn, "Config", lambda app, **kw: dict(app=app, **kw))
    monkeypatch.setattr(entry.uvicorn, "Server", lambda config: config)
    monkeypatch.setattr(entry.launcher, "already_running", lambda url: False)
    monkeypatch.setattr(
        entry.launcher, "run", lambda config, url, **kw: ran.update(config, url=url, **kw)
    )
    monkeypatch.setattr(entry.logs, "install_excepthooks", lambda: None)  # not for the tests
    monkeypatch.delenv(logs.LEVEL_ENV_VAR, raising=False)
    return ran


def test_lindleys_messages_are_logged_beside_uvicorns(monkeypatch, tmp_path):
    ran = _started(monkeypatch)
    entry.main(["--settings", str(_settings(tmp_path)), "--port", "9000"])
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


def test_debug_logs_in_detail_by_flag_or_by_environment(monkeypatch, tmp_path):
    ran = _started(monkeypatch)
    settings = str(_settings(tmp_path))
    entry.main(["--settings", settings, "--debug"])
    assert ran["log_config"]["loggers"]["lindley"]["level"] == "DEBUG"
    monkeypatch.setenv(logs.LEVEL_ENV_VAR, "debug")
    entry.main(["--settings", settings])
    assert ran["log_config"]["loggers"]["lindley"]["level"] == "DEBUG"
    monkeypatch.setenv(logs.LEVEL_ENV_VAR, "nonsense")  # not a level: as usual
    entry.main(["--settings", settings])
    assert ran["log_config"]["loggers"]["lindley"]["level"] == "INFO"


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
    path = _settings(tmp_path)
    entry.main(["--reload", "--settings", str(path)])
    assert ran["app"] == "lindley.app:create_app" and ran["factory"] and ran["reload"]
    assert ran["reload_dirs"] == [str(entry.SOURCE)] and (entry.SOURCE / "lindley").is_dir()
    assert entry.os.environ[entry.SETTINGS_ENV_VAR] == str(path)
    assert "lindley" in ran["log_config"]["loggers"]
    # Logged to the file too, by one writer at a time: two processes can't rotate it
    handler = ran["log_config"]["handlers"]["file"]
    assert handler["filename"] == str(tmp_path / "data" / "logs" / "lindley.log")
    assert handler["class"] == "logging.FileHandler"


def test_settings_it_cant_read_are_said_in_words_not_a_traceback(
    monkeypatch, tmp_path, capsys, caplog
):
    ran = _started(monkeypatch)
    monkeypatch.setattr(entry.logs, "default_log_file", lambda: tmp_path / "logs" / "lindley.log")
    monkeypatch.setattr(entry.logging.config, "dictConfig", lambda config: None)
    bad = tmp_path / "settings.json"
    bad.write_text("{ not json")
    with pytest.raises(SystemExit) as stopped:
        entry.main(["--settings", str(bad)])
    assert stopped.value.code == 1 and not ran  # didn't start
    said = capsys.readouterr().err
    assert str(bad) in said and "couldn't read its settings" in said
    assert "Traceback" not in said
    bad.write_text(json.dumps({"move_files": "sometimes"}))
    with pytest.raises(SystemExit):
        entry.main(["--settings", str(bad)])
    assert "move_files" in capsys.readouterr().err  # which setting, not "1 validation error"
