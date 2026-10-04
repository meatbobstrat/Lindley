"""python -m lindley: the server, with Lindley's own messages in its log."""

from lindley import __main__ as entry


def test_lindleys_messages_are_logged_beside_uvicorns(monkeypatch, tmp_path):
    ran = {}
    monkeypatch.setattr(entry.uvicorn, "run", lambda app, **kw: ran.update(app=app, **kw))
    entry.main(["--settings", str(tmp_path / "settings.json"), "--port", "9000"])
    assert ran["port"] == 9000 and not isinstance(ran["app"], str)
    loggers = ran["log_config"]["loggers"]
    assert loggers["lindley"] == {"handlers": ["default"], "level": "INFO", "propagate": False}
    assert "uvicorn.access" in loggers  # uvicorn's own, as they were


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
