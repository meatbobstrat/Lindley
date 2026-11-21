"""What goes wrong in the app and the database: said in words to the screen, in full in the log."""

import json
import logging
import sqlite3

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from lindley import history
from lindley.api import needs_ai
from lindley.app import create_app
from lindley.browse import _json_list
from lindley.db import database
from lindley.db.database import SCHEMA_VERSION, connect, init_db


@pytest.fixture(autouse=True)
def everything_logged(caplog):
    caplog.set_level(logging.DEBUG, logger="lindley")


def _app_with(settings, tmp_path, **routes):
    """The app with extra routes in front (the frontend, where built, is mounted behind them)."""
    app = create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    for path, fail in routes.items():
        app.router.routes.insert(0, APIRoute(path, fail, methods=["GET"]))
    return app


def _boom(error):
    def fail():
        raise error

    return fail


# ---------------------------------------------------------------- what the screen is told


def test_an_unexpected_error_is_answered_in_words_and_logged(settings, tmp_path, caplog):
    secret = RuntimeError(r"no such column secret_col in C:\Users\someone\lindley.db")
    app = _app_with(settings, tmp_path, **{"/api/boom": _boom(secret)})
    with TestClient(app, base_url="http://127.0.0.1", raise_server_exceptions=False) as c:
        r = c.get("/api/boom")
    assert r.status_code == 500
    assert r.json() == {
        "detail": "Something went wrong inside Lindley. The details are in Lindley's log."
    }
    assert "secret_col" not in r.text and "someone" not in r.text  # nothing of its insides
    assert "GET /api/boom failed: RuntimeError" in caplog.text  # which request, in the log
    assert "secret_col" in caplog.text


def test_a_busy_database_is_try_again_not_a_failure(settings, tmp_path, caplog):
    app = _app_with(
        settings, tmp_path, **{"/api/busy": _boom(sqlite3.OperationalError("database is locked"))}
    )
    with TestClient(app, base_url="http://127.0.0.1", raise_server_exceptions=False) as c:
        r = c.get("/api/busy")
    assert r.status_code == 503 and "busy" in r.json()["detail"]
    assert "the database was busy" in caplog.text


def test_a_database_that_fails_is_a_500_with_the_cause_in_the_log(settings, tmp_path, caplog):
    app = _app_with(
        settings,
        tmp_path,
        **{"/api/broken": _boom(sqlite3.DatabaseError("database disk image is malformed"))},
    )
    with TestClient(app, base_url="http://127.0.0.1", raise_server_exceptions=False) as c:
        r = c.get("/api/broken")
    assert r.status_code == 500 and "malformed" not in r.text
    assert "database disk image is malformed" in caplog.text
    assert any(rec.exc_info for rec in caplog.records if rec.levelno == logging.ERROR)


def test_a_change_from_another_site_is_refused_and_logged(client, caplog):
    r = client.post("/api/quit", headers={"Origin": "http://evil.example"})
    assert r.status_code == 403
    assert "Refused POST /api/quit from another site" in caplog.text
    assert "evil.example" in caplog.text


# ---------------------------------------------------------------- settings


def test_settings_that_cant_be_saved_leave_the_settings_in_use_as_they_were(
    client, monkeypatch, caplog
):
    def cant(settings, path=None):
        raise PermissionError(13, "Permission denied", "C:\\secret\\settings.json")

    monkeypatch.setattr("lindley.api.settings.save_settings", cant)
    current = client.get("/api/settings").json()
    current["move_files"] = True
    r = client.put("/api/settings", json=current)
    assert r.status_code == 500 and "Permission denied" in r.json()["detail"]
    assert "secret" not in r.text  # the words, not the path
    assert client.get("/api/settings").json()["move_files"] is False


def test_a_step_after_saving_that_fails_is_named_and_the_rest_still_run(
    client, monkeypatch, caplog
):
    ran = []
    monkeypatch.setattr(
        "lindley.api.settings.FolderWatcher", lambda s: (_ for _ in ()).throw(OSError("no"))
    )
    monkeypatch.setattr("lindley.api.settings.local_server.use", lambda local: ran.append("ai"))
    client.app.state.watcher = object.__new__(type("W", (), {"stop": lambda self, wait=True: None}))
    current = client.get("/api/settings").json()
    current["move_files"] = True
    r = client.put("/api/settings", json=current)
    assert r.status_code == 500
    assert "saved, but Lindley couldn't restart the folder watcher" in r.json()["detail"]
    assert ran == ["ai"]  # what came after it still happened
    assert client.get("/api/settings").json()["move_files"] is True  # and the settings are kept
    assert "couldn't restart the folder watcher" in caplog.text
    assert "changed: move_files" in caplog.text  # the names of what changed, not the values


# ---------------------------------------------------------------- the database


def test_an_upgrade_cut_off_carries_on_from_where_it_got_to(tmp_path, monkeypatch, caplog):
    path = tmp_path / "lindley.db"
    init_db(path)
    conn = connect(path)
    conn.execute("PRAGMA user_version = 9")  # as if it were an older Lindley's, up to step 9
    conn.commit()
    conn.close()
    steps = dict(database.MIGRATIONS)
    attempts = []

    def flaky(c):
        attempts.append(1)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(
        database,
        "MIGRATIONS",
        steps | {10: "", 11: "", 12: flaky, 13: "", 14: "", 15: "", 16: "", 17: ""},
    )
    with pytest.raises(sqlite3.OperationalError):
        init_db(path)
    conn = connect(path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 11  # done is done
    conn.close()
    assert "Upgrading the database to schema 12 failed" in caplog.text
    init_db(path)  # started again: carries on at 12, doesn't redo 10 and 11
    conn = connect(path)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    conn.close()
    assert len(attempts) == 2


def test_a_step_that_fails_in_the_middle_is_put_back_whole(tmp_path, monkeypatch):
    path = tmp_path / "lindley.db"
    init_db(path)
    conn = connect(path)
    conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.close()
    two = "ALTER TABLE history ADD COLUMN extra_column INTEGER; SELECT nothing_here FROM nowhere;"
    monkeypatch.setattr(database, "MIGRATIONS", database.MIGRATIONS | {2: two})
    with pytest.raises(sqlite3.OperationalError):
        init_db(path)
    conn = connect(path)
    assert "extra_column" not in {r[1] for r in conn.execute("PRAGMA table_info(history)")}
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    assert not conn.in_transaction
    conn.close()


def test_an_ai_table_upgrade_cut_off_is_picked_up(tmp_path, caplog):
    path = tmp_path / "lindley.db"
    init_db(path)
    conn = connect(path)
    conn.execute("INSERT INTO ai_calls (provider, purpose, automatic) VALUES ('x', 'vision', 1)")
    conn.commit()
    # Cut off after the first statement of the v15 step: the table's own copy is beside it
    conn.execute("ALTER TABLE ai_calls RENAME TO ai_calls_v14")
    conn.execute("PRAGMA user_version = 14")
    conn.commit()
    conn.close()
    init_db(path)
    conn = connect(path)
    assert conn.execute("SELECT COUNT(*) FROM ai_calls").fetchone()[0] == 1
    assert "ai_calls_v14" not in {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    conn.close()
    assert "cut off" in caplog.text


def test_a_file_the_database_cant_be_read_from_says_why(tmp_path, caplog):
    junk = tmp_path / "junk.db"
    junk.write_text("not a database at all, just some words " * 20)
    assert "isn't a database Lindley can open" in database.not_lindleys(junk)
    assert "junk.db" in caplog.text


# ---------------------------------------------------------------- damaged records


def test_a_damaged_list_in_the_database_is_empty_and_said_in_the_log(caplog):
    assert _json_list("[1, 2]", "x") == [1, 2] and _json_list(None, "x") == []
    assert _json_list("{not json", "document 4's reasons") == []
    assert _json_list('{"a": 1}', "x") == []  # not a list
    assert "document 4's reasons is damaged" in caplog.text


def test_one_damaged_row_doesnt_take_the_needs_ai_list_down(settings, caplog):
    init_db(settings.db_path)
    conn = connect(settings.db_path)
    conn.execute("INSERT INTO needs_ai (pages, proposal) VALUES ('[1', '[]')")
    assert needs_ai.sort_items(conn) == []
    conn.close()
    assert "Skipping row" in caplog.text


def test_a_change_whose_record_is_damaged_cant_be_undone_but_doesnt_crash(settings, caplog):
    init_db(settings.db_path)
    conn = connect(settings.db_path)
    conn.execute(
        "INSERT INTO history (actor, action, target_type, target_id, before, after, batch)"
        " VALUES ('user', 'move', 'from_a_newer_lindley', 1, ?, ?, 1)",
        (json.dumps({}), json.dumps({})),
    )
    conn.commit()
    with pytest.raises(history.UndoError, match="damaged"):
        history.undo(conn, 1)
    assert "can't be undone" in caplog.text
    with pytest.raises(history.NothingToUndo):
        history.undo(conn, 99)
    conn.close()


def test_undo_and_its_refusals_are_in_the_log(client, settings, caplog):
    assert client.post("/api/undo").status_code == 404  # nothing to undo
    assert "Undo: There's nothing to undo" in caplog.text
