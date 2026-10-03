"""When Lindley may call an AI on its own, and the record of the calls it makes."""

import pytest

from lindley.db.database import connect, init_db
from lindley.providers import allowance


@pytest.fixture
def conn(settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


def test_by_default_every_ai_waits_for_an_ok(conn, settings):
    assert allowance.automatic_left(conn, settings, "local") == 0
    assert not allowance.may_call(conn, settings, "local")
    assert not allowance.may_call(conn, settings, "not set up")
    assert not allowance.may_call(conn, settings, None)


def test_an_ai_allowed_to_run_on_its_own_keeps_to_its_daily_limit(conn, settings):
    cfg = settings.ai.providers["local"]
    cfg.allow = "auto"
    assert allowance.automatic_left(conn, settings, "local") is None  # no limit
    cfg.daily_limit = 2
    assert allowance.automatic_left(conn, settings, "local") == 2
    allowance.record(conn, "local", "vision", True)
    allowance.record(conn, "local", "vision", False, count=5)  # OKed by a person: not counted
    assert allowance.automatic_left(conn, settings, "local") == 1
    allowance.record(conn, "local", "assemble", True)
    assert not allowance.may_call(conn, settings, "local")
    assert "2 automatic calls are used up" in allowance.why_waiting(conn, settings, "local")
    conn.execute("UPDATE ai_calls SET at = datetime('now', '-2 days')")
    assert allowance.automatic_left(conn, settings, "local") == 2  # a new day


def test_a_monthly_limit_too(conn, settings):
    cfg = settings.ai.providers["local"]
    cfg.allow, cfg.daily_limit, cfg.monthly_limit = "auto", 10, 3
    assert allowance.automatic_left(conn, settings, "local") == 3  # the smaller of the two
    allowance.record(conn, "local", "vision", True, count=3)
    assert not allowance.may_call(conn, settings, "local")
    assert "this month's 3 automatic calls" in allowance.why_waiting(conn, settings, "local")
    conn.execute("UPDATE ai_calls SET at = datetime('now', 'start of month', '-1 day')")
    assert allowance.calls_this_month(conn, "local") == 0
    assert allowance.automatic_left(conn, settings, "local") == 3  # a new month


def test_the_calls_are_recorded_with_what_they_were_for(conn):
    allowance.record(conn, "local", "vision", True, ok=False)
    allowance.record(conn, "local", "assemble", False, count=2)
    rows = conn.execute("SELECT purpose, automatic, ok FROM ai_calls ORDER BY id").fetchall()
    assert [tuple(r) for r in rows] == [("vision", 1, 0), ("assemble", 0, 1), ("assemble", 0, 1)]
    assert allowance.calls_today(conn, "local", automatic=False) == 2


def test_only_calls_that_worked_count_against_the_limits(conn, settings):
    cfg = settings.ai.providers["local"]
    cfg.allow, cfg.daily_limit = "auto", 2
    allowance.record(conn, "local", "vision", True, ok=False)
    allowance.record(conn, "local", "vision", True)
    assert allowance.automatic_left(conn, settings, "local") == 1
    assert allowance.calls_today(conn, "local") == 2  # both are shown


def test_an_ai_whose_calls_keep_failing_is_left_alone_for_a_while(conn, settings):
    settings.ai.providers["local"].allow = "auto"
    allowance.record(conn, "local", "vision", True, ok=False, count=allowance.FAILING_AFTER - 1)
    assert not allowance.failing(conn, "local")
    allowance.record(conn, "local", "vision", False, ok=False)  # a person's call isn't counted
    assert not allowance.failing(conn, "local")
    allowance.record(conn, "local", "vision", True, ok=False)
    assert allowance.failing(conn, "local")
    assert "calls failed" in allowance.why_waiting(conn, settings, "local")
    conn.execute(
        "UPDATE ai_calls SET at = datetime('now', ?)", (f"-{allowance.FAILING_WAIT_MIN} minutes",)
    )
    assert not allowance.failing(conn, "local")  # time to try it again
    allowance.record(conn, "local", "vision", True)
    assert not allowance.failing(conn, "local")
