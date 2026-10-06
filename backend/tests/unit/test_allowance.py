"""When Lindley may call an AI on its own, and the record of the calls it makes."""

from datetime import date

import pytest

from lindley.db.database import connect, init_db
from lindley.providers import allowance
from lindley.providers.base import Usage
from lindley.providers.prices import cost


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


def test_what_each_call_used_and_cost_is_recorded(conn):
    opus = Usage("claude-opus-5-5", input_tokens=2000, output_tokens=500)
    allowance.record(conn, "claude", "assemble", False, count=2, used=[opus])
    # a call the AI answered, then failed (cut off): charged, and recorded as failed
    allowance.record(conn, "claude", "vision", True, page_id=None, count=0, used=[opus])
    allowance.record(conn, "local", "vision", True, used=[Usage("gemma4:e4b", 1000, 300)])
    rows = conn.execute(
        "SELECT provider, purpose, ok, model, input_tokens, output_tokens, cost_usd"
        " FROM ai_calls ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in rows] == [
        ("claude", "assemble", 1, "claude-opus-5-5", 2000, 500, 0.018),
        ("claude", "assemble", 1, None, None, None, None),  # the AI didn't say
        ("claude", "vision", 0, "claude-opus-5-5", 2000, 500, 0.018),
        ("local", "vision", 1, "gemma4:e4b", 1000, 300, None),  # no price on a laptop
    ]
    assert allowance.spent(conn, "claude") == pytest.approx(0.036)
    assert allowance.spent(conn, "claude", "month") == pytest.approx(0.036)
    assert allowance.spent(conn, "local") == 0
    conn.execute("UPDATE ai_calls SET at = datetime('now', 'start of month', '-1 day')")
    assert allowance.spent(conn, "claude", "month") == 0


def test_the_cost_of_a_call():
    assert cost(Usage("claude-opus-5-5", input_tokens=1_000_000)) == 4.0
    assert cost(Usage("claude-opus-5-5", output_tokens=1_000_000)) == 20.0
    assert cost(Usage("claude-opus-5-5", cache_read_tokens=1_000_000)) == pytest.approx(0.2)
    assert cost(Usage("claude-opus-5-5", cache_write_tokens=1_000_000)) == 5.0
    assert cost(Usage("claude-sonnet-5-5", 1000, 1000)) == pytest.approx(0.012)
    assert cost(Usage("llama", 1000, 1000)) is None
    assert cost(Usage(None)) is None


MILLION = 1_000_000


def test_openai_and_google_calls_cost_their_own_prices():
    sol, k100 = "gpt-6.1-sol", 100_000  # under OpenAI's long prompts
    assert cost(Usage(sol, k100, k100)) == pytest.approx(0.2 + 1.0)
    assert cost(Usage(sol, cache_read_tokens=k100)) == pytest.approx(0.01)
    assert cost(Usage(sol, cache_write_tokens=k100)) == pytest.approx(0.25)
    # "-" in OpenAI's table: no discount for a cached read, and a write costs as input
    assert cost(Usage("gpt-5.5-pro", cache_read_tokens=k100)) == pytest.approx(3.0)
    assert cost(Usage("gpt-5.4", cache_write_tokens=k100)) == pytest.approx(0.25)
    assert cost(Usage("text-embedding-3-small", MILLION)) == pytest.approx(0.02)
    assert cost(Usage("gemini-2.5-flash", MILLION, MILLION, MILLION)) == pytest.approx(2.83)
    assert cost(Usage("gemini-embedding-2", MILLION)) == pytest.approx(0.20)


def test_a_long_prompt_is_priced_as_long_for_the_whole_call():
    # OpenAI over 272K tokens sent, cache included
    assert cost(Usage("gpt-6.1-sol", 272_000)) == pytest.approx(0.544)
    assert cost(Usage("gpt-6.1-sol", 200_000, 1000, 72_001)) == pytest.approx(
        (200_000 * 4.0 + 1000 * 15.0 + 72_001 * 0.20) / MILLION
    )
    # Gemini Pro over 200K; Claude has no long price
    assert cost(Usage("gemini-2.5-pro", 200_001)) == pytest.approx(200_001 * 2.5 / MILLION)
    assert cost(Usage("claude-opus-5-5", 900_000)) == pytest.approx(3.6)


def test_gemini_flash_costs_twice_as_much_from_2027():
    flash = Usage("gemini-3.8-flash", MILLION, MILLION)
    assert cost(flash, date(2026, 12, 31)) == 4.5
    assert cost(flash, date(2027, 1, 1)) == 9.0


def test_a_dated_snapshot_costs_as_its_model():
    assert cost(Usage("gpt-6.1-sol-2026-09-15", 100_000)) == pytest.approx(0.2)
    assert cost(Usage("claude-haiku-4-5-20251001", MILLION)) == 1.0
    assert cost(Usage("gemini-2.5-flash-001", MILLION)) == pytest.approx(0.30)
    # Not a snapshot: another model, not priced as one it starts like
    assert cost(Usage("claude-opus-5-6", MILLION)) is None
    assert cost(Usage("gemini-2.5-flash-lite", MILLION)) == pytest.approx(0.10)
