"""When Lindley may call an AI on its own, and a record of every call it makes.

Each provider's `allow` says whether background work (reading hard pages, sorting pages into
documents) is sent as soon as there is some, or waits for a person's OK. With "auto", its
`daily_limit` and `monthly_limit` cap the calls made on its own each day and each calendar
month; past either, work waits for an OK again. Only calls that worked count: a failed call
returns nothing. Calls a person OKed or asked for are recorded too, but never count against the
limits. Each call's tokens and estimated cost are kept too, when the AI says what it used.
When the last FAILING_AFTER calls made on its own all failed, the AI looks down, and
Lindley waits FAILING_WAIT_MIN minutes before trying it on its own again.
(Calls a minute and at once are in throttle.py.)
"""

from __future__ import annotations

import sqlite3
from itertools import zip_longest

from lindley.config import ProviderConfig, Settings
from lindley.providers.base import Usage
from lindley.providers.prices import cost

FAILING_AFTER = 3  # calls made on its own that failed in a row: the AI looks down
FAILING_WAIT_MIN = 15  # then Lindley waits this long before calling it on its own again


def provider_config(settings: Settings, name: str | None) -> ProviderConfig | None:
    return settings.ai.providers.get(name) if name else None


def calls_today(
    conn: sqlite3.Connection, provider: str, automatic: bool = True, worked: bool = False
) -> int:
    """Calls to a provider today (this computer's day), made on Lindley's own, or OKed.
    `worked`: only the ones that worked, as the limits count them."""
    return conn.execute(
        "SELECT COUNT(*) FROM ai_calls WHERE provider = ? AND automatic = ? AND ok >= ?"
        " AND date(at, 'localtime') = date('now', 'localtime')",
        (provider, int(automatic), int(worked)),
    ).fetchone()[0]


def calls_this_month(
    conn: sqlite3.Connection, provider: str, automatic: bool = True, worked: bool = False
) -> int:
    """Calls to a provider this calendar month (this computer's), on Lindley's own, or OKed."""
    return conn.execute(
        "SELECT COUNT(*) FROM ai_calls WHERE provider = ? AND automatic = ? AND ok >= ?"
        " AND strftime('%Y-%m', at, 'localtime') = strftime('%Y-%m', 'now', 'localtime')",
        (provider, int(automatic), int(worked)),
    ).fetchone()[0]


def failing(conn: sqlite3.Connection, name: str | None) -> bool:
    """The last FAILING_AFTER calls made on its own all failed, the latest less than
    FAILING_WAIT_MIN minutes ago: Lindley doesn't call it on its own for now."""
    rows = conn.execute(
        "SELECT ok, at > datetime('now', ?) FROM ai_calls WHERE provider = ? AND automatic = 1"
        " ORDER BY id DESC LIMIT ?",
        (f"-{FAILING_WAIT_MIN} minutes", name or "unnamed", FAILING_AFTER),
    ).fetchall()
    return len(rows) == FAILING_AFTER and not any(ok for ok, _ in rows) and bool(rows[0][1])


def automatic_left(conn: sqlite3.Connection, settings: Settings, name: str | None) -> int | None:
    """How many calls Lindley may still make to this provider on its own now: what's left of
    today's limit or this month's, whichever is less. 0 when it must ask, None: no limit."""
    cfg = provider_config(settings, name)
    if cfg is None or cfg.allow != "auto":
        return 0
    left = []
    if cfg.daily_limit is not None:
        left.append(cfg.daily_limit - calls_today(conn, name, worked=True))
    if cfg.monthly_limit is not None:
        left.append(cfg.monthly_limit - calls_this_month(conn, name, worked=True))
    return max(0, min(left)) if left else None


def may_call(conn: sqlite3.Connection, settings: Settings, name: str | None) -> bool:
    left = automatic_left(conn, settings, name)
    return left is None or left > 0


def why_waiting(conn: sqlite3.Connection, settings: Settings, name: str | None) -> str:
    """Why work for this provider is waiting, in words for a person."""
    cfg = provider_config(settings, name)
    if cfg is not None and cfg.allow == "auto":
        if failing(conn, name):
            return (
                f"Waiting: the last {FAILING_AFTER} calls failed, so Lindley tries again on its"
                f" own in {FAILING_WAIT_MIN} minutes"
            )
        if cfg.monthly_limit and calls_this_month(conn, name, worked=True) >= cfg.monthly_limit:
            return (
                f"Waiting for your OK: this month's {cfg.monthly_limit} automatic calls are used up"
            )
        if cfg.daily_limit and calls_today(conn, name, worked=True) >= cfg.daily_limit:
            return f"Waiting for your OK: today's {cfg.daily_limit} automatic calls are used up"
    return "Waiting for you to OK the vision model"


def record(
    conn: sqlite3.Connection,
    provider: str | None,
    purpose: str,
    automatic: bool,
    page_id: int | None = None,
    ok: bool = True,
    count: int = 1,
    used: list[Usage] | None = None,
) -> None:
    """Record calls made (the caller commits). `used`: what each call used, as the provider
    reported it (throttle.metered). One reported beyond `count` was a call that failed after
    the AI had answered, and was charged: it's recorded as a failed call."""
    rows = []
    for i, usage in enumerate(zip_longest(range(count), used or [])):
        u = usage[1]
        tokens = (
            (u.model, u.input_tokens, u.output_tokens, u.cache_read_tokens, u.cache_write_tokens)
            if u
            else (None,) * 5
        )
        worked = ok and i < count
        row = (provider or "unnamed", purpose, int(automatic), page_id, int(worked))
        rows.append((*row, *tokens, cost(u) if u else None))
    conn.executemany(
        "INSERT INTO ai_calls (provider, purpose, automatic, page_id, ok, model, input_tokens,"
        " output_tokens, cache_read_tokens, cache_write_tokens, cost_usd)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def spent(conn: sqlite3.Connection, provider: str, period: str = "day") -> float:
    """What a provider's calls cost (estimated, US dollars) today or this calendar month,
    whoever made them."""
    fmt = "%Y-%m-%d" if period == "day" else "%Y-%m"
    return conn.execute(
        "SELECT coalesce(sum(cost_usd), 0) FROM ai_calls WHERE provider = ?"
        " AND strftime(?, at, 'localtime') = strftime(?, 'now', 'localtime')",
        (provider, fmt, fmt),
    ).fetchone()[0]
