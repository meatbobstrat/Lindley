"""When Lindley may call an AI on its own, and a record of every call it makes.

Each provider's `allow` says whether background work (reading hard pages, sorting pages into
documents) is sent as soon as there is some, or waits for a person's OK. With "auto", its
`daily_limit` caps the calls made on its own each day; past it, work waits for an OK again.
Calls a person OKed or asked for are recorded too, but never count against the limit.
"""

from __future__ import annotations

import sqlite3

from lindley.config import ProviderConfig, Settings


def provider_config(settings: Settings, name: str | None) -> ProviderConfig | None:
    return settings.ai.providers.get(name) if name else None


def calls_today(conn: sqlite3.Connection, provider: str, automatic: bool = True) -> int:
    """Calls to a provider today (this computer's day), made on Lindley's own, or OKed."""
    return conn.execute(
        "SELECT COUNT(*) FROM ai_calls WHERE provider = ? AND automatic = ?"
        " AND date(at, 'localtime') = date('now', 'localtime')",
        (provider, int(automatic)),
    ).fetchone()[0]


def automatic_left(conn: sqlite3.Connection, settings: Settings, name: str | None) -> int | None:
    """How many calls Lindley may still make to this provider on its own today: 0 when it must
    ask, None when there's no limit."""
    cfg = provider_config(settings, name)
    if cfg is None or cfg.allow != "auto":
        return 0
    if cfg.daily_limit is None:
        return None
    return max(0, cfg.daily_limit - calls_today(conn, name))


def may_call(conn: sqlite3.Connection, settings: Settings, name: str | None) -> bool:
    left = automatic_left(conn, settings, name)
    return left is None or left > 0


def why_waiting(settings: Settings, name: str | None) -> str:
    """Why work for this provider is waiting, in words for a person."""
    cfg = provider_config(settings, name)
    if cfg is not None and cfg.allow == "auto" and cfg.daily_limit:
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
) -> None:
    """Record calls made (the caller commits)."""
    conn.executemany(
        "INSERT INTO ai_calls (provider, purpose, automatic, page_id, ok) VALUES (?, ?, ?, ?, ?)",
        [(provider or "unnamed", purpose, int(automatic), page_id, int(ok))] * count,
    )
