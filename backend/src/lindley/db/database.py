"""SQLite connection helpers, schema initialisation and migrations."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path

SCHEMA_VERSION = 11

# Numbered migrations from one version to the next: {2: "ALTER TABLE ...", ...}, or a function
# given the connection.
# schema.sql always describes the latest version, for new databases.
MIGRATIONS: dict[int, str | Callable[[sqlite3.Connection], None]] = {
    2: "ALTER TABLE documents ADD COLUMN reasons TEXT;",
    3: "",  # new tables only (duplicates, duplicate_checks, text_sketch): schema.sql adds them
    4: "ALTER TABLE history ADD COLUMN batch INTEGER;",
    5: "",  # new table only (ai_calls)
    6: "",  # new table only (ai_answers)
    7: "",  # new table only (learned_weights)
    8: "",  # new table only (needs_ai)
    9: "",  # new table only (seen_files), and an index
    # Pages read before a poorly read page was tried upside down: each queued once, for the
    # watcher to check (Pipeline.check_upside_down). Pages read since were tried as they were read.
    10: """
        INSERT INTO intake_steps (scan_id, page_id, step, status, error)
        SELECT p.scan_id, p.id, 'ocr', 'queued', 'To be read turned over, in case it''s upside down'
        FROM pages p JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1
        WHERE t.source = 'tesseract' AND t.confirmed_at IS NULL AND coalesce(t.confidence, 0) < 100
          AND p.detected_rotation = 0 AND p.user_rotation = 0;
    """,
    11: lambda conn: _ai_call_usage(conn),
}

# Tables from the pre-release placeholder schema (user_version 0). They never held real data.
_PLACEHOLDER_TABLES = ("pages_fts", "jobs", "pages", "documents")


def connect(db_path: Path, *, any_thread: bool = False) -> sqlite3.Connection:
    """`any_thread`: the connection may be used from another thread than the one that made it
    (one request in the API's threadpool). It must still only be used by one at a time."""
    # The watcher, API requests and scripts share the database: a writer waits up to `timeout`
    # seconds for another to finish (SQLite's busy timeout) rather than failing at once.
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=not any_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _ai_call_usage(conn: sqlite3.Connection) -> None:
    """v11: what each AI call used and cost. A database from before v5 has no ai_calls yet:
    schema.sql makes it whole."""
    if "ai_calls" not in _tables(conn):
        return
    has = {r[1] for r in conn.execute("PRAGMA table_info(ai_calls)")}
    for column in (
        "model TEXT",
        "input_tokens INTEGER",
        "output_tokens INTEGER",
        "cache_read_tokens INTEGER",
        "cache_write_tokens INTEGER",
        "cost_usd REAL",
    ):
        if column.split()[0] not in has:
            conn.execute(f"ALTER TABLE ai_calls ADD COLUMN {column}")


def _drop_placeholder(conn: sqlite3.Connection) -> None:
    """Remove the empty placeholder tables so the real schema can be created."""
    existing = _tables(conn)
    for table in ("jobs", "pages", "documents"):
        if table in existing and conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
            raise RuntimeError(
                f"The database has data in the old '{table}' table. "
                "Move it aside and start Lindley again to create a new one."
            )
    for table in _PLACEHOLDER_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")


def init_db(db_path: Path) -> None:
    """Create or upgrade the database to SCHEMA_VERSION (safe to call repeatedly)."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    conn = connect(db_path)
    try:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"The database is from a newer Lindley (schema {version}); "
                f"this version understands up to {SCHEMA_VERSION}."
            )
        if version == 0 and "jobs" in _tables(conn):
            _drop_placeholder(conn)
        # A new database is built from schema.sql; an existing one is upgraded step by step.
        for target in range(version + 1 if version else SCHEMA_VERSION + 1, SCHEMA_VERSION + 1):
            step = MIGRATIONS[target]
            step(conn) if callable(step) else conn.executescript(step)
        conn.executescript(schema)  # idempotent: creates a new database, fills in anything missing
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.commit()
    finally:
        conn.close()
