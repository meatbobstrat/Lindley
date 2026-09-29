"""SQLite connection helpers and schema initialisation."""

from __future__ import annotations

import sqlite3
from importlib.resources import files
from pathlib import Path


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(db_path: Path) -> None:
    """Create the database file and apply schema.sql (safe to call repeatedly)."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    with connect(db_path) as conn:
        conn.executescript(schema)
    conn.close()
