"""SQLite FTS5 search over page text.

Stub: implemented in the backend phase, once the UI decides result shape.
Semantic (embedding) search will sit alongside this, using an EmbeddingProvider.
"""

from __future__ import annotations

import sqlite3


def search_pages(conn: sqlite3.Connection, query: str, limit: int = 50) -> list[sqlite3.Row]:
    raise NotImplementedError
