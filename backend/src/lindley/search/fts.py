"""SQLite FTS5 search over the text of every page, as it reads now (the reading in use).

Each word of the query must appear on the page; the last one may be the start of a word, so
results come as a person types. Semantic (embedding) search will sit alongside this.
"""

from __future__ import annotations

import re
import sqlite3

# Marks around the words found, in a snippet; the UI turns them into highlights.
HIT_START, HIT_END = "\x02", "\x03"


def fts_query(query: str) -> str | None:
    """A person's words as an FTS5 query: each word quoted, so punctuation and FTS5's own
    syntax are taken as text; the last may be a prefix. None: no words to look for."""
    words = re.findall(r"\w+", query)
    if not words:
        return None
    quoted = [f'"{w}"' for w in words]
    quoted[-1] += "*"
    return " ".join(quoted)


def search_pages(conn: sqlite3.Connection, query: str, limit: int = 50) -> list[sqlite3.Row]:
    """Pages whose text has every word, best match first: page_id, snippet (with HIT_START
    and HIT_END around the words found), file, document_id, document_name, position,
    set_aside_at."""
    q = fts_query(query)
    if q is None:
        return []
    return conn.execute(
        "SELECT t.page_id,"
        " snippet(transcriptions_fts, 0, char(2), char(3), '…', 24) AS snippet,"
        " s.original_name AS file, p.document_id, d.name AS document_name, p.position,"
        " p.set_aside_at"
        " FROM transcriptions_fts f"
        " JOIN transcriptions t ON t.id = f.rowid AND t.is_current = 1"
        " JOIN pages p ON p.id = t.page_id"
        " JOIN scans s ON s.id = p.scan_id"
        " LEFT JOIN documents d ON d.id = p.document_id"
        " WHERE transcriptions_fts MATCH ? ORDER BY f.rank LIMIT ?",
        (q, limit),
    ).fetchall()
