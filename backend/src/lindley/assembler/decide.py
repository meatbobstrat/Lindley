"""A person's answer to the assembler's hints: "Do these go together?", "Add to …?", "Set aside?".

A person is asked before any AI, since an answer costs nothing and is right. Accepting a hint
makes the change it proposes; it's one batch in `history`, so lindley.history.undo can take it
back. A document made from an accepted grouping counts as the person's own (origin 'user'), so
Lindley only suggests changes to it from then on. Dismissing a hint means it's never made again,
and the AI isn't asked about those pages on its own (see lindley.assembler.run).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from lindley import history
from lindley.assembler.apply import HINT_KINDS


@dataclass
class Accepted:
    batch: int  # what lindley.history.undo takes to undo it
    document_id: int | None = None
    pages: tuple[int, ...] = ()


def _open(conn: sqlite3.Connection, suggestion_id: int) -> sqlite3.Row:
    s = conn.execute(
        f"SELECT * FROM suggestions WHERE id = ? AND status = 'open' AND kind IN {HINT_KINDS}",
        (suggestion_id,),
    ).fetchone()
    if s is None:
        raise LookupError("There's no open suggestion with that id")
    return s


def _pages(s: sqlite3.Row) -> list[int]:
    return json.loads(s["payload"] or "{}").get("pages") or [s["page_id"]]


def _in_inbox(conn: sqlite3.Connection, pages: list[int]) -> None:
    for pid in pages:
        r = conn.execute(
            "SELECT document_id, set_aside_at FROM pages WHERE id = ?", (pid,)
        ).fetchone()
        if r is None or r["document_id"] is not None or r["set_aside_at"] is not None:
            raise ValueError("Some of these pages have been moved since, so this is out of date")


def _move(conn: sqlite3.Connection, batch: int, action: str, pid: int, sql: str, args) -> None:
    before = history.place(conn, pid)
    conn.execute(sql + ", updated_at = datetime('now') WHERE id = ?", (*args, pid))
    history.log(conn, batch, action, "page", pid, before, history.place(conn, pid))


def accept(conn: sqlite3.Connection, suggestion_id: int, batch: int | None = None) -> Accepted:
    """Make the change a hint proposes: in `batch` if given (several accepted as one change to
    undo), else a batch of its own."""
    with history.deciding(conn):
        s = _open(conn, suggestion_id)
        pages = _pages(s)
        _in_inbox(conn, pages)
        batch = batch or history.new_batch(conn)
        doc = s["document_id"]
        if s["kind"] == "group_pages":
            doc = _make_document(conn, batch, s, pages)
        elif s["kind"] == "add_to_document":
            _add(conn, batch, s, pages)
        else:  # set_aside
            for pid in pages:
                sql = "UPDATE pages SET set_aside_at = datetime('now')"
                _move(conn, batch, "accept_set_aside", pid, sql, ())
        marks = ",".join("?" * len(pages))
        conn.execute(
            "UPDATE suggestions SET status = 'accepted', resolved_at = datetime('now')"
            f" WHERE status = 'open' AND kind = ? AND document_id IS ? AND page_id IN ({marks})",
            (s["kind"], s["document_id"], *pages),
        )
        # Other hints about these pages no longer apply; the next run makes new ones if need be
        conn.execute(
            f"DELETE FROM suggestions WHERE status = 'open' AND kind IN {HINT_KINDS}"
            f" AND page_id IN ({marks})",
            pages,
        )
    return Accepted(batch, doc, tuple(pages))


def _make_document(conn: sqlite3.Connection, batch: int, s: sqlite3.Row, pages: list[int]) -> int:
    p = json.loads(s["payload"] or "{}")
    doc = conn.execute(
        "INSERT INTO documents (name, name_source, origin, status, doc_type, doc_date,"
        " date_source, grouping_confidence, reasons)"
        " VALUES (?, 'lindley', 'user', 'progress', ?, ?, ?, ?, ?)",
        (
            p.get("name") or "Pages put together",
            p.get("type"),
            p.get("date"),
            "lindley" if p.get("date") else None,
            s["confidence"],
            s["reasons"],
        ),
    ).lastrowid
    history.log(conn, batch, "accept_grouping", "new_document", doc, None, {"pages": pages})
    for i, pid in enumerate(pages, 1):
        sql = "UPDATE pages SET document_id = ?, position = ?"
        _move(conn, batch, "accept_grouping", pid, sql, (doc, i))
    return doc


def _add(conn: sqlite3.Connection, batch: int, s: sqlite3.Row, pages: list[int]) -> None:
    doc = s["document_id"]
    if not conn.execute(
        "SELECT 1 FROM documents WHERE id = ? AND status = 'progress'", (doc,)
    ).fetchone():
        raise ValueError("That document is finished or gone, so nothing can be added to it")
    at_start = json.loads(s["payload"] or "{}").get("at") == "start"
    sql = "UPDATE pages SET document_id = ?, position = ?"
    if at_start:  # the pages there move down to make room, each recorded so undo can move it back
        there = conn.execute(
            "SELECT id, position FROM pages WHERE document_id = ? ORDER BY position DESC", (doc,)
        ).fetchall()
        for pid, pos in there:
            _move(conn, batch, "accept_addition", pid, sql, (doc, pos + len(pages)))
        # the first places, whether the document counts from 0 (a person's) or 1 (Lindley's)
        first = there[-1][1] if there else 1
    else:
        first = (
            1
            + conn.execute(
                "SELECT coalesce(max(position), 0) FROM pages WHERE document_id = ?", (doc,)
            ).fetchone()[0]
        )
    for i, pid in enumerate(pages):
        _move(conn, batch, "accept_addition", pid, sql, (doc, first + i))


def dismiss(conn: sqlite3.Connection, suggestion_id: int) -> None:
    """Never make this suggestion again. For pages hinted at one document together, that's the
    hint for each of them."""
    with history.deciding(conn):
        s = _open(conn, suggestion_id)
        pages = _pages(s) if s["kind"] == "add_to_document" else [s["page_id"]]
        marks = ",".join("?" * len(pages))
        conn.execute(
            "UPDATE suggestions SET status = 'dismissed', resolved_at = datetime('now')"
            f" WHERE status = 'open' AND kind = ? AND document_id IS ? AND page_id IN ({marks})",
            (s["kind"], s["document_id"], *pages),
        )
