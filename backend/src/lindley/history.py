"""A person's changes, recorded so they can be undone.

Each decision is one batch of `history` rows, each with the target's state before and after.
Undo replays a batch backwards, putting each target back as it was. It only does so if nothing
has changed since: every target must still look the way the decision left it. If a page has
been moved again, say, the undo is refused rather than guessed at.

Targets undo knows how to put back:
- page: its place (document_id, position, set_aside_at)
- duplicate_set: the status of each duplicate pair in it
- document: a document removed because it had no pages left (and its open suggestions)
- new_document: a document made by the decision, removed again once its pages have gone back
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

PLACE = ("document_id", "position", "set_aside_at")


class UndoError(Exception):
    """Nothing to undo, or something changed since, so it can't be undone safely."""


@dataclass
class Undone:
    batch: int
    actions: list[str] = field(default_factory=list)  # in the order they were undone
    pages: list[int] = field(default_factory=list)  # pages put back where they were


def new_batch(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT coalesce(max(batch), 0) + 1 FROM history").fetchone()[0]


def log(
    conn: sqlite3.Connection,
    batch: int | None,
    action: str,
    target_type: str,
    target_id: int,
    before: object,
    after: object,
    actor: str = "user",
) -> None:
    """Write one history row (the caller commits)."""
    conn.execute(
        "INSERT INTO history (actor, action, target_type, target_id, before, after, batch)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            actor,
            action,
            target_type,
            target_id,
            json.dumps(before) if before is not None else None,
            json.dumps(after) if after is not None else None,
            batch,
        ),
    )


def place(conn: sqlite3.Connection, page_id: int) -> dict:
    """Where a page is: the state a page's history row records."""
    r = conn.execute(
        "SELECT document_id, position, set_aside_at FROM pages WHERE id = ?", (page_id,)
    ).fetchone()
    return dict(zip(PLACE, r, strict=True))


def latest(conn: sqlite3.Connection) -> int | None:
    """The most recent batch that can still be undone."""
    r = conn.execute(
        "SELECT max(batch) FROM history WHERE batch IS NOT NULL AND action != 'undo'"
        " AND batch NOT IN (SELECT target_id FROM history WHERE action = 'undo')"
    ).fetchone()
    return r[0]


def undo(conn: sqlite3.Connection, batch: int | None = None) -> Undone:
    """Undo one batch (the latest, if none is given), all or nothing."""
    if batch is None:
        batch = latest(conn)
        if batch is None:
            raise UndoError("There's nothing to undo")
    rows = conn.execute(
        "SELECT * FROM history WHERE batch = ? ORDER BY id DESC", (batch,)
    ).fetchall()
    if not rows or any(r["action"] == "undo" for r in rows):
        raise UndoError(f"There's no change {batch} to undo")
    if conn.execute(
        "SELECT 1 FROM history WHERE action = 'undo' AND target_id = ?", (batch,)
    ).fetchone():
        raise UndoError("That change has already been undone")
    done = Undone(batch)
    with conn:  # all or nothing: an UndoError part-way rolls everything back
        for r in rows:
            before = json.loads(r["before"]) if r["before"] else None
            after = json.loads(r["after"]) if r["after"] else None
            _UNDO[r["target_type"]](conn, r["target_id"], before, after)
            done.actions.append(r["action"])
            if r["target_type"] == "page" and r["target_id"] not in done.pages:
                done.pages.append(r["target_id"])
        log(conn, new_batch(conn), "undo", "history_batch", batch, None, {"rows": len(rows)})
    return done


def _undo_page(conn: sqlite3.Connection, page_id: int, before: dict, after: dict) -> None:
    now = place(conn, page_id)
    if now != after:
        raise UndoError(f"Page {page_id} has moved since, so this can't be undone")
    conn.execute(
        "UPDATE pages SET document_id = ?, position = ?, set_aside_at = ?,"
        " updated_at = datetime('now') WHERE id = ?",
        (*(before[k] for k in PLACE), page_id),
    )


def _undo_duplicate_set(conn: sqlite3.Connection, _set_id: int, before: list, after: list) -> None:
    for was, became in zip(before, after, strict=True):
        status = conn.execute("SELECT status FROM duplicates WHERE id = ?", (was["id"],)).fetchone()
        if status is None or status[0] != became["status"]:
            raise UndoError("Those duplicates have been decided again since")
        conn.execute(
            "UPDATE duplicates SET status = ?, kept_page = ?, resolved_at = ? WHERE id = ?",
            (was["status"], was["kept_page"], was["resolved_at"], was["id"]),
        )


def _undo_document(conn: sqlite3.Connection, doc_id: int, before: dict, _after: None) -> None:
    if conn.execute("SELECT 1 FROM documents WHERE id = ?", (doc_id,)).fetchone():
        raise UndoError(f"Document {doc_id} exists again, so this can't be undone")
    _insert(conn, "documents", before["document"])
    for s in before.get("suggestions", []):
        _insert(conn, "suggestions", s)


def _undo_new_document(conn: sqlite3.Connection, doc_id: int, _before: None, _after: dict) -> None:
    if conn.execute("SELECT 1 FROM pages WHERE document_id = ?", (doc_id,)).fetchone():
        raise UndoError(f"Document {doc_id} has had pages added since, so this can't be undone")
    if conn.execute("SELECT 1 FROM exports WHERE document_id = ?", (doc_id,)).fetchone():
        raise UndoError(f"Document {doc_id} has been exported since, so this can't be undone")
    conn.execute("DELETE FROM suggestions WHERE document_id = ?", (doc_id,))
    conn.execute("DELETE FROM facts WHERE document_id = ?", (doc_id,))
    conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))


def _insert(conn: sqlite3.Connection, table: str, row: dict) -> None:
    cols = ", ".join(row)
    conn.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})", list(row.values())
    )


_UNDO = {
    "page": _undo_page,
    "duplicate_set": _undo_duplicate_set,
    "document": _undo_document,
    "new_document": _undo_new_document,
}
