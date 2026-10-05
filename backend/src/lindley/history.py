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
- page_rotation: the turn a person gave a page
- page_mirror: whether a person turned a page round left to right (a mirror image)
- page_text: which reading of a page is in use, and whether a person checked it
- document_fields: a document's name, type, date and folder
- new_folder: a folder made by the decision, removed again while it's still empty
- folder_fields: a folder's name
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


# A document `d` a person has worked on: they removed it, gave its name, type, date or folder,
# or moved pages into or out of it. Lindley suggests changes to such a document, but never makes
# them itself.
WORKED_ON_SQL = """EXISTS (
    SELECT 1 FROM history h WHERE h.actor = 'user' AND (
        (h.target_type IN ('document', 'document_fields') AND h.target_id = d.id)
        OR (h.target_type = 'page' AND d.id IN (
            json_extract(h.before, '$.document_id'), json_extract(h.after, '$.document_id')))))"""


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
    try:
        with conn:  # all or nothing: an UndoError part-way rolls everything back
            for r in rows:
                before = json.loads(r["before"]) if r["before"] else None
                after = json.loads(r["after"]) if r["after"] else None
                _UNDO[r["target_type"]](conn, r["target_id"], before, after)
                done.actions.append(r["action"])
                if r["target_type"] == "page" and r["target_id"] not in done.pages:
                    done.pages.append(r["target_id"])
            log(conn, new_batch(conn), "undo", "history_batch", batch, None, {"rows": len(rows)})
    except sqlite3.IntegrityError as e:  # something it would put back is taken by now
        raise UndoError("Things have changed since, so this can't be undone") from e
    return done


def _refuse_if_complete(conn: sqlite3.Connection, doc_ids: object) -> None:
    """A completed document is changed only once it's reopened, by undo too."""
    for doc_id in {d for d in doc_ids if d is not None}:
        r = conn.execute(
            "SELECT name FROM documents WHERE id = ? AND status = 'complete'", (doc_id,)
        ).fetchone()
        if r:
            raise UndoError(f"“{r[0]}” is completed. Reopen it to undo this.")


def _document_of(conn: sqlite3.Connection, page_id: int) -> int | None:
    r = conn.execute("SELECT document_id FROM pages WHERE id = ?", (page_id,)).fetchone()
    return r[0] if r else None


def _undo_page(conn: sqlite3.Connection, page_id: int, before: dict, after: dict) -> None:
    now = place(conn, page_id)
    if now != after:
        raise UndoError(f"Page {page_id} has moved since, so this can't be undone")
    _refuse_if_complete(conn, (before["document_id"], after["document_id"]))
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
    for s in before.get("suggestions", []):  # hints: a new id, as theirs may be taken by now
        _insert(conn, "suggestions", {k: v for k, v in s.items() if k != "id"})


def _undo_new_document(conn: sqlite3.Connection, doc_id: int, _before: None, _after: dict) -> None:
    if conn.execute("SELECT 1 FROM pages WHERE document_id = ?", (doc_id,)).fetchone():
        raise UndoError(f"Document {doc_id} has had pages added since, so this can't be undone")
    if conn.execute("SELECT 1 FROM exports WHERE document_id = ?", (doc_id,)).fetchone():
        raise UndoError(f"Document {doc_id} has been exported since, so this can't be undone")
    conn.execute("DELETE FROM suggestions WHERE document_id = ?", (doc_id,))
    conn.execute("DELETE FROM facts WHERE document_id = ?", (doc_id,))
    conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))


def _undo_rotation(conn: sqlite3.Connection, page_id: int, before: dict, after: dict) -> None:
    now = conn.execute("SELECT user_rotation FROM pages WHERE id = ?", (page_id,)).fetchone()
    if now is None or now[0] != after["user_rotation"]:
        raise UndoError(f"Page {page_id} has been turned again since, so this can't be undone")
    _refuse_if_complete(conn, (_document_of(conn, page_id),))
    conn.execute(
        "UPDATE pages SET user_rotation = ?, updated_at = datetime('now') WHERE id = ?",
        (before["user_rotation"], page_id),
    )


def _undo_mirror(conn: sqlite3.Connection, page_id: int, before: dict, after: dict) -> None:
    now = conn.execute("SELECT user_mirror FROM pages WHERE id = ?", (page_id,)).fetchone()
    if now is None or now[0] != after["user_mirror"]:
        raise UndoError(f"Page {page_id} has been flipped again since, so this can't be undone")
    _refuse_if_complete(conn, (_document_of(conn, page_id),))
    conn.execute(
        "UPDATE pages SET user_mirror = ?, updated_at = datetime('now') WHERE id = ?",
        (before["user_mirror"], page_id),
    )


def _undo_text(conn: sqlite3.Connection, page_id: int, before: dict, after: dict) -> None:
    now = conn.execute(
        "SELECT id, confirmed_at FROM transcriptions WHERE page_id = ? AND is_current = 1",
        (page_id,),
    ).fetchone()
    if now is None or [now[0], now[1]] != [after["current"], after["confirmed_at"]]:
        raise UndoError(f"Page {page_id}'s text has changed since, so this can't be undone")
    if before["current"] != after["current"]:  # the correction stays, no longer in use
        conn.execute("UPDATE transcriptions SET is_current = 0 WHERE id = ?", (after["current"],))
        if before["current"] is not None:
            conn.execute(
                "UPDATE transcriptions SET is_current = 1 WHERE id = ?", (before["current"],)
            )
    if before["current"] is not None:
        conn.execute(
            "UPDATE transcriptions SET confirmed_at = ? WHERE id = ?",
            (before["confirmed_at"], before["current"]),
        )


def _undo_document_fields(conn: sqlite3.Connection, doc_id: int, before: dict, after: dict) -> None:
    row = conn.execute(
        f"SELECT {', '.join(after)} FROM documents WHERE id = ?", (doc_id,)
    ).fetchone()
    if row is None or dict(zip(after, row, strict=True)) != after:
        raise UndoError(f"Document {doc_id} has changed since, so this can't be undone")
    if {k for k in after if before.get(k) != after[k]} - {"folder_id"}:  # filing it is fine
        _refuse_if_complete(conn, (doc_id,))
    sets = ", ".join(f"{k} = ?" for k in before)
    conn.execute(
        f"UPDATE documents SET {sets}, updated_at = datetime('now') WHERE id = ?",
        (*before.values(), doc_id),
    )


def _undo_new_folder(conn: sqlite3.Connection, folder_id: int, _before: None, _after: dict) -> None:
    if conn.execute(
        "SELECT 1 FROM documents WHERE folder_id = ? UNION SELECT 1 FROM folders"
        " WHERE parent_id = ?",
        (folder_id, folder_id),
    ).fetchone():
        raise UndoError(f"Folder {folder_id} has had things put in it since, so it stays")
    conn.execute("DELETE FROM folders WHERE id = ?", (folder_id,))


def _undo_folder_fields(
    conn: sqlite3.Connection, folder_id: int, before: dict, after: dict
) -> None:
    row = conn.execute("SELECT name FROM folders WHERE id = ?", (folder_id,)).fetchone()
    if row is None or row[0] != after["name"]:
        raise UndoError(f"Folder {folder_id} has been renamed since, so this can't be undone")
    conn.execute("UPDATE folders SET name = ? WHERE id = ?", (before["name"], folder_id))


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
    "page_rotation": _undo_rotation,
    "page_mirror": _undo_mirror,
    "page_text": _undo_text,
    "document_fields": _undo_document_fields,
    "new_folder": _undo_new_folder,
    "folder_fields": _undo_folder_fields,
}
