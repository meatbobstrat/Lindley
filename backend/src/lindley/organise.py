"""A person organising their archive: moving, ordering and turning pages, starting and naming
documents, folders, and checking what Lindley read.

Every change is one batch in `history`, with each target's state before and after, so
lindley.history.undo can put it all back. Nothing is destroyed: a page set aside is kept, a
correction is a new reading beside the old one, and a document is only removed once it has no
pages left. A completed (exported) document is left as it is until it's reopened.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Literal

from lindley import history

DOCUMENT_FIELDS = ("name", "name_source", "doc_type", "doc_date", "date_source", "folder_id")
Where = Literal["document", "inbox", "aside"]


@dataclass
class Change:
    """What a change did. `batch` is what lindley.history.undo takes to undo it."""

    batch: int | None  # None: it changed nothing, so there's nothing to undo
    document_id: int | None = None  # the document made, or moved into
    folder_id: int | None = None  # the folder made
    removed: list[int] = field(default_factory=list)  # documents left with no pages


# ---------------------------------------------------------------- Pieces (the caller commits)


def move_page(
    conn: sqlite3.Connection,
    batch: int,
    page_id: int,
    document_id: int | None,
    position: int | None,
    *,
    aside: bool,
    action: str,
) -> None:
    """Put a page in a document at `position`, in the Inbox, or Set aside, recording it."""
    before = history.place(conn, page_id)
    conn.execute(
        "UPDATE pages SET document_id = ?, position = ?,"
        " set_aside_at = CASE WHEN ? THEN coalesce(set_aside_at, datetime('now')) END,"
        " updated_at = datetime('now') WHERE id = ?",
        (document_id, position, aside, page_id),
    )
    history.log(conn, batch, action, "page", page_id, before, history.place(conn, page_id))


def close_gaps(conn: sqlite3.Connection, batch: int, doc_id: int) -> None:
    """Number a document's pages 0, 1, 2... again, recording each page that moves up."""
    rows = conn.execute(
        "SELECT id, position FROM pages WHERE document_id = ? ORDER BY position", (doc_id,)
    ).fetchall()
    for i, r in enumerate(rows):
        if r["position"] != i:
            before = history.place(conn, r["id"])
            conn.execute("UPDATE pages SET position = ? WHERE id = ?", (i, r["id"]))
            after = history.place(conn, r["id"])
            history.log(conn, batch, "close_gap", "page", r["id"], before, after)


def remove_if_empty(conn: sqlite3.Connection, batch: int, doc_id: int) -> bool:
    """A document left with no pages goes, unless something else still refers to it. Its open
    suggestions go with it; both are recorded so undo can bring them back. True if it went."""
    if conn.execute("SELECT 1 FROM pages WHERE document_id = ? LIMIT 1", (doc_id,)).fetchone():
        return False
    doc = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if doc is None:
        return False
    suggestions = conn.execute(
        "SELECT * FROM suggestions WHERE document_id = ? AND status = 'open'", (doc_id,)
    ).fetchall()
    conn.execute("SAVEPOINT remove_doc")
    try:
        conn.execute("DELETE FROM suggestions WHERE document_id = ? AND status = 'open'", (doc_id,))
        conn.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK TO remove_doc")  # facts or an export still refer to it: keep it
        conn.execute("RELEASE remove_doc")
        return False
    before = {"document": dict(doc), "suggestions": [dict(r) for r in suggestions]}
    history.log(conn, batch, "remove_empty_document", "document", doc_id, before, None)
    conn.execute("RELEASE remove_doc")
    return True


def _open_document(conn: sqlite3.Connection, doc_id: int) -> sqlite3.Row:
    doc = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if doc is None:
        raise LookupError(f"There's no document {doc_id}")
    if doc["status"] == "complete":
        raise ValueError(f"“{doc['name']}” is completed. Reopen it to change it.")
    return doc


def _from(conn: sqlite3.Connection, page_ids: list[int]) -> set[int]:
    """The documents these pages are in now, checking each page exists and can be moved."""
    if not page_ids:
        raise ValueError("Choose at least one page")
    docs = set()
    for pid in page_ids:
        r = conn.execute("SELECT document_id FROM pages WHERE id = ?", (pid,)).fetchone()
        if r is None:
            raise LookupError(f"There's no page {pid}")
        if r["document_id"] is not None:
            _open_document(conn, r["document_id"])
            docs.add(r["document_id"])
    return docs


def _done(conn: sqlite3.Connection, change: Change) -> Change:
    """A change that changed nothing (pages already where they were asked to go, a name as it
    was) has nothing to undo: its batch number would be the next change's."""
    if not conn.execute(
        "SELECT 1 FROM history WHERE batch = ? LIMIT 1", (change.batch,)
    ).fetchone():
        change.batch = None
    return change


def _top(conn: sqlite3.Connection, doc_id: int) -> int:
    return conn.execute(
        "SELECT coalesce(max(position), -1) FROM pages WHERE document_id = ?", (doc_id,)
    ).fetchone()[0]


def _tidy(conn: sqlite3.Connection, batch: int, docs: set[int], change: Change) -> None:
    """Close the gaps left in documents pages came out of; remove any left empty."""
    for doc in sorted(docs):
        close_gaps(conn, batch, doc)
    for doc in sorted(docs):
        if remove_if_empty(conn, batch, doc):
            change.removed.append(doc)


# ---------------------------------------------------------------- Decisions (a person's)


def move_pages(
    conn: sqlite3.Connection, page_ids: list[int], to: Where, document_id: int | None = None
) -> Change:
    """Move pages, in this order, to the end of a document, back to the Inbox, or Set aside."""
    with history.deciding(conn):
        if to == "document":
            if document_id is None:
                raise ValueError("Choose a document to move the pages to")
            _open_document(conn, document_id)
        left = _from(conn, page_ids) - ({document_id} if to == "document" else set())
        change = Change(history.new_batch(conn), document_id if to == "document" else None)
        top = _top(conn, document_id) if to == "document" else 0
        for pid in page_ids:
            if to == "document":
                if history.place(conn, pid)["document_id"] == document_id:
                    continue  # already there
                top += 1
                move_page(conn, change.batch, pid, document_id, top, aside=False, action="move")
            elif to == "inbox":
                move_page(conn, change.batch, pid, None, None, aside=False, action="to_inbox")
            else:
                move_page(conn, change.batch, pid, None, None, aside=True, action="set_aside")
        _tidy(conn, change.batch, left, change)
        return _done(conn, change)


def reorder(conn: sqlite3.Connection, doc_id: int, page_ids: list[int]) -> Change:
    """Put a document's pages in this order. `page_ids` must be all of its pages."""
    with history.deciding(conn):
        _open_document(conn, doc_id)
        now = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM pages WHERE document_id = ? ORDER BY position", (doc_id,)
            )
        ]
        if sorted(now) != sorted(page_ids) or len(set(page_ids)) != len(page_ids):
            raise ValueError("The order must list every page of the document once")
        change = Change(history.new_batch(conn), doc_id)
        for i, pid in enumerate(page_ids):
            if history.place(conn, pid)["position"] != i:
                move_page(conn, change.batch, pid, doc_id, i, aside=False, action="reorder")
        return _done(conn, change)


def rotate(conn: sqlite3.Connection, page_ids: list[int], degrees: int) -> Change:
    """Turn pages a quarter turn or more. The scan itself is never changed: the turn is kept
    beside it, and applied when the page is shown, read or exported. Tesseract reads its text
    again the new way round, in the background (Pipeline.read_turned_again)."""
    if degrees % 90:
        raise ValueError("Pages turn in quarter turns")
    with history.deciding(conn):
        _from(conn, page_ids)
        change = Change(history.new_batch(conn))
        for pid in page_ids:
            was = conn.execute("SELECT user_rotation FROM pages WHERE id = ?", (pid,)).fetchone()[0]
            now = (was + degrees) % 360
            conn.execute(
                "UPDATE pages SET user_rotation = ?, updated_at = datetime('now') WHERE id = ?",
                (now, pid),
            )
            history.log(
                conn,
                change.batch,
                "rotate",
                "page_rotation",
                pid,
                {"user_rotation": was},
                {"user_rotation": now},
            )
    return change


def flip(conn: sqlite3.Connection, page_ids: list[int]) -> Change:
    """Turn pages round left to right: a mirror image (the back of a carbon copy) the right way
    round, or one Lindley took for a mirror image back. As with a turn, the scan itself is never
    changed, and its text is read again the new way round."""
    with history.deciding(conn):
        _from(conn, page_ids)
        change = Change(history.new_batch(conn))
        for pid in page_ids:
            was = conn.execute("SELECT user_mirror FROM pages WHERE id = ?", (pid,)).fetchone()[0]
            now = 1 - was
            conn.execute(
                "UPDATE pages SET user_mirror = ?, updated_at = datetime('now') WHERE id = ?",
                (now, pid),
            )
            history.log(
                conn,
                change.batch,
                "flip",
                "page_mirror",
                pid,
                {"user_mirror": was},
                {"user_mirror": now},
            )
    return change


def new_document(
    conn: sqlite3.Connection,
    page_ids: list[int],
    name: str,
    folder_id: int | None = None,
    suggested: bool = False,
) -> Change:
    """Start a document with these pages, in this order. `suggested`: the name is Lindley's,
    kept as it was, so it's still shown as a suggestion."""
    name = name.strip() or "Untitled document"
    with history.deciding(conn):
        if folder_id is not None and not _folder(conn, folder_id):
            raise LookupError(f"There's no folder {folder_id}")
        left = _from(conn, page_ids)
        change = Change(history.new_batch(conn))
        doc = conn.execute(
            "INSERT INTO documents (name, name_source, origin, folder_id, reasons)"
            " VALUES (?, ?, 'user', ?, ?)",
            (
                name,
                "lindley" if suggested else "user",
                folder_id,
                json.dumps(["You put these pages together yourself"]),
            ),
        ).lastrowid
        change.document_id = doc
        history.log(
            conn, change.batch, "new_document", "new_document", doc, None, {"pages": page_ids}
        )
        for i, pid in enumerate(page_ids):
            move_page(conn, change.batch, pid, doc, i, aside=False, action="new_document")
        _tidy(conn, change.batch, left, change)
    return change


def update_document(conn: sqlite3.Connection, doc_id: int, changes: dict) -> Change:
    """Rename a document, give its type or date, or file it in a folder (None: out of any).
    A name or date a person gives is theirs: Lindley only suggests changes to it after that."""
    with history.deciding(conn):
        doc = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
        if doc is None:
            raise LookupError(f"There's no document {doc_id}")
        if doc["status"] == "complete" and set(changes) - {"folder_id"}:
            raise ValueError(f"“{doc['name']}” is completed. Reopen it to change it.")
        if changes.get("folder_id") is not None and not _folder(conn, changes["folder_id"]):
            raise LookupError(f"There's no folder {changes['folder_id']}")
        new = dict(changes)
        if "name" in new:
            new["name"] = (new["name"] or "").strip()
            if not new["name"]:
                raise ValueError("A document needs a name")
            new["name_source"] = "user"
        if "doc_date" in new:
            new["doc_date"] = (new["doc_date"] or "").strip() or None
            if new["doc_date"] != doc["doc_date"]:  # the same date is still Lindley's, if it was
                new["date_source"] = "user" if new["doc_date"] else None
        if "doc_type" in new:
            new["doc_type"] = (new["doc_type"] or "").strip() or None
        before = {k: doc[k] for k in DOCUMENT_FIELDS}
        after = before | {k: v for k, v in new.items() if k in DOCUMENT_FIELDS}
        if after == before:
            return Change(None, doc_id)
        change = Change(history.new_batch(conn), doc_id)
        _set_document(conn, doc_id, after)
        history.log(conn, change.batch, "update_document", "document_fields", doc_id, before, after)
    return change


def _set_document(conn: sqlite3.Connection, doc_id: int, fields: dict) -> None:
    sets = ", ".join(f"{k} = ?" for k in DOCUMENT_FIELDS)
    conn.execute(
        f"UPDATE documents SET {sets}, updated_at = datetime('now') WHERE id = ?",
        (*(fields[k] for k in DOCUMENT_FIELDS), doc_id),
    )


def _folder(conn: sqlite3.Connection, folder_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM folders WHERE id = ?", (folder_id,)).fetchone()


def new_folder(conn: sqlite3.Connection, name: str, parent_id: int | None = None) -> Change:
    name = name.strip() or "New folder"
    with history.deciding(conn):
        if parent_id is not None and not _folder(conn, parent_id):
            raise LookupError(f"There's no folder {parent_id}")
        change = Change(history.new_batch(conn))
        position = conn.execute(
            "SELECT coalesce(max(position), -1) + 1 FROM folders WHERE parent_id IS ?",
            (parent_id,),
        ).fetchone()[0]
        change.folder_id = conn.execute(
            "INSERT INTO folders (name, parent_id, position) VALUES (?, ?, ?)",
            (name, parent_id, position),
        ).lastrowid
        history.log(conn, change.batch, "new_folder", "new_folder", change.folder_id, None, {})
    return change


def rename_folder(conn: sqlite3.Connection, folder_id: int, name: str) -> Change:
    name = name.strip()
    if not name:
        raise ValueError("A folder needs a name")
    with history.deciding(conn):
        f = _folder(conn, folder_id)
        if f is None:
            raise LookupError(f"There's no folder {folder_id}")
        if f["name"] == name:
            return Change(None, folder_id=folder_id)
        change = Change(history.new_batch(conn), folder_id=folder_id)
        if f["name"] != name:
            conn.execute("UPDATE folders SET name = ? WHERE id = ?", (name, folder_id))
            history.log(
                conn,
                change.batch,
                "rename_folder",
                "folder_fields",
                folder_id,
                {"name": f["name"]},
                {"name": name},
            )
    return change


def check_text(conn: sqlite3.Connection, page_id: int, text: str | None = None) -> Change:
    """A person checked a page's text: as it is (`text` None, or unchanged), or corrected.
    A correction is a new reading of its own, and the one in use from then on; the earlier
    readings are kept."""
    with history.deciding(conn):
        if not conn.execute("SELECT 1 FROM pages WHERE id = ?", (page_id,)).fetchone():
            raise LookupError(f"There's no page {page_id}")
        cur = conn.execute(
            "SELECT id, text, confirmed_at FROM transcriptions"
            " WHERE page_id = ? AND is_current = 1",
            (page_id,),
        ).fetchone()
        if text is not None:
            text = text.strip()
        if cur is None and not text:
            raise ValueError("This page hasn't been read yet, so there's no text to check")
        change = Change(history.new_batch(conn))
        before = {
            "current": cur["id"] if cur else None,
            "confirmed_at": cur and cur["confirmed_at"],
        }
        if text is None or (cur is not None and text == cur["text"]):
            conn.execute(
                "UPDATE transcriptions SET confirmed_at = coalesce(confirmed_at, datetime('now'))"
                " WHERE id = ?",
                (cur["id"],),
            )
            new_id, action = cur["id"], "confirm_text"
        else:
            if cur is not None:
                conn.execute("UPDATE transcriptions SET is_current = 0 WHERE id = ?", (cur["id"],))
            new_id = conn.execute(
                "INSERT INTO transcriptions (page_id, source, text, is_current, confirmed_at)"
                " VALUES (?, 'user', ?, 1, datetime('now'))",
                (page_id, text),
            ).lastrowid
            action = "correct_text"
        confirmed = conn.execute(
            "SELECT confirmed_at FROM transcriptions WHERE id = ?", (new_id,)
        ).fetchone()[0]
        after = {"current": new_id, "confirmed_at": confirmed}
        history.log(conn, change.batch, action, "page_text", page_id, before, after)
        # A scan waiting only for the AI to read its pages (ocr.engine "vision") is read once
        # each has text: a person's will do
        conn.execute(
            "UPDATE scans SET status = 'read' WHERE status = 'queued'"
            " AND id = (SELECT scan_id FROM pages WHERE id = ?) AND NOT EXISTS ("
            " SELECT 1 FROM pages p WHERE p.scan_id = scans.id AND NOT EXISTS ("
            "  SELECT 1 FROM transcriptions t WHERE t.page_id = p.id AND t.is_current = 1))",
            (page_id,),
        )
    return change
