"""What the app shows: pages with where they are and how well they were read, documents, folders,
the review queue and the counts beside each place. Read only; lindley.organise makes changes.

Each page has a `state`, the one thing the UI shows beside it, in this order of precedence:
- reading: not read yet (failed: its scan couldn't be read)
- checked: a person checked or corrected its text
- ai_reading: the vision model is reading it now
- needs_ai / ai_failed: waiting for the vision model, or its call failed (see Needs AI)
- review: read with less than ocr.review_below, so a person should check it: in the Inbox or
  a document in progress, as the review queue has them. A completed document's pages, and
  pages set aside, don't wait for review.
- ok
"""

from __future__ import annotations

import json
import sqlite3

from lindley.db.progress import document_progress
from lindley.duplicates import resolve
from lindley.worker import image as pageimage
from lindley.worker.pipeline import _LAST_VISION

_PAGES = f"""
SELECT p.id, p.scan_id, p.page_index, s.original_name AS file, s.origin, s.imported_at,
       s.scanned_at, s.status AS scan_status, s.error AS scan_error, p.width_px, p.height_px,
       p.dpi, p.color_mode, p.script, p.blank_score, p.detected_rotation, p.user_rotation,
       p.document_id, p.position, p.set_aside_at, c.page_id IS NOT NULL AS has_text, c.source,
       c.confidence, coalesce(c.reviewed, 0) AS reviewed, v.status AS vision_status,
       v.error AS vision_error, d.status AS document_status
FROM pages p
JOIN scans s ON s.id = p.scan_id
LEFT JOIN documents d ON d.id = p.document_id
LEFT JOIN v_current_text c ON c.page_id = p.id
LEFT JOIN ({_LAST_VISION}) v ON v.page_id = p.id
"""

INBOX = "p.document_id IS NULL AND p.set_aside_at IS NULL"
ASIDE = "p.set_aside_at IS NOT NULL"


def state(r: sqlite3.Row, review_below: float) -> str:
    if r["scan_status"] != "read" or not r["has_text"]:
        return "failed" if r["scan_status"] == "failed" else "reading"
    if r["reviewed"]:
        return "checked"
    if r["vision_status"] == "running" and r["set_aside_at"] is None:
        return "ai_reading"
    if r["vision_status"] in ("queued", "failed") and r["set_aside_at"] is None:
        return "ai_failed" if r["vision_status"] == "failed" else "needs_ai"
    # As lindley.db.progress counts them: a reading with no confidence (a blank page) isn't one
    waits = r["set_aside_at"] is None and r["document_status"] in (None, "progress")
    if waits and r["confidence"] is not None and r["confidence"] < review_below:
        return "review"
    return "ok"


def page_json(r: sqlite3.Row, review_below: float) -> dict:
    rotation = (r["detected_rotation"] + r["user_rotation"]) % 360
    where = "document" if r["document_id"] else "aside" if r["set_aside_at"] else "inbox"
    return {
        "id": r["id"],
        "file": r["file"],
        "page_index": r["page_index"],
        "origin": r["origin"],
        "imported_at": r["imported_at"],
        "scanned_at": r["scanned_at"],
        "where": where,
        "document_id": r["document_id"],
        "position": r["position"],
        "state": state(r, review_below),
        "source": r["source"],  # tesseract, vision or user
        "confidence": round(r["confidence"]) if r["confidence"] is not None else None,
        "why": r["vision_error"] if r["vision_status"] in ("queued", "failed") else r["scan_error"],
        "script": r["script"],
        "blank": (r["blank_score"] or 0) >= pageimage.BLANK_AT,
        "dpi": r["dpi"],
        "size": [r["width_px"], r["height_px"]] if r["width_px"] else None,
        "color_mode": r["color_mode"],
        "turned": r["user_rotation"],  # by a person, on top of what Lindley detected
        # The image, upright; `v` changes with every turn so a cached one isn't shown
        "image": f"/api/pages/{r['id']}/image?v={rotation}",
    }


def pages(
    conn: sqlite3.Connection, where: str, args: tuple, review_below: float, order: str
) -> list[dict]:
    rows = conn.execute(f"{_PAGES} WHERE {where} ORDER BY {order}", args).fetchall()
    return [page_json(r, review_below) for r in rows]


def inbox(conn: sqlite3.Connection, review_below: float) -> list[dict]:
    return pages(conn, INBOX, (), review_below, "s.imported_at DESC, p.scan_id DESC, p.page_index")


def aside(conn: sqlite3.Connection, review_below: float) -> list[dict]:
    out = pages(conn, ASIDE, (), review_below, "p.set_aside_at DESC, p.id DESC")
    for p in out:
        kept = resolve.duplicate_of(conn, p["id"])
        p["duplicate_of"] = kept and {"id": kept, "file": _file(conn, kept)}
    return out


def _file(conn: sqlite3.Connection, page_id: int) -> str | None:
    r = conn.execute(
        "SELECT s.original_name FROM pages p JOIN scans s ON s.id = p.scan_id WHERE p.id = ?",
        (page_id,),
    ).fetchone()
    return r[0] if r else None


def page(conn: sqlite3.Connection, page_id: int, review_below: float) -> dict | None:
    rows = pages(conn, "p.id = ?", (page_id,), review_below, "p.id")
    if not rows:
        return None
    out = rows[0]
    t = conn.execute(
        "SELECT text, source, engine_model, confidence, unsure_spans, confirmed_at"
        " FROM transcriptions WHERE page_id = ? AND is_current = 1",
        (page_id,),
    ).fetchone()
    out["text"] = t["text"] if t else ""
    out["engine_model"] = t["engine_model"] if t else None
    out["unsure"] = json.loads(t["unsure_spans"] or "[]") if t and out["state"] != "checked" else []
    out["readings"] = conn.execute(
        "SELECT COUNT(*) FROM transcriptions WHERE page_id = ?", (page_id,)
    ).fetchone()[0]
    if out["document_id"]:
        d = conn.execute(
            "SELECT name, name_source, status FROM documents WHERE id = ?", (out["document_id"],)
        ).fetchone()
        out["document"] = {
            "id": out["document_id"],
            "name": d["name"],
            "suggested": d["name_source"] == "lindley",
            "status": d["status"],
            "page_number": 1
            + conn.execute(
                "SELECT COUNT(*) FROM pages WHERE document_id = ? AND position < ?",
                (out["document_id"], out["position"]),
            ).fetchone()[0],
        }
    kept = resolve.duplicate_of(conn, page_id) if out["where"] == "aside" else None
    out["duplicate_of"] = kept and {"id": kept, "file": _file(conn, kept)}
    return out


# ---------------------------------------------------------------- Review


def review_queue(conn: sqlite3.Connection, review_below: float) -> list[dict]:
    """Pages waiting for a person's review: in the Inbox and in documents in progress, in
    order. Pages waiting for the vision model aren't here: they're in Needs AI."""
    rows = pages(
        conn,
        f"(({INBOX}) OR p.document_id IN (SELECT id FROM documents WHERE status = 'progress'))",
        (),
        review_below,
        "p.document_id IS NULL, p.document_id, p.position, s.imported_at, p.scan_id, p.page_index",
    )
    return [p for p in rows if p["state"] == "review"]


def review_groups(conn: sqlite3.Connection, review_below: float) -> list[dict]:
    """The review queue by place: each document in progress, then the Inbox."""
    groups: dict[int | None, dict] = {}
    for p in review_queue(conn, review_below):
        g = groups.get(p["document_id"])
        if g is None:
            g = groups[p["document_id"]] = {"document": None, "pages": []}
            if p["document_id"]:
                g["document"] = _document_brief(conn, p["document_id"])
        g["pages"].append(p)
    return sorted(groups.values(), key=lambda g: g["document"] is None)


def _document_brief(conn: sqlite3.Connection, doc_id: int) -> dict:
    d = conn.execute(
        "SELECT id, name, name_source, folder_id FROM documents WHERE id = ?", (doc_id,)
    ).fetchone()
    return {
        "id": d["id"],
        "name": d["name"],
        "suggested": d["name_source"] == "lindley",
        "folder_id": d["folder_id"],
    }


# ---------------------------------------------------------------- Documents and folders


def documents(conn: sqlite3.Connection, review_below: float) -> list[dict]:
    """Every document, with what the tree shows beside it."""
    review: dict[int, int] = {}
    for p in review_queue(conn, review_below):
        if p["document_id"]:
            review[p["document_id"]] = review.get(p["document_id"], 0) + 1
    needs_ai: dict[int, int] = {}
    vision = conn.execute(
        f"SELECT p.document_id, COUNT(*) FROM ({_LAST_VISION}) v JOIN pages p ON p.id = v.page_id"
        " LEFT JOIN v_current_text c ON c.page_id = p.id"
        " WHERE v.status IN ('queued', 'failed') AND p.document_id IS NOT NULL"
        " AND NOT coalesce(c.reviewed, 0) GROUP BY p.document_id"
    )
    needs_ai.update({r[0]: r[1] for r in vision})
    rows = conn.execute(
        "SELECT d.*, (SELECT COUNT(*) FROM pages p WHERE p.document_id = d.id) AS pages,"
        " (SELECT max(exported_at) FROM exports e WHERE e.document_id = d.id) AS exported_at,"
        " (SELECT p.id FROM pages p WHERE p.document_id = d.id ORDER BY p.position LIMIT 1)"
        " AS first_page"
        " FROM documents d ORDER BY d.status, d.created_at, d.id"
    ).fetchall()
    out = []
    for d in rows:
        ready = d["status"] == "progress" and document_progress(conn, d["id"], review_below).ready
        out.append(
            _document_json(d)
            | {
                "pages": d["pages"],
                "to_review": review.get(d["id"], 0) if d["status"] == "progress" else 0,
                "needs_ai": needs_ai.get(d["id"], 0) if d["status"] == "progress" else 0,
                "ready": ready,
                "exported_at": d["exported_at"],
                "first_page": d["first_page"],
            }
        )
    return out


def _document_json(d: sqlite3.Row) -> dict:
    return {
        "id": d["id"],
        "name": d["name"],
        "suggested": d["name_source"] == "lindley",  # shown in italics: Lindley's name
        "origin": d["origin"],
        "status": d["status"],
        "folder_id": d["folder_id"],
        "doc_type": d["doc_type"],
        "doc_date": d["doc_date"],
        "date_source": d["date_source"],
        "confidence": round(d["grouping_confidence"])
        if d["grouping_confidence"] is not None
        else None,
    }


def document(conn: sqlite3.Connection, doc_id: int, review_below: float) -> dict | None:
    d = conn.execute("SELECT * FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if d is None:
        return None
    progress = document_progress(conn, doc_id, review_below)
    export = conn.execute(
        "SELECT pdf_path, exported_at FROM exports WHERE document_id = ?"
        " ORDER BY exported_at DESC, id DESC LIMIT 1",
        (doc_id,),
    ).fetchone()
    # Inbox pages Lindley thinks belong here ("Add to …?")
    hinted = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT s.page_id FROM suggestions s JOIN pages p ON p.id = s.page_id"
            " WHERE s.status = 'open' AND s.kind = 'add_to_document' AND s.document_id = ?"
            " AND p.document_id IS NULL AND p.set_aside_at IS NULL",
            (doc_id,),
        )
    ]
    return _document_json(d) | {
        "reasons": json.loads(d["reasons"] or "[]"),
        "summary": d["summary"],
        "pages": pages(conn, "p.document_id = ?", (doc_id,), review_below, "p.position"),
        "progress": {
            "ready": d["status"] == "progress" and progress.ready,
            "checks": [
                {"key": c.key, "label": c.label, "done": c.done, "detail": c.detail}
                for c in progress.checks
            ],
        },
        "export": export
        and {
            "file_name": export["pdf_path"].replace("\\", "/").rsplit("/", 1)[-1],
            "exported_at": export["exported_at"],
        },
        "inbox_hints": hinted,
    }


def folders(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, parent_id, name, position FROM folders ORDER BY position, id"
        )
    ]


# ---------------------------------------------------------------- Counts


def counts(conn: sqlite3.Connection, review_below: float, needs_ai: int) -> dict:
    """The numbers beside each place in the tree, and what the status bar says."""
    sets = resolve.open_sets(conn)
    pairs = resolve.document_pairs(sets, conn)
    one = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
    return {
        "inbox": one(f"SELECT COUNT(*) FROM pages p WHERE {INBOX}"),
        "aside": one(f"SELECT COUNT(*) FROM pages p WHERE {ASIDE}"),
        "review": len(review_queue(conn, review_below)),
        # A document scanned twice is one thing to decide
        "duplicates": len(sets) - sum(len(p.set_ids) - 1 for p in pairs),
        "needs_ai": needs_ai,
        "reading": one("SELECT COUNT(*) FROM scans WHERE status IN ('queued', 'reading')"),
        "failed": one("SELECT COUNT(*) FROM scans WHERE status = 'failed'"),
    }
