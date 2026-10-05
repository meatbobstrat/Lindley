"""Exporting a document: one searchable PDF of its pages, in order, in the library's Exports.

Exporting marks the document Completed: it stays in its folder, or moves to Completed if it
has none, and Lindley leaves it alone from then on. Pages still waiting for review don't stop
an export; the PDF carries Lindley's best reading of them. Reopening a document keeps its PDF
until it's exported again, which replaces it (under its new name, if it was renamed).

Every export is recorded in `exports`, with the pages it held, and in `history`. It isn't
an undoable decision: reopening is how to take it back.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from lindley import history
from lindley.config import REVIEW_BELOW
from lindley.db.progress import pages_to_review
from lindley.export.pdf import PdfInfo, PdfPage, build_pdf
from lindley.export.textlayer import page_text

log = logging.getLogger(__name__)

EXPORTS = "Exports"
MAX_NAME = 120
_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED = re.compile(r"^(con|prn|aux|nul|com\d|lpt\d)(\..*)?$", re.IGNORECASE)


@dataclass
class Exported:
    document_id: int
    pdf_path: Path
    exported_at: str
    pages: list[int]
    to_review: list[int]  # exported with Lindley's best reading, before a person checked it
    unplaced: list[int]  # searchable, but the text isn't over the writing (no word positions)


def exports_dir(library_dir: Path) -> Path:
    return (library_dir / EXPORTS).resolve()


def export_document(
    conn: sqlite3.Connection,
    library_dir: Path,
    doc_id: int,
    review_below: float = REVIEW_BELOW,
) -> Exported:
    doc = conn.execute(
        "SELECT id, name, status, doc_type, doc_date FROM documents WHERE id = ?", (doc_id,)
    ).fetchone()
    if doc is None:
        raise LookupError(f"There's no document {doc_id}")
    rows = conn.execute(
        "SELECT p.id, p.image_path, p.detected_rotation, p.user_rotation, p.dpi, p.color_mode,"
        " p.detected_mirror != p.user_mirror AS mirrored,"
        " p.width_px, p.height_px, s.status AS scan_status"
        " FROM pages p JOIN scans s ON s.id = p.scan_id"
        " WHERE p.document_id = ? ORDER BY p.position",
        (doc_id,),
    ).fetchall()
    if not rows:
        raise ValueError("This document has no pages to export")
    if reading := [r for r in rows if r["scan_status"] != "read"]:
        raise ValueError(f"{_plural(len(reading), 'page')} still being read. Export it after.")
    if missing := [r for r in rows if not r["image_path"] or not Path(r["image_path"]).is_file()]:
        raise ValueError(f"The scan of {_plural(len(missing), 'page')} is missing from the library")

    pages, unplaced = [], []
    for r in rows:
        rotation = (r["detected_rotation"] + r["user_rotation"]) % 360
        size = (r["width_px"] or 0, r["height_px"] or 0)
        if rotation in (90, 270):
            size = size[::-1]
        text = page_text(conn, r["id"], size)
        if text.words and not text.placed:
            unplaced.append(r["id"])
        pages.append(
            PdfPage(
                Path(r["image_path"]),
                rotation,
                r["dpi"],
                r["color_mode"],
                text.words,
                bool(r["mirrored"]),
            )
        )
    subject = ", ".join(v for v in (doc["doc_type"], doc["doc_date"]) if v) or None
    data = build_pdf(pages, PdfInfo(doc["name"], subject))

    folder = exports_dir(library_dir)
    folder.mkdir(parents=True, exist_ok=True)
    dest = _destination(conn, folder, doc_id, doc["name"])
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    except PermissionError as e:  # on Windows, open in a PDF viewer
        tmp.unlink(missing_ok=True)
        raise ValueError(
            f"{dest.name} is open in another program. Close it there, and export again."
        ) from e

    page_ids = [r["id"] for r in rows]
    with conn:
        before = [
            Path(p)
            for (p,) in conn.execute(
                "SELECT DISTINCT pdf_path FROM exports WHERE document_id = ?", (doc_id,)
            )
        ]
        export_id = conn.execute(
            "INSERT INTO exports (document_id, pdf_path, page_ids) VALUES (?, ?, ?)",
            (doc_id, str(dest), json.dumps(page_ids)),
        ).lastrowid
        conn.execute(
            "UPDATE documents SET status = 'complete', updated_at = datetime('now') WHERE id = ?",
            (doc_id,),
        )
        history.log(
            conn,
            None,
            "export",
            "document",
            doc_id,
            {"status": doc["status"]},
            {"status": "complete", "pdf_path": str(dest), "pages": page_ids},
        )
    for old in before:
        if old != dest and old.parent == folder and not _held_by_another(conn, old, doc_id):
            try:
                old.unlink(missing_ok=True)
            except OSError as e:  # open in a viewer, say: it's exported all the same
                log.warning("Couldn't remove the earlier PDF %s: %s", old, e)

    at = conn.execute("SELECT exported_at FROM exports WHERE id = ?", (export_id,)).fetchone()[0]
    return Exported(
        doc_id, dest, at, page_ids, pages_to_review(conn, doc_id, review_below), unplaced
    )


def reopen_document(conn: sqlite3.Connection, doc_id: int) -> None:
    """Back to In progress (or its folder) to be worked on. Its PDF is kept until it's
    exported again."""
    doc = conn.execute("SELECT status FROM documents WHERE id = ?", (doc_id,)).fetchone()
    if doc is None:
        raise LookupError(f"There's no document {doc_id}")
    if doc["status"] != "complete":
        return
    with conn:
        conn.execute(
            "UPDATE documents SET status = 'progress', updated_at = datetime('now') WHERE id = ?",
            (doc_id,),
        )
        history.log(
            conn, None, "reopen", "document", doc_id, {"status": "complete"}, {"status": "progress"}
        )


def latest_export(conn: sqlite3.Connection, doc_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM exports WHERE document_id = ? ORDER BY id DESC LIMIT 1", (doc_id,)
    ).fetchone()


def safe_name(name: str) -> str:
    """A document's name as a Windows file name (without .pdf)."""
    stem = " ".join(_UNSAFE.sub(" ", name).split())[:MAX_NAME].rstrip(" .")
    if _RESERVED.match(stem):
        stem = f"{stem} (document)"
    return stem


def _destination(conn: sqlite3.Connection, folder: Path, doc_id: int, name: str) -> Path:
    """The PDF's path: the document's name, numbered when another file has it already."""
    stem = safe_name(name) or f"Document {doc_id}"
    n = 1
    while True:
        path = folder / (f"{stem}.pdf" if n == 1 else f"{stem} ({n}).pdf")
        if not _held_by_another(conn, path, doc_id) and (
            not path.exists() or _ours(conn, path, doc_id)
        ):
            return path
        n += 1


def _same(a: str, b: Path) -> bool:
    return os.path.normcase(a) == os.path.normcase(str(b))


def _held_by_another(conn: sqlite3.Connection, path: Path, doc_id: int) -> bool:
    """Whether another document's latest export is at `path`."""
    return any(
        _same(p, path)
        for (p,) in conn.execute(
            "SELECT e.pdf_path FROM exports e WHERE e.document_id != ?"
            " AND e.id = (SELECT max(id) FROM exports WHERE document_id = e.document_id)",
            (doc_id,),
        )
    )


def _ours(conn: sqlite3.Connection, path: Path, doc_id: int) -> bool:
    return any(
        _same(p, path)
        for (p,) in conn.execute("SELECT pdf_path FROM exports WHERE document_id = ?", (doc_id,))
    )


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"
