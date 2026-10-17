"""Completing a document, then exporting it: one searchable PDF of its pages, in order.

Two steps. Completing is a person's word that the document is right: it's locked, and Lindley
leaves it alone. Exporting gives it back: the PDF goes to the output folder (settings'
`output_dir`, which a person chooses in Setup or Settings). A document whose original scans
Lindley holds (move mode) gets a folder of its own there, `<name>/`, with the PDF and those
originals, unchanged and under the names the person gave them. Otherwise the PDF goes in the
output folder itself, as `<name>.pdf`. An original some of whose pages are in another document,
or in the Inbox or Set aside, stays in Lindley's keeping for now, and the export says so.

Pages still waiting for review don't stop either step; the PDF carries Lindley's best reading
of them. Once every page of a scan is exported, Lindley lets go of its own copy of it
(lindley.export.keeping). Reopening a document brings those back from the originals, and keeps
what was exported until it's exported again, which replaces the PDF (under its new name, if it
was renamed) and brings its originals along.

Every export is recorded in `exports`, with the pages it held and the originals it gave back,
and in `history`. Neither step is an undoable decision: reopening is how to take it back.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from lindley import history
from lindley.config import REVIEW_BELOW, Settings
from lindley.db.progress import pages_to_review
from lindley.export.keeping import bring_back, let_go
from lindley.export.pdf import PdfInfo, PdfPage, build_pdf
from lindley.export.textlayer import page_text
from lindley.worker.intake import move_checked, same_file

log = logging.getLogger(__name__)

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
    folder: Path | None = None  # the document's own folder, when its originals go with it
    originals: list[Path] = field(default_factory=list)  # the originals beside the PDF
    kept: list[str] = field(default_factory=list)  # originals with pages elsewhere: still held
    problems: list[str] = field(default_factory=list)  # originals that couldn't be given back
    # Originals not where Lindley left them (copy mode): Lindley keeps its own copy of them
    copies_kept: list[str] = field(default_factory=list)


def complete_document(
    conn: sqlite3.Connection, doc_id: int, review_below: float = REVIEW_BELOW
) -> list[int]:
    """A person says the document is right: it's locked until it's reopened. Returns its pages
    still waiting for review (they don't stop it). Completing one complete already does nothing."""
    doc = _document(conn, doc_id)
    if doc["status"] == "progress":
        _pages(conn, doc_id)
        with conn:
            conn.execute(
                "UPDATE documents SET status = 'complete', updated_at = datetime('now')"
                " WHERE id = ?",
                (doc_id,),
            )
            history.log(
                conn,
                None,
                "complete",
                "document",
                doc_id,
                {"status": "progress"},
                {"status": "complete"},
            )
    return pages_to_review(conn, doc_id, review_below)


def export_document(
    conn: sqlite3.Connection,
    output_dir: Path,
    doc_id: int,
    review_below: float = REVIEW_BELOW,
    library: Path | None = None,
) -> Exported:
    """Give a completed document back: its PDF, and the originals Lindley holds for it, in the
    output folder. With `library` (Lindley's), it then lets go of its copies of the scans now
    wholly exported. Raises LookupError for no such document, ValueError when it can't be now."""
    doc = _document(conn, doc_id)
    if doc["status"] == "progress":
        raise ValueError(f"“{doc['name']}” isn't completed yet. Complete it, then export it.")
    if doc["status"] == "exported":
        raise ValueError(f"“{doc['name']}” is exported already. Reopen it to export it again.")
    rows = _pages(conn, doc_id)

    pages, unplaced = [], []
    for r in rows:
        rotation = (r["detected_rotation"] + r["user_rotation"]) % 360
        size = (r["width_px"] or 0, r["height_px"] or 0)
        if rotation in (90, 270):
            size = size[::-1]
        text = page_text(conn, r["id"], size, (rotation, bool(r["mirrored"])))
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

    output = output_dir.resolve()
    going, kept = _originals(conn, doc_id)
    stem = safe_name(doc["name"]) or f"Document {doc_id}"
    folder = _folder(conn, output, doc_id, stem) if going or kept else None
    dest = _destination(conn, folder or output, doc_id, folder.name if folder else stem)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, dest)
    except PermissionError as e:  # on Windows, open in a PDF viewer
        tmp.unlink(missing_ok=True)
        raise ValueError(
            f"{dest.name} is open in another program. Close it there, and export again."
        ) from e

    out = Exported(doc_id, dest, "", [r["id"] for r in rows], [], unplaced, folder)
    out.kept = [r["original_name"] for r in kept]
    moved: list[tuple[int, Path | None]] = []  # (scan, where its original is now; None: gone)
    for r in going:
        src = Path(r["original_path"])
        if not src.is_file():
            moved.append((r["id"], None))
            out.problems.append(f"{r['original_name']} wasn't where Lindley kept it")
            continue
        there = src if src.parent == folder else _free_name(folder, r["original_name"], r)
        try:
            if there != src:
                move_checked(src, there, r["sha256"])
        except OSError as e:
            out.problems.append(f"{r['original_name']} couldn't be moved there: {e}")
            continue
        moved.append((r["id"], there))
        out.originals.append(there)

    with conn:
        before = [
            Path(p)
            for (p,) in conn.execute(
                "SELECT DISTINCT pdf_path FROM exports WHERE document_id = ?", (doc_id,)
            )
        ]
        export_id = conn.execute(
            "INSERT INTO exports (document_id, pdf_path, page_ids, originals) VALUES (?, ?, ?, ?)",
            (doc_id, str(dest), json.dumps(out.pages), json.dumps([str(p) for p in out.originals])),
        ).lastrowid
        for scan_id, where in moved:
            conn.execute(
                "UPDATE scans SET original_path = coalesce(?, original_path),"
                " original_state = CASE WHEN ? IS NULL THEN 'missing' ELSE 'exported' END"
                " WHERE id = ?",
                (where and str(where), where and str(where), scan_id),
            )
        conn.execute(
            "UPDATE documents SET status = 'exported', updated_at = datetime('now') WHERE id = ?",
            (doc_id,),
        )
        history.log(
            conn,
            None,
            "export",
            "document",
            doc_id,
            {"status": doc["status"]},
            {
                "status": "exported",
                "pdf_path": str(dest),
                "pages": out.pages,
                "originals": [str(p) for p in out.originals],
            },
        )
    for old in before:  # its earlier PDF, under another name or in another folder
        mine = old.parent == output or old.parent.parent == output
        if old != dest and mine and not _held_by_another(conn, old, doc_id):
            try:
                old.unlink(missing_ok=True)
                if old.parent != output and old.parent != folder and not any(old.parent.iterdir()):
                    old.parent.rmdir()
            except OSError as e:  # open in a viewer, say: it's exported all the same
                log.warning("Couldn't remove the earlier PDF %s: %s", old, e)

    out.exported_at = conn.execute(
        "SELECT exported_at FROM exports WHERE id = ?", (export_id,)
    ).fetchone()[0]
    out.to_review = pages_to_review(conn, doc_id, review_below)
    if library is not None:
        scans = {r["scan_id"] for r in rows}
        out.copies_kept = let_go(conn, library, scans).kept
    return out


def reopen_document(
    conn: sqlite3.Connection,
    doc_id: int,
    settings: Settings | None = None,
    found: dict[int, Path] | None = None,
) -> None:
    """Back to In progress (or its folder) to be worked on. What it exported is kept until it's
    exported again. Scans Lindley let go of are brought back from their originals first (with
    `settings`; `found`: originals a person showed it): keeping.OriginalsNeeded when one can't be
    found, and the document stays as it is."""
    doc = _document(conn, doc_id)
    if doc["status"] == "progress":
        return
    if doc["status"] == "exported":
        bring_back(conn, settings, doc_id, found)
    with conn:
        conn.execute(
            "UPDATE documents SET status = 'progress', updated_at = datetime('now') WHERE id = ?",
            (doc_id,),
        )
        history.log(
            conn,
            None,
            "reopen",
            "document",
            doc_id,
            {"status": doc["status"]},
            {"status": "progress"},
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


def _document(conn: sqlite3.Connection, doc_id: int) -> sqlite3.Row:
    doc = conn.execute(
        "SELECT id, name, status, doc_type, doc_date FROM documents WHERE id = ?", (doc_id,)
    ).fetchone()
    if doc is None:
        raise LookupError(f"There's no document {doc_id}")
    return doc


def _pages(conn: sqlite3.Connection, doc_id: int) -> list[sqlite3.Row]:
    """The document's pages, in order, once each is read and its scan in the library."""
    rows = conn.execute(
        "SELECT p.id, p.scan_id, p.image_path, p.detected_rotation, p.user_rotation, p.dpi,"
        " p.color_mode, p.detected_mirror != p.user_mirror AS mirrored,"
        " p.width_px, p.height_px, s.status AS scan_status"
        " FROM pages p JOIN scans s ON s.id = p.scan_id"
        " WHERE p.document_id = ? ORDER BY p.position",
        (doc_id,),
    ).fetchall()
    if not rows:
        raise ValueError("This document has no pages")
    if reading := [r for r in rows if r["scan_status"] != "read"]:
        raise ValueError(f"{_plural(len(reading), 'page')} still being read. Try again after.")
    if missing := [r for r in rows if not r["image_path"] or not Path(r["image_path"]).is_file()]:
        raise ValueError(f"The scan of {_plural(len(missing), 'page')} is missing from the library")
    return rows


def _originals(
    conn: sqlite3.Connection, doc_id: int
) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    """The originals Lindley holds for this document: those that go with it (every page of the
    scan is in it), and those kept back (some of its pages are elsewhere). An original this
    document gave back before, exported again, goes with it again."""
    earlier = {
        p
        for (o,) in conn.execute(
            "SELECT originals FROM exports WHERE document_id = ? AND originals IS NOT NULL",
            (doc_id,),
        )
        for p in json.loads(o)
    }
    rows = conn.execute(
        "SELECT s.id, s.sha256, s.original_name, s.original_path, s.original_state,"
        " NOT EXISTS (SELECT 1 FROM pages o WHERE o.scan_id = s.id"
        "  AND o.document_id IS NOT ?) AS whole"
        " FROM scans s WHERE s.id IN (SELECT scan_id FROM pages WHERE document_id = ?)"
        " AND (s.original_state = 'held' OR s.original_state = 'exported')"
        " ORDER BY (SELECT min(position) FROM pages WHERE scan_id = s.id AND document_id = ?)",
        (doc_id, doc_id, doc_id),
    ).fetchall()
    ours = [r for r in rows if r["original_state"] == "held" or r["original_path"] in earlier]
    return [r for r in ours if r["whole"]], [r for r in ours if not r["whole"]]


def _folder(conn: sqlite3.Connection, output: Path, doc_id: int, stem: str) -> Path:
    """The document's own folder in the output folder: its name, numbered when another
    document's folder, or a folder Lindley didn't make, has it already."""
    mine = {Path(p).parent for p in _pdfs(conn, doc_id)}
    theirs = {Path(p).parent for p in _latest_pdfs(conn, doc_id)}
    n = 1
    while True:
        path = output / (stem if n == 1 else f"{stem} ({n})")
        if not any(_same(str(t), path) for t in theirs) and (
            not path.exists() or any(_same(str(m), path) for m in mine)
        ):
            return path
        n += 1


def _free_name(folder: Path, name: str, scan: sqlite3.Row) -> Path:
    """Where an original goes in the document's folder: its own name, numbered when another
    file has it already (the same file there already is where it goes)."""
    p = Path(Path(name).name)
    n = 1
    while True:
        path = folder / (p.name if n == 1 else f"{p.stem} ({n}){p.suffix}")
        if not path.exists() or same_file(path, scan["sha256"]):
            return path
        n += 1


def _destination(conn: sqlite3.Connection, folder: Path, doc_id: int, stem: str) -> Path:
    """The PDF's path: the document's name, numbered when another file has it already."""
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


def _pdfs(conn: sqlite3.Connection, doc_id: int) -> list[str]:
    return [
        p for (p,) in conn.execute("SELECT pdf_path FROM exports WHERE document_id = ?", (doc_id,))
    ]


def _latest_pdfs(conn: sqlite3.Connection, doc_id: int) -> list[str]:
    """Each other document's latest PDF."""
    return [
        p
        for (p,) in conn.execute(
            "SELECT e.pdf_path FROM exports e WHERE e.document_id != ?"
            " AND e.id = (SELECT max(id) FROM exports WHERE document_id = e.document_id)",
            (doc_id,),
        )
    ]


def _held_by_another(conn: sqlite3.Connection, path: Path, doc_id: int) -> bool:
    """Whether another document's latest export is at `path`."""
    return any(_same(p, path) for p in _latest_pdfs(conn, doc_id))


def _ours(conn: sqlite3.Connection, path: Path, doc_id: int) -> bool:
    return any(_same(p, path) for p in _pdfs(conn, doc_id))


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"
