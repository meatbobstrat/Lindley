"""Lindley's own copies of a person's scans once they're exported: let go, and brought back.

Lindley works on copies of its own: each scan's working copy (`<library>/scans`) and, for a PDF
or TIFF, its page images (`<library>/pages/<scan>`). Once every page of a scan is in an exported
document, those are let go: the person has the PDF and their original, and that's all Lindley
needed them for. The scan's text, fingerprints and history stay, so search, Ask Lindley and
duplicate checks still work, and the original isn't imported again when it's seen.

Only an exact copy of an original that's safe is removed. The original must be where Lindley
left it, unchanged (its sha256 checked): in the person's scans folder (copy mode), or beside the
PDF (move mode). A scan added from the person's computer in copy mode is theirs, where they
keep it: Lindley can't see it, and doesn't need to. When an original isn't where Lindley left
it, Lindley keeps its copy and says so. Only files inside Lindley's library are ever removed.

Reopening an exported document brings them back: the originals are looked for where Lindley
left them, checked, and copied in again, and the page images are made again, so the document
keeps its pages, their order and their text. One that isn't there is asked for: the person shows
Lindley where it is (`found`). If an original can't be found, nothing is brought back and the
document stays exported.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from lindley.config import Settings
from lindley.worker.intake import (
    held_path,
    library_copy,
    move_checked,
    page_images,
    pages_folder,
    same_file,
)

log = logging.getLogger(__name__)

# Scans whose every page is in an exported document, and that Lindley still has a copy of
_WHOLLY_EXPORTED = (
    "SELECT s.* FROM scans s WHERE s.library_path IS NOT NULL"
    " AND EXISTS (SELECT 1 FROM pages p WHERE p.scan_id = s.id)"
    " AND NOT EXISTS (SELECT 1 FROM pages p LEFT JOIN documents d ON d.id = p.document_id"
    "  WHERE p.scan_id = s.id AND coalesce(d.status, '') != 'exported')"
)


@dataclass
class LetGo:
    scans: list[int] = field(default_factory=list)  # whose working copies were let go
    kept: list[str] = field(default_factory=list)  # originals not where Lindley left them


class OriginalsNeeded(ValueError):
    """Reopening needs originals that aren't where Lindley left them: (scan id, file name)."""

    def __init__(self, needed: list[tuple[int, str]]):
        self.needed = needed
        names = ", ".join(n for _, n in needed[:3]) + (" and more" if len(needed) > 3 else "")
        one = len(needed) == 1
        super().__init__(
            f"To reopen it, Lindley needs the original {'scan' if one else 'scans'} again:"
            f" {names}. {'It isn’t' if one else 'They aren’t'} where Lindley left"
            f" {'it' if one else 'them'}."
        )


def let_go(conn: sqlite3.Connection, library: Path, scan_ids: Iterable[int] | None = None) -> LetGo:
    """Let go of the working copies and page images of these scans (None: of every scan) once
    every page of a scan is in an exported document and its original is safe. A file that can't
    be removed now (open in another program) is tried again when Lindley next starts."""
    wanted = None if scan_ids is None else set(scan_ids)
    out = LetGo()
    for r in conn.execute(_WHOLLY_EXPORTED).fetchall():
        if wanted is not None and r["id"] not in wanted:
            continue
        if not _original_safe(r):
            out.kept.append(r["original_name"])
            continue
        if _remove_copies(conn, library, r):
            with conn:
                conn.execute("UPDATE pages SET image_path = NULL WHERE scan_id = ?", (r["id"],))
                conn.execute("UPDATE scans SET library_path = NULL WHERE id = ?", (r["id"],))
            out.scans.append(r["id"])
    if out.scans:
        log.info("Let go of Lindley's copies of %d exported scan(s)", len(out.scans))
    return out


def originals_needed(conn: sqlite3.Connection, doc_id: int) -> list[tuple[int, str]]:
    """The document's scans that were let go, whose originals aren't where Lindley left them."""
    return [
        (r["id"], r["original_name"])
        for r in _let_go_scans(conn, doc_id)
        if not (r["original_path"] and same_file(Path(r["original_path"]), r["sha256"]))
    ]


def bring_back(
    conn: sqlite3.Connection,
    settings: Settings | None,
    doc_id: int,
    found: dict[int, Path] | None = None,
) -> None:
    """Copy the document's let-go scans in again from their originals, and make their page
    images again. `found`: originals a person showed Lindley, by scan id (each checked against
    its fingerprint). Raises OriginalsNeeded, bringing nothing back, when one can't be found."""
    found = found or {}
    scans = _let_go_scans(conn, doc_id)
    if not scans:
        return
    if settings is None:
        raise ValueError("Lindley's library is needed to bring its scans back")
    sources: dict[int, Path] = {}
    needed = []
    for r in scans:
        src = found.get(r["id"])
        if src is None and r["original_path"]:
            src = Path(r["original_path"])
        if src is not None and same_file(src, r["sha256"]):
            sources[r["id"]] = src
        else:
            needed.append((r["id"], r["original_name"]))
    if needed:
        raise OriginalsNeeded(needed)
    for r in scans:
        src = sources[r["id"]]
        copy = library_copy(settings, src, r["sha256"])
        images = page_images(settings, r["id"], copy, r["mime_type"])
        known = {
            i for (i,) in conn.execute("SELECT page_index FROM pages WHERE scan_id = ?", (r["id"],))
        }
        if {i for i, *_ in images} != known:
            raise ValueError(f"{r['original_name']} doesn't have the pages it had")
        with conn:
            conn.executemany(
                "UPDATE pages SET image_path = ? WHERE scan_id = ? AND page_index = ?",
                [(str(path), r["id"], i) for i, path, *_ in images],
            )
            conn.execute("UPDATE scans SET library_path = ? WHERE id = ?", (str(copy), r["id"]))
        if r["id"] in found and r["import_mode"] == "move" and r["original_state"] != "held":
            # The one beside its PDF went: the copy the person showed Lindley is kept as the
            # original, to give back with the PDF next time
            held = held_path(settings, r["sha256"], Path(r["original_name"]).suffix)
            move_checked(src, held, r["sha256"])
            with conn:
                conn.execute(
                    "UPDATE scans SET original_path = ?, original_state = 'held' WHERE id = ?",
                    (str(held), r["id"]),
                )


def _let_go_scans(conn: sqlite3.Connection, doc_id: int) -> list[sqlite3.Row]:
    """The document's scans Lindley let go of: their page images are gone."""
    return conn.execute(
        "SELECT s.* FROM scans s WHERE s.id IN (SELECT scan_id FROM pages WHERE document_id = ?)"
        " AND EXISTS (SELECT 1 FROM pages p WHERE p.scan_id = s.id AND p.image_path IS NULL)"
        " ORDER BY s.id",
        (doc_id,),
    ).fetchall()


def _original_safe(r: sqlite3.Row) -> bool:
    """Whether the scan's original is where Lindley left it, unchanged."""
    if r["original_state"] not in ("in_place", "exported"):
        return False  # held by Lindley (some pages elsewhere), or gone
    if r["original_path"] is None:  # added from the person's computer, and left there
        return r["origin"] == "added" and r["original_state"] == "in_place"
    return same_file(Path(r["original_path"]), r["sha256"])


def _remove_copies(conn: sqlite3.Connection, library: Path, r: sqlite3.Row) -> bool:
    """Remove the scan's working copy and page images: only files in Lindley's library, never
    the original. True when none is left."""
    library = library.resolve()
    original = (
        os.path.normcase(str(Path(r["original_path"]).resolve())) if r["original_path"] else None
    )
    files = [Path(r["library_path"])] + [
        Path(p)
        for (p,) in conn.execute(
            "SELECT image_path FROM pages WHERE scan_id = ? AND image_path IS NOT NULL", (r["id"],)
        )
    ]
    ok = True
    for f in dict.fromkeys(f.resolve() for f in files):
        if not f.is_relative_to(library) or os.path.normcase(str(f)) == original:
            log.warning("Not removing %s: it isn't Lindley's own copy", f)
            ok = False
            continue
        try:
            f.unlink(missing_ok=True)
        except OSError as e:
            log.warning("Couldn't remove %s, Lindley's copy of an exported scan: %s", f, e)
            ok = False
    for folder in (pages_folder(library, r["id"]), Path(r["library_path"]).resolve().parent):
        try:
            if folder.is_relative_to(library) and folder != library and folder.is_dir():
                folder.rmdir()  # only when empty
        except OSError:
            pass
    return ok
