"""Intake: bring one scan file into Lindley, then split it into pages.

Steps (each recorded in intake_steps): hash, a verified copy into the library, exif, split.
The original file is never altered. In move mode it is removed only once everything has worked;
a file that fails is copied (or, in move mode, moved) to the quarantine folder.
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from PIL import ExifTags, Image

from lindley.config import Settings
from lindley.worker.pipeline import Step, StepStatus, now, record_step, run_step

if TYPE_CHECKING:
    from lindley.worker.pipeline import Pipeline

SUPPORTED_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp", ".pdf"})
PDF_DPI = 300
_EXIF_IFD = 0x8769
# EXIF tags that say when the page was scanned, best first.
_SCAN_TIME_TAGS = (36867, 36868, 306)  # DateTimeOriginal, DateTimeDigitized, DateTime


@dataclass
class ImportResult:
    path: Path
    status: Literal["new", "duplicate", "failed"]
    scan_id: int | None = None
    pages: int = 0
    error: str | None = None
    reading: str | None = None  # after ingest: 'read' or 'failed'; None if nothing to read


def is_supported(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_SUFFIXES


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def import_file(
    conn: sqlite3.Connection,
    settings: Settings,
    path: Path,
    origin: Literal["watched", "added"] = "added",
) -> ImportResult:
    """Import one scan file: hash, copy into the library, read EXIF and split into pages."""
    path = Path(path)
    if not is_supported(path):
        return ImportResult(path, "failed", error=f"Not a supported file type: {path.suffix}")
    started = now()
    try:
        sha = sha256_of(path)
    except OSError as e:
        return _quarantine(settings, path, ImportResult(path, "failed", error=str(e)))

    existing = conn.execute(
        "SELECT s.id, s.status, (SELECT COUNT(*) FROM pages p WHERE p.scan_id = s.id) AS n"
        " FROM scans s WHERE s.sha256 = ?",
        (sha,),
    ).fetchone()
    # A file that failed before it was split can be tried again; anything else is a duplicate.
    if existing and not (existing["status"] == "failed" and existing["n"] == 0):
        return ImportResult(path, "duplicate", existing["id"], existing["n"])

    try:
        copy = _library_copy(settings, path, sha)
    except OSError as e:
        return _quarantine(settings, path, ImportResult(path, "failed", error=str(e)))

    mime = mimetypes.guess_type(path.name)[0]
    with conn:
        if existing:
            scan_id = existing["id"]
            conn.execute(
                "UPDATE scans SET status = 'queued', error = NULL, library_path = ? WHERE id = ?",
                (str(copy), scan_id),
            )
        else:
            scan_id = conn.execute(
                "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode,"
                " library_path, mime_type, file_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    sha,
                    path.name,
                    str(path.resolve()),
                    origin,
                    "move" if settings.move_files else "copy",
                    str(copy),
                    mime,
                    copy.stat().st_size,
                ),
            ).lastrowid
        record_step(conn, scan_id, Step.HASH, StepStatus.DONE, started_at=started)

    try:
        with run_step(conn, scan_id, Step.EXIF), conn:
            conn.execute(
                "UPDATE scans SET file_created_at = ?, file_modified_at = ?, scanner_make = ?,"
                " scanner_model = ?, scanned_at = ?, exif_json = ? WHERE id = ?",
                (*_file_times(path), *_exif(copy, mime), scan_id),
            )
        with run_step(conn, scan_id, Step.SPLIT):
            pages = _split(conn, settings, scan_id, copy, mime)
    except Exception as e:
        with conn:
            conn.execute(
                "UPDATE scans SET status = 'failed', error = ? WHERE id = ?", (str(e), scan_id)
            )
        return _quarantine(settings, path, ImportResult(path, "failed", scan_id, error=str(e)))

    if settings.move_files:
        path.unlink()
    return ImportResult(path, "new", scan_id, pages)


def ingest(
    conn: sqlite3.Connection,
    settings: Settings,
    pipeline: Pipeline,
    path: Path,
    origin: Literal["watched", "added"] = "added",
) -> ImportResult:
    """Import a file, then read any of its pages not read yet (a scan that failed is retried)."""
    r = import_file(conn, settings, path, origin)
    if r.scan_id and r.pages:
        status = conn.execute("SELECT status FROM scans WHERE id = ?", (r.scan_id,)).fetchone()[0]
        if status != "read":
            r.reading = pipeline.process_scan(conn, r.scan_id)
    return r


def _library_copy(settings: Settings, path: Path, sha: str) -> Path:
    """Lindley's own copy, named by its hash, checked against the original before it's trusted."""
    dest = settings.library_dir / "scans" / sha[:2] / f"{sha}{path.suffix.lower()}"
    if not (dest.exists() and sha256_of(dest) == sha):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        if sha256_of(dest) != sha:
            dest.unlink()
            raise OSError(f"The copy of {path.name} in the library didn't match the original")
    return dest


def _quarantine(settings: Settings, path: Path, result: ImportResult) -> ImportResult:
    """Put a failed file aside. Copy mode keeps the original where it was."""
    if not path.exists():
        return result
    dest = settings.quarantine_dir / path.name
    if dest.exists():
        dest = dest.with_name(f"{path.stem}-{datetime.now(UTC):%Y%m%d%H%M%S%f}{path.suffix}")
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if settings.move_files:
            shutil.move(path, dest)
        else:
            shutil.copy2(path, dest)
    except OSError as e:
        result.error = f"{result.error}; also couldn't quarantine it: {e}"
    return result


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d %H:%M:%S")


def _file_times(path: Path) -> tuple[str, str]:
    st = path.stat()
    created = getattr(st, "st_birthtime", st.st_ctime)
    return _iso(created), _iso(st.st_mtime)


def _json_safe(v: object) -> object:
    if isinstance(v, bytes):
        return v.decode("ascii", "replace").strip("\x00") if len(v) <= 256 else None
    if isinstance(v, tuple):
        return [_json_safe(x) for x in v]
    if isinstance(v, (int, str)):
        return v
    try:
        return float(v)  # IFDRational and friends
    except (TypeError, ValueError):
        return str(v)


def _exif_time(v: object) -> str | None:
    """'2024:01:02 03:04:05' -> '2024-01-02 03:04:05'."""
    if not isinstance(v, str) or len(v) < 10:
        return None
    return v[:10].replace(":", "-") + v[10:19]


def _pdf_time(v: str | None) -> str | None:
    """PDF dates look like 'D:20240102030405+01'00''."""
    digits = (v or "").removeprefix("D:")[:14]
    if len(digits) < 8 or not digits.isdigit():
        return None
    d = digits.ljust(14, "0")
    return f"{d[:4]}-{d[4:6]}-{d[6:8]} {d[8:10]}:{d[10:12]}:{d[12:14]}"


def _exif(copy: Path, mime: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    """(scanner_make, scanner_model, scanned_at, exif_json) from image EXIF or PDF metadata."""
    if mime == "application/pdf":
        import pypdfium2 as pdfium

        pdf = pdfium.PdfDocument(copy)
        try:
            meta = {k: v for k, v in pdf.get_metadata_dict().items() if v}
        finally:
            pdf.close()
        when = _pdf_time(meta.get("CreationDate"))
        return None, None, when, json.dumps(meta) if meta else None

    with Image.open(copy) as img:
        exif = img.getexif()
        tags = {**exif, **exif.get_ifd(_EXIF_IFD)}
    if not tags:
        return None, None, None, None
    named = {ExifTags.TAGS.get(k, str(k)): _json_safe(v) for k, v in tags.items()}
    named = {k: v for k, v in named.items() if v not in (None, "")}
    when = next((t for k in _SCAN_TIME_TAGS if (t := _exif_time(tags.get(k)))), None)
    make = str(tags.get(271, "")).strip("\x00 ") or None
    model = str(tags.get(272, "")).strip("\x00 ") or None
    return make, model, when, json.dumps(named, default=str)


def _color_mode(mode: str) -> str:
    return "bilevel" if mode == "1" else "gray" if mode in ("L", "LA", "I", "I;16") else "rgb"


def _dpi(img: Image.Image) -> int | None:
    dpi = img.info.get("dpi")
    return round(float(dpi[0])) if dpi and dpi[0] else None


def _split(
    conn: sqlite3.Connection, settings: Settings, scan_id: int, copy: Path, mime: str | None
) -> int:
    """One pages row per page. Single images are read in place; others become page PNGs."""
    out = settings.library_dir / "pages" / str(scan_id)
    rows: list[tuple] = []
    if mime == "application/pdf":
        import pypdfium2 as pdfium

        pdf = pdfium.PdfDocument(copy)
        try:
            out.mkdir(parents=True, exist_ok=True)
            for i in range(len(pdf)):
                img = pdf[i].render(scale=PDF_DPI / 72).to_pil()
                dest = out / f"{i:04d}.png"
                img.save(dest, dpi=(PDF_DPI, PDF_DPI))
                rows.append((i, dest, img.width, img.height, PDF_DPI, _color_mode(img.mode)))
        finally:
            pdf.close()
    else:
        with Image.open(copy) as img:
            frames = getattr(img, "n_frames", 1)
            if frames == 1:
                rows.append((0, copy, img.width, img.height, _dpi(img), _color_mode(img.mode)))
            else:
                out.mkdir(parents=True, exist_ok=True)
                for i in range(frames):
                    img.seek(i)
                    dest = out / f"{i:04d}.png"
                    dpi = _dpi(img)
                    img.save(dest, **({"dpi": (dpi, dpi)} if dpi else {}))
                    rows.append((i, dest, img.width, img.height, dpi, _color_mode(img.mode)))
    if not rows:
        raise ValueError("The file has no pages")
    with conn:
        conn.executemany(
            "INSERT INTO pages (scan_id, page_index, image_path, width_px, height_px, dpi,"
            " color_mode) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(scan_id, i, str(p), w, h, d, m) for i, p, w, h, d, m in rows],
        )
        conn.execute("UPDATE scans SET page_count = ? WHERE id = ?", (len(rows), scan_id))
    return len(rows)
