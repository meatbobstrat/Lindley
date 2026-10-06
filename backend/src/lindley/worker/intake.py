"""Intake: bring one scan file into Lindley, then split it into pages.

Steps (each recorded in intake_steps): hash, a verified copy into the library, exif, split.
The original file is never altered. In move mode it is removed once its pages are in the
library (reading works from Lindley's own copy), and a file already there is removed when it
turns up again. A file that fails is copied (or, in move mode, moved) to the quarantine folder,
once: the same file failing again isn't copied again.
"""

from __future__ import annotations

import glob
import hashlib
import json
import logging
import mimetypes
import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from PIL import ExifTags, Image, ImageOps

from lindley.config import Settings
from lindley.worker.image import exif_orientation
from lindley.worker.pipeline import Step, StepStatus, now, record_step, run_step

if TYPE_CHECKING:
    from lindley.worker.pipeline import Pipeline

log = logging.getLogger(__name__)

SUPPORTED_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp", ".pdf"})
PDF_DPI = 300
# PDF pages are never rendered bigger than this many pixels (an A3 page at 300 dpi is 17 million).
MAX_RENDER_PX = 40_000_000
_PNG_MODES = frozenset({"1", "L", "LA", "I", "I;16", "P", "RGB", "RGBA"})
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


def looks_complete(path: Path) -> bool:
    """Whether the file's ending is there yet: a scanner or a copy may still be writing it.

    PDFs end with %%EOF, JPEGs with an end-of-image marker and PNGs with an IEND chunk; BMP and
    WebP files give their own length at the start. TIFFs have no ending to look for, so a steady
    size has to do.
    """
    try:
        with path.open("rb") as f:
            head = f.read(16)
            size = f.seek(0, 2)
            f.seek(max(0, size - 1024))
            tail = f.read()
    except OSError:
        return False
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return b"%%EOF" in tail
    if suffix in (".jpg", ".jpeg"):
        return b"\xff\xd9" in tail
    if suffix == ".png":
        return b"IEND" in tail[-32:]
    if suffix == ".bmp":
        return len(head) >= 6 and int.from_bytes(head[2:6], "little") <= size
    if suffix == ".webp":
        return head[:4] == b"RIFF" and int.from_bytes(head[4:8], "little") + 8 <= size
    return True


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
        st = path.stat()  # before hashing: a file changed since is noticed again
        sha = sha256_of(path)
    except OSError as e:
        return _quarantine(settings, path, ImportResult(path, "failed", error=str(e)))
    with conn:
        conn.execute(
            "INSERT INTO seen_files (path, size, mtime_ns, sha256) VALUES (?, ?, ?, ?)"
            " ON CONFLICT (path) DO UPDATE SET size = excluded.size,"
            " mtime_ns = excluded.mtime_ns, sha256 = excluded.sha256, seen_at = datetime('now')",
            (str(path.resolve()), st.st_size, st.st_mtime_ns, sha),
        )

    existing = conn.execute(
        "SELECT s.id, s.status, s.library_path,"
        " (SELECT COUNT(*) FROM pages p WHERE p.scan_id = s.id) AS n"
        " FROM scans s WHERE s.sha256 = ?",
        (sha,),
    ).fetchone()
    # A file that failed before it was split can be tried again; anything else is a duplicate.
    if existing and not (existing["status"] == "failed" and existing["n"] == 0):
        lib = existing["library_path"]
        if settings.move_files and lib and Path(lib).is_file() and sha256_of(Path(lib)) == sha:
            _remove_original(path)  # it's safe in the library already
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
        started = now()
        try:
            exif, exif_error = _exif(copy, mime), None
        except Exception as e:  # metadata it can't make out doesn't stop the pages being read
            exif, exif_error = (None, None, None, None), _in_words(e)
        with conn:
            conn.execute(
                "UPDATE scans SET file_created_at = ?, file_modified_at = ?, scanner_make = ?,"
                " scanner_model = ?, scanned_at = ?, exif_json = ? WHERE id = ?",
                (*_file_times(path), *exif, scan_id),
            )
            status = StepStatus.FAILED if exif_error else StepStatus.DONE
            record_step(conn, scan_id, Step.EXIF, status, started_at=started, error=exif_error)
        with run_step(conn, scan_id, Step.SPLIT):
            pages = _split(conn, settings, scan_id, copy, mime)
    except Exception as e:
        error = _in_words(e)
        with conn:
            conn.execute(
                "UPDATE scans SET status = 'failed', error = ? WHERE id = ?", (error, scan_id)
            )
        return _quarantine(settings, path, ImportResult(path, "failed", scan_id, error=error))

    if settings.move_files:
        _remove_original(path)
    return ImportResult(path, "new", scan_id, pages)


def already_seen(conn: sqlite3.Connection, path: Path, st: os.stat_result) -> bool:
    """Seen before at this path, size and time, and split into pages: nothing to do. (Reading
    pages that were never read is the watcher's start-up work, from Lindley's own copy.) A
    file that failed before it was split is tried again."""
    return (
        conn.execute(
            "SELECT 1 FROM seen_files f JOIN scans s ON s.sha256 = f.sha256"
            " WHERE f.path = ? AND f.size = ? AND f.mtime_ns = ?"
            " AND EXISTS (SELECT 1 FROM pages p WHERE p.scan_id = s.id)",
            (str(path.resolve()), st.st_size, st.st_mtime_ns),
        ).fetchone()
        is not None
    )


def absolute_paths(conn: sqlite3.Connection) -> int:
    """Library paths stored relative to the folder Lindley ran in (as the default settings
    made them) become absolute, while that's still where they are. Returns how many changed."""
    changed = 0
    with conn:
        for table, column in (("scans", "library_path"), ("pages", "image_path")):
            rows = conn.execute(f"SELECT id, {column} FROM {table} WHERE {column} IS NOT NULL")
            for row_id, value in rows.fetchall():
                if not Path(value).is_absolute() and Path(value).exists():
                    conn.execute(
                        f"UPDATE {table} SET {column} = ? WHERE id = ?",
                        (str(Path(value).resolve()), row_id),
                    )
                    changed += 1
    return changed


def ingest(
    conn: sqlite3.Connection,
    settings: Settings,
    pipeline: Pipeline,
    path: Path,
    origin: Literal["watched", "added"] = "added",
) -> ImportResult:
    """Import a file, then read any of its pages not read yet (a scan that failed is retried).
    Many files are better imported first and read together (Pipeline.process_scans)."""
    r = import_file(conn, settings, path, origin)
    if needs_reading(conn, r):
        r.reading = pipeline.process_scan(conn, r.scan_id)
    return r


def needs_reading(conn: sqlite3.Connection, r: ImportResult) -> bool:
    """Whether an imported file's scan has pages still to read: new, or read before and failed."""
    if not (r.scan_id and r.pages):
        return False
    status = conn.execute("SELECT status FROM scans WHERE id = ?", (r.scan_id,)).fetchone()[0]
    return status != "read"


def _library_copy(settings: Settings, path: Path, sha: str) -> Path:
    """Lindley's own copy, named by its hash, checked against the original before it's trusted."""
    # Absolute: the database mustn't depend on the folder Lindley happens to run in.
    dest = settings.library_dir.resolve() / "scans" / sha[:2] / f"{sha}{path.suffix.lower()}"
    if not (dest.exists() and sha256_of(dest) == sha):
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        if sha256_of(dest) != sha:
            dest.unlink()
            raise OSError(f"The copy of {path.name} in the library didn't match the original")
    return dest


def _remove_original(path: Path) -> None:
    """Move mode: remove the original. One another program has open stays for now; it's a
    duplicate when it's next seen, and removed then."""
    try:
        path.unlink()
    except OSError as e:
        log.warning("Couldn't remove %s after reading it in: %s", path, e)


def _quarantine(settings: Settings, path: Path, result: ImportResult) -> ImportResult:
    """Put a failed file aside. Copy mode keeps the original where it was. A file already in
    quarantine (copy mode tries a failed file again each time Lindley starts) isn't copied
    again."""
    if not path.exists():
        return result
    folder = settings.quarantine_dir
    dest = folder / path.name
    if dest.exists():
        dest = dest.with_name(f"{path.stem}-{datetime.now(UTC):%Y%m%d%H%M%S%f}{path.suffix}")
    try:
        folder.mkdir(parents=True, exist_ok=True)
        if _in_quarantine(folder, path):
            if settings.move_files:
                path.unlink()
        elif settings.move_files:
            shutil.move(path, dest)
        else:
            shutil.copy2(path, dest)
    except OSError as e:
        result.error = f"{result.error}; also couldn't quarantine it: {e}"
    return result


def _in_quarantine(folder: Path, path: Path) -> bool:
    """A copy of this file is in quarantine already, under its name or a dated one."""
    stem, suffix = glob.escape(path.stem), glob.escape(path.suffix)
    try:
        size, sha = path.stat().st_size, None
        for q in [folder / path.name, *folder.glob(f"{stem}-*{suffix}")]:
            if q.is_file() and q.stat().st_size == size:
                sha = sha or sha256_of(path)
                if sha256_of(q) == sha:
                    return True
    except OSError:
        pass  # it can't be read: quarantine it anyway
    return False


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


def _png_ready(img: Image.Image) -> Image.Image:
    """The image in a mode PNG can store: CMYK and the like become RGB."""
    if img.mode in _PNG_MODES:
        return img
    try:
        return img.convert("RGBA" if "A" in img.mode else "RGB")
    except ValueError:  # no direct conversion, e.g. 32-bit float
        return img.convert("L").convert("RGB")


def _pdf_scale(page) -> float:
    """How much to enlarge a PDF page (in points, 1/72 inch) to render it.

    PDF_DPI, unless that makes the picture enormous: a scan put into a PDF at 72 dpi has a page
    four feet tall. Then the scan's own resolution (nothing it holds is lost), or failing that
    whatever fits MAX_RENDER_PX.
    """
    w, h = page.get_size()
    area = max(w * h, 1.0)
    scale = PDF_DPI / 72
    if area * scale**2 <= MAX_RENDER_PX:
        return scale
    native = _scan_scale(page)
    if native and area * native**2 <= MAX_RENDER_PX:
        return native
    return 0.999 * (MAX_RENDER_PX / area) ** 0.5  # rendering rounds the sides up


def _scan_scale(page) -> float | None:
    """Pixels per point of the biggest picture on a PDF page: a scanned page is one picture."""
    import pypdfium2.raw as pdfium_c

    best, scale = 0.0, None
    for obj in page.get_objects(filter=[pdfium_c.FPDF_PAGEOBJ_IMAGE], max_depth=3):
        left, bottom, right, top = obj.get_bounds()
        shown = (right - left) * (top - bottom)
        if shown > best:
            best, scale = shown, max(obj.get_px_size()) / max(right - left, top - bottom)
    return scale


def _in_words(e: Exception) -> str:
    """What went wrong opening a scan, for a person: Pillow and PDFium say it their own way
    ("Truncated File Read", "Data format error")."""
    from pypdfium2 import PdfiumError

    if isinstance(e, Image.DecompressionBombError):
        return (
            "The picture is too big to read safely on this computer"
            f" (over {Image.MAX_IMAGE_PIXELS * 2 // 1_000_000} million pixels)"
        )
    if isinstance(e, Image.UnidentifiedImageError):
        return "It isn't a picture Lindley can open. It may be damaged, or not what its name says"
    if isinstance(e, OSError) and "truncated" in str(e).lower():
        return "The file is cut short. It may be damaged, or not have finished copying"
    if isinstance(e, PdfiumError):
        if "password" in str(e).lower():
            return "The PDF is locked with a password. Save a copy without one, and add that"
        return "The PDF can't be opened. It may be damaged, or not what its name says"
    return str(e) or type(e).__name__


def _split(
    conn: sqlite3.Connection, settings: Settings, scan_id: int, copy: Path, mime: str | None
) -> int:
    """One pages row per page. Single images are read in place; others become page PNGs."""
    out = settings.library_dir.resolve() / "pages" / str(scan_id)
    rows: list[tuple] = []
    if mime == "application/pdf":
        import pypdfium2 as pdfium

        pdf = pdfium.PdfDocument(copy)
        try:
            out.mkdir(parents=True, exist_ok=True)
            for i in range(len(pdf)):
                scale = _pdf_scale(pdf[i])
                img = pdf[i].render(scale=scale).to_pil()
                dest = out / f"{i:04d}.png"
                dpi = round(scale * 72)
                img.save(dest, dpi=(dpi, dpi))
                rows.append((i, dest, img.width, img.height, dpi, _color_mode(img.mode)))
        finally:
            pdf.close()
    else:
        with Image.open(copy) as img:
            frames = getattr(img, "n_frames", 1)
            if frames == 1:
                w, h = img.size  # as a viewer shows it: an EXIF turn of 90° swaps them
                if exif_orientation(img) in (5, 6, 7, 8):
                    w, h = h, w
                rows.append((0, copy, w, h, _dpi(img), _color_mode(img.mode)))
            else:
                out.mkdir(parents=True, exist_ok=True)
                for i in range(frames):
                    img.seek(i)
                    dest = out / f"{i:04d}.png"
                    dpi = _dpi(img)
                    # Each page saved upright, as a viewer shows it: a PNG keeps no orientation.
                    frame = _png_ready(ImageOps.exif_transpose(img))
                    frame.save(dest, **({"dpi": (dpi, dpi)} if dpi else {}))
                    rows.append((i, dest, frame.width, frame.height, dpi, _color_mode(frame.mode)))
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
