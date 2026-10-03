"""Intake pipeline: each scan runs a series of steps, each recorded in the intake_steps table.

worker.intake does hash, exif and split; Pipeline checks each page image (image) and reads it
(ocr, vision). The facts and embed steps come later. See design/database.md for what each step
extracts. MATCH is not per scan: once new scans have settled, lindley.assembler.assemble runs once
for the Inbox.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from lindley.config import Settings
from lindley.providers.base import ProviderError, VisionProvider
from lindley.providers.registry import get_provider
from lindley.worker import image as pageimage
from lindley.worker.ocr.base import OcrEngine, PageResult
from lindley.worker.ocr.tesseract import TesseractEngine
from lindley.worker.ocr.vision import VisionEngine


class Step(StrEnum):
    """Intake steps, in order. Matches the CHECK list on intake_steps.step."""

    HASH = "hash"  # sha256; a known hash is a duplicate file
    EXIF = "exif"  # scanner, scan time, file times
    SPLIT = "split"  # one page image per page of a PDF or TIFF
    IMAGE = "image"  # size, dpi, perceptual hash, paper colour, blank score, rotation, script
    OCR = "ocr"  # Tesseract reading of printed pages
    VISION = "vision"  # vision model for handwriting and low-confidence pages
    FACTS = "facts"  # dates, names, places, letterheads, page markers, first and last lines
    EMBED = "embed"  # text embedding for similarity
    MATCH = "match"  # lindley.assembler.assemble: evidence, Lindley documents, hints


class StepStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


def now() -> str:
    """UTC timestamp in SQLite's datetime('now') format."""
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def record_step(
    conn: sqlite3.Connection,
    scan_id: int,
    step: Step,
    status: StepStatus,
    *,
    page_id: int | None = None,
    engine_version: str | None = None,
    started_at: str | None = None,
    error: str | None = None,
) -> int:
    """Write one finished intake_steps row (the caller commits)."""
    return conn.execute(
        "INSERT INTO intake_steps (scan_id, page_id, step, status, engine_version, started_at,"
        " finished_at, error) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), ?)",
        (scan_id, page_id, step, status, engine_version, started_at or now(), error),
    ).lastrowid


@contextmanager
def run_step(
    conn: sqlite3.Connection,
    scan_id: int,
    step: Step,
    *,
    page_id: int | None = None,
    engine_version: str | None = None,
) -> Iterator[None]:
    """Record a step as running while the block runs, then as done, or failed with the error.

    The block should commit its own writes (`with conn:`) so a failure rolls them back.
    """
    with conn:
        row = conn.execute(
            "INSERT INTO intake_steps (scan_id, page_id, step, status, engine_version, started_at)"
            " VALUES (?, ?, ?, 'running', ?, datetime('now'))",
            (scan_id, page_id, step, engine_version),
        ).lastrowid
    try:
        yield
    except Exception as e:
        with conn:
            conn.execute(
                "UPDATE intake_steps SET status = 'failed', error = ?,"
                " finished_at = datetime('now')"
                " WHERE id = ?",
                (str(e) or type(e).__name__, row),
            )
        raise
    with conn:
        conn.execute(
            "UPDATE intake_steps SET status = 'done', finished_at = datetime('now') WHERE id = ?",
            (row,),
        )


class Pipeline:
    """Reads every page of an imported scan (Tesseract, with the vision model for hard pages).

    Every reading is kept; the best one for each page is current. The scan ends `read`, or
    `failed` with the error, and a failed scan can simply be processed again.
    """

    def __init__(
        self, settings: Settings, tesseract: OcrEngine, vision: VisionProvider | None = None
    ) -> None:
        self.settings = settings
        self.tesseract = tesseract
        self.vision = vision

    @classmethod
    def from_settings(cls, settings: Settings, use_ai: bool = True) -> Pipeline:
        vision = None
        if use_ai and settings.ocr.engine != "tesseract" and settings.ocr.vision_provider:
            try:
                vision = get_provider(settings.ai, settings.ocr.vision_provider)
            except ProviderError:
                vision = None
        return cls(settings, TesseractEngine(settings.ocr), vision)

    def process_scan(self, conn: sqlite3.Connection, scan_id: int) -> str:
        """Read the scan's unread pages. Returns the scan's new status: 'read' or 'failed'."""
        with conn:
            conn.execute(
                "UPDATE scans SET status = 'reading', error = NULL WHERE id = ?", (scan_id,)
            )
        pages = conn.execute(
            "SELECT p.id, p.image_path FROM pages p WHERE p.scan_id = ? AND NOT EXISTS ("
            " SELECT 1 FROM transcriptions t WHERE t.page_id = p.id AND t.is_current = 1)"
            " ORDER BY p.page_index",
            (scan_id,),
        ).fetchall()
        try:
            for page in pages:
                self._read_page(conn, scan_id, page["id"], Path(page["image_path"]))
        except Exception as e:
            with conn:
                conn.execute(
                    "UPDATE scans SET status = 'failed', error = ? WHERE id = ?",
                    (str(e) or type(e).__name__, scan_id),
                )
            return "failed"
        with conn:
            conn.execute("UPDATE scans SET status = 'read' WHERE id = ?", (scan_id,))
        return "read"

    def _read_page(self, conn: sqlite3.Connection, scan_id: int, page_id: int, image: Path) -> None:
        page = self._page(conn, page_id)
        if page["blank_score"] is None:  # not checked yet (a retry doesn't check it again)
            with run_step(
                conn, scan_id, Step.IMAGE, page_id=page_id, engine_version=pageimage.ENGINE
            ):
                self._check_image(conn, page_id, image, page["dpi"])
            page = self._page(conn, page_id)
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        with self._upright(page_id, image, rotation, page["dpi"]) as upright:
            self._read_upright(conn, scan_id, page_id, upright, page["blank_score"])

    def _page(self, conn: sqlite3.Connection, page_id: int) -> sqlite3.Row:
        return conn.execute(
            "SELECT dpi, blank_score, detected_rotation, user_rotation FROM pages WHERE id = ?",
            (page_id,),
        ).fetchone()

    def _upright(self, page_id: int, image: Path, rotation: int, dpi: int | None):
        """The page to read: the image itself, or a turned copy that's removed afterwards."""
        if not pageimage.needs_turning(image, rotation):
            return nullcontext(image)
        work = self.settings.processing_dir / f"page-{page_id}.png"
        return _removed_after(pageimage.upright_copy(image, work, rotation, dpi))

    def _check_image(
        self, conn: sqlite3.Connection, page_id: int, image: Path, dpi: int | None
    ) -> None:
        """Blank score, paper colour and hash; and, for a page with writing, which way up it is."""
        info = pageimage.analyse(image)
        rotation = 0
        orientation = getattr(self.tesseract, "orientation", None)
        if orientation and info.blank_score < pageimage.BLANK_AT:
            with self._upright(page_id, image, 0, dpi) as upright:
                rotation = orientation(upright) or 0
        with conn:
            conn.execute(
                "UPDATE pages SET phash = ?, paper_color = ?, blank_score = ?,"
                " detected_rotation = ?, updated_at = datetime('now') WHERE id = ?",
                (info.phash, info.paper_color, info.blank_score, rotation, page_id),
            )

    def _read_upright(
        self,
        conn: sqlite3.Connection,
        scan_id: int,
        page_id: int,
        image: Path,
        blank_score: float,
    ) -> None:
        ocr = self.settings.ocr
        blank = blank_score >= pageimage.BLANK_AT
        readings: list[tuple[str, str, PageResult]] = []  # (source, engine_model, result)
        if ocr.engine != "vision":
            version = getattr(self.tesseract, "version", self.tesseract.name)
            with run_step(conn, scan_id, Step.OCR, page_id=page_id, engine_version=version):
                readings.append(("tesseract", version, self.tesseract.recognize(image)[0]))

        conf = readings[0][2].confidence if readings else None
        if blank and readings:
            with conn:
                record_step(
                    conn,
                    scan_id,
                    Step.VISION,
                    StepStatus.SKIPPED,
                    page_id=page_id,
                    error="The page looks blank",
                )
        elif ocr.engine == "vision" or conf is None or conf < ocr.confidence_threshold:
            if self.vision is None:
                if ocr.engine == "vision":
                    raise RuntimeError("Reading is set to the vision model, but none is set up")
                with conn:
                    record_step(
                        conn,
                        scan_id,
                        Step.VISION,
                        StepStatus.SKIPPED,
                        page_id=page_id,
                        error="No vision model is set up",
                    )
            else:
                model = getattr(self.vision, "model", None) or "vision"
                try:
                    with run_step(
                        conn, scan_id, Step.VISION, page_id=page_id, engine_version=model
                    ):
                        readings.append(
                            ("vision", model, VisionEngine(self.vision).recognize(image)[0])
                        )
                except Exception:
                    if ocr.engine == "vision" or not readings:
                        raise  # nothing else to fall back on

        best = max(range(len(readings)), key=lambda i: _preference(readings[i]))
        with conn:
            for i, (source, model, r) in enumerate(readings):
                conn.execute(
                    "INSERT INTO transcriptions (page_id, source, engine_model, text, confidence,"
                    " words, is_current) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        page_id,
                        source,
                        model,
                        r.text,
                        r.confidence,
                        json.dumps(r.words) if r.words else None,
                        int(i == best),
                    ),
                )
            tesseract = next((r for source, _, r in readings if source == "tesseract"), None)
            script = pageimage.classify_script(tesseract and tesseract.words, blank_score)
            language = ocr.languages[0] if len(ocr.languages) == 1 else None
            conn.execute(
                "UPDATE pages SET script = ?, language = coalesce(?, language),"
                " updated_at = datetime('now') WHERE id = ?",
                (script, language, page_id),
            )


@contextmanager
def _removed_after(path: Path) -> Iterator[Path]:
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


def _preference(reading: tuple[str, str, PageResult]) -> tuple[bool, float]:
    """Which reading becomes current. Vision is only asked when Tesseract struggled, so a vision
    reading with text wins unless it reports a lower confidence than Tesseract's."""
    source, _, r = reading
    has_text = bool(r.text.strip())
    if source == "vision":
        return has_text, r.confidence if r.confidence is not None else 100.0
    return has_text, r.confidence or 0.0
