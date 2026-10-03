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
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from lindley.config import Settings
from lindley.providers.base import ProviderError, VisionProvider
from lindley.providers.registry import get_provider
from lindley.worker import image as pageimage
from lindley.worker.ocr.base import OcrEngine, PageResult
from lindley.worker.ocr.tesseract import ORIENTATION_MIN_CONF, TesseractEngine
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


WAITING = "Waiting for you to OK the vision model"
STOP_AFTER_FAILURES = 3  # read_waiting stops after this many vision failures in a row
# When Tesseract is unsure which way up a page is, the turned reading must beat this many points.
TURN_MARGIN = 10

# Each page's latest vision step.
_LAST_VISION = (
    "SELECT s.* FROM intake_steps s WHERE s.step = 'vision' AND s.id ="
    " (SELECT max(id) FROM intake_steps WHERE page_id = s.page_id AND step = 'vision')"
)


@dataclass
class WaitingRun:
    """What read_waiting did."""

    read: int = 0
    failed: int = 0
    waiting: int = 0  # pages still waiting afterwards
    stopped: str | None = None  # why it stopped early, if it did


def waiting_for_vision(conn: sqlite3.Connection) -> int:
    """Pages waiting for a person to OK sending them to the vision model."""
    sql = f"SELECT COUNT(*) FROM ({_LAST_VISION}) WHERE status = 'queued'"
    return conn.execute(sql).fetchone()[0]


def vision_failures(conn: sqlite3.Connection) -> tuple[int, str | None]:
    """Pages whose last vision call failed (they wait for a person to retry), and an error."""
    sql = f"SELECT COUNT(*), MAX(error) FROM ({_LAST_VISION}) WHERE status = 'failed'"
    n, error = conn.execute(sql).fetchone()
    return n, error


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
    queued: int | None = None,
) -> Iterator[None]:
    """Record a step as running while the block runs, then as done, or failed with the error.

    `queued` is the id of a queued row to run, instead of adding a new one. The block should
    commit its own writes (`with conn:`) so a failure rolls them back.
    """
    with conn:
        if queued:
            row = queued
            conn.execute(
                "UPDATE intake_steps SET status = 'running', engine_version = ?, error = NULL,"
                " started_at = datetime('now') WHERE id = ?",
                (engine_version, row),
            )
        else:
            row = conn.execute(
                "INSERT INTO intake_steps (scan_id, page_id, step, status, engine_version,"
                " started_at) VALUES (?, ?, ?, 'running', ?, datetime('now'))",
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

    Every reading is kept; the best one for each page is current. The scan ends `read`; `queued`
    if a page has no reading yet because it's waiting for the vision model; or `failed` with the
    error, and a failed scan can simply be processed again.

    The vision model can cost money, so it's only called on its own in `vision_mode = "auto"`.
    Otherwise pages wait until a person runs read_waiting. A vision call that failed is never
    repeated on its own either: that page waits too.
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
        """Read the scan's unread pages. Returns its new status: 'read', 'queued' or 'failed'."""
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
        return self._settle(conn, scan_id)

    def _settle(self, conn: sqlite3.Connection, scan_id: int) -> str:
        """'read' once every page has a reading; until then 'queued' (waiting for vision)."""
        unread = conn.execute(
            "SELECT COUNT(*) FROM pages p WHERE p.scan_id = ? AND NOT EXISTS ("
            " SELECT 1 FROM transcriptions t WHERE t.page_id = p.id AND t.is_current = 1)",
            (scan_id,),
        ).fetchone()[0]
        status = "queued" if unread else "read"
        with conn:
            conn.execute("UPDATE scans SET status = ? WHERE id = ?", (status, scan_id))
        return status

    def read_waiting(
        self,
        conn: sqlite3.Connection,
        page_ids: list[int] | None = None,
        retry_failed: bool = False,
    ) -> WaitingRun:
        """Send the pages waiting for the vision model. Only a person starts this.

        Pages whose vision call failed are sent again only with retry_failed. It stops after
        STOP_AFTER_FAILURES failures in a row, so a broken provider isn't called page after page.
        A vision reading becomes current if it's better, but never replaces a person's text.
        """
        if self.vision is None:
            raise RuntimeError("No vision model is set up")
        statuses = ("queued", "failed") if retry_failed else ("queued",)
        rows = conn.execute(
            "SELECT v.id, v.status, v.scan_id, v.page_id, p.image_path, p.dpi,"
            f" p.detected_rotation, p.user_rotation FROM ({_LAST_VISION}) v"
            " JOIN pages p ON p.id = v.page_id"
            f" WHERE v.status IN ({', '.join('?' * len(statuses))})"
            " ORDER BY v.scan_id, p.page_index",
            statuses,
        ).fetchall()
        if page_ids is not None:
            rows = [r for r in rows if r["page_id"] in set(page_ids)]
        model = getattr(self.vision, "model", None) or "vision"
        run = WaitingRun()
        in_a_row, scans, error = 0, set(), ""
        for r in rows:
            if in_a_row >= STOP_AFTER_FAILURES:
                run.stopped = f"Stopped after {in_a_row} failures in a row: {error}"
                break
            rotation = (r["detected_rotation"] + r["user_rotation"]) % 360
            queued = r["id"] if r["status"] == "queued" else None
            try:
                with run_step(
                    conn,
                    r["scan_id"],
                    Step.VISION,
                    page_id=r["page_id"],
                    engine_version=model,
                    queued=queued,
                ):
                    image = Path(r["image_path"])
                    with self._upright(r["page_id"], image, rotation, r["dpi"]) as upright:
                        result = self._vision_reader().recognize(upright)[0]
                    self._add_vision_reading(conn, r["page_id"], model, result)
            except Exception as e:
                run.failed += 1
                in_a_row += 1
                error = str(e) or type(e).__name__
                continue
            run.read += 1
            in_a_row = 0
            scans.add(r["scan_id"])
        for scan_id in scans:
            status = conn.execute("SELECT status FROM scans WHERE id = ?", (scan_id,)).fetchone()
            if status[0] == "queued":
                self._settle(conn, scan_id)
        run.waiting = waiting_for_vision(conn)
        return run

    def _add_vision_reading(
        self, conn: sqlite3.Connection, page_id: int, model: str, result: PageResult
    ) -> None:
        current = conn.execute(
            "SELECT id, source, text, confidence, confirmed_at FROM transcriptions"
            " WHERE page_id = ? AND is_current = 1",
            (page_id,),
        ).fetchone()
        better = current is None or (
            current["source"] != "user"
            and current["confirmed_at"] is None
            and _preference(("vision", model, result))
            > _preference(
                (
                    current["source"],
                    "",
                    PageResult(1, current["text"], current["confidence"], current["source"]),
                )
            )
        )
        with conn:
            if better and current:
                conn.execute(
                    "UPDATE transcriptions SET is_current = 0 WHERE id = ?", (current["id"],)
                )
            conn.execute(
                "INSERT INTO transcriptions (page_id, source, engine_model, text, confidence,"
                " is_current) VALUES (?, 'vision', ?, ?, ?, ?)",
                (page_id, model, result.text, result.confidence, int(better)),
            )

    def _read_page(self, conn: sqlite3.Connection, scan_id: int, page_id: int, image: Path) -> None:
        page = self._page(conn, page_id)
        guess = None  # a turn Tesseract suggested without being sure
        if page["blank_score"] is None:  # not checked yet (a retry doesn't check it again)
            with run_step(
                conn, scan_id, Step.IMAGE, page_id=page_id, engine_version=pageimage.ENGINE
            ):
                guess = self._check_image(conn, page_id, image, page["dpi"])
            page = self._page(conn, page_id)
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        first = None
        if guess and self.settings.ocr.engine != "vision":
            rotation, first = self._try_turning(conn, scan_id, page_id, image, page, guess)
        with self._upright(page_id, image, rotation, page["dpi"]) as upright:
            self._read_upright(conn, scan_id, page_id, upright, page["blank_score"], first)

    def _try_turning(
        self,
        conn: sqlite3.Connection,
        scan_id: int,
        page_id: int,
        image: Path,
        page: sqlite3.Row,
        guess: int,
    ) -> tuple[int, PageResult]:
        """Tesseract wasn't sure which way up the page is. If it reads poorly as it is, read it
        turned the way Tesseract guessed too, and keep the turn only if it reads clearly better.
        Returns the rotation to use and its Tesseract reading."""
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        as_is = self._tesseract_reading(conn, scan_id, page_id, image, rotation, page["dpi"])
        if (as_is.confidence or 0) >= self.settings.ocr.confidence_threshold:
            return rotation, as_is
        turned_to = (rotation + guess) % 360
        turned = self._tesseract_reading(conn, scan_id, page_id, image, turned_to, page["dpi"])
        if (turned.confidence or 0) < (as_is.confidence or 0) + TURN_MARGIN:
            return rotation, as_is
        with conn:
            conn.execute(
                "UPDATE pages SET detected_rotation = ?, updated_at = datetime('now') WHERE id = ?",
                ((page["detected_rotation"] + guess) % 360, page_id),
            )
        return turned_to, turned

    def _tesseract_reading(
        self,
        conn: sqlite3.Connection,
        scan_id: int,
        page_id: int,
        image: Path,
        rotation: int,
        dpi: int | None,
    ) -> PageResult:
        version = getattr(self.tesseract, "version", self.tesseract.name)
        with (
            run_step(conn, scan_id, Step.OCR, page_id=page_id, engine_version=version),
            self._upright(page_id, image, rotation, dpi) as upright,
        ):
            return self.tesseract.recognize(upright)[0]

    def _vision_reader(self) -> VisionEngine:
        return VisionEngine(self.vision, self.settings.ocr.vision_max_side)

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
    ) -> int | None:
        """Blank score, paper colour and hash; and, for a page with writing, which way up it is.

        Returns a turn Tesseract suggested without being sure, for the reading to decide.
        """
        info = pageimage.analyse(image)
        rotation, guess = 0, None
        orientation = getattr(self.tesseract, "orientation", None)
        if orientation and info.blank_score < pageimage.BLANK_AT:
            with self._upright(page_id, image, 0, dpi) as upright:
                found = orientation(upright)
            if found and found[1] >= ORIENTATION_MIN_CONF:
                rotation = found[0]
            elif found and found[0]:
                guess = found[0]
        with conn:
            conn.execute(
                "UPDATE pages SET phash = ?, paper_color = ?, blank_score = ?,"
                " detected_rotation = ?, updated_at = datetime('now') WHERE id = ?",
                (info.phash, info.paper_color, info.blank_score, rotation, page_id),
            )
        return guess

    def _read_upright(
        self,
        conn: sqlite3.Connection,
        scan_id: int,
        page_id: int,
        image: Path,
        blank_score: float,
        first: PageResult | None = None,  # Tesseract's reading, if already made (_try_turning)
    ) -> None:
        ocr = self.settings.ocr
        blank = blank_score >= pageimage.BLANK_AT
        readings: list[tuple[str, str, PageResult]] = []  # (source, engine_model, result)
        if ocr.engine != "vision":
            version = getattr(self.tesseract, "version", self.tesseract.name)
            if first is None:
                with run_step(conn, scan_id, Step.OCR, page_id=page_id, engine_version=version):
                    first = self.tesseract.recognize(image)[0]
            readings.append(("tesseract", version, first))

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
            elif _last_vision_status(conn, page_id) in ("queued", "failed"):
                pass  # already waiting for a person; never sent again on its own
            elif ocr.vision_mode == "ask":
                with conn:
                    conn.execute(
                        "INSERT INTO intake_steps (scan_id, page_id, step, status, error)"
                        " VALUES (?, ?, 'vision', 'queued', ?)",
                        (scan_id, page_id, WAITING),
                    )
            else:
                model = getattr(self.vision, "model", None) or "vision"
                try:
                    with run_step(
                        conn, scan_id, Step.VISION, page_id=page_id, engine_version=model
                    ):
                        readings.append(
                            ("vision", model, self._vision_reader().recognize(image)[0])
                        )
                except Exception:
                    pass  # recorded as failed; the page waits for a person to try again

        if not readings:
            return  # vision only, and the page is waiting for the vision model
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


def _last_vision_status(conn: sqlite3.Connection, page_id: int) -> str | None:
    row = conn.execute(
        "SELECT status FROM intake_steps WHERE page_id = ? AND step = 'vision'"
        " ORDER BY id DESC LIMIT 1",
        (page_id,),
    ).fetchone()
    return row[0] if row else None


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
