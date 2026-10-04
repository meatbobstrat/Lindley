"""Intake pipeline: each scan runs a series of steps, each recorded in the intake_steps table.

worker.intake does hash, exif and split; Pipeline checks each page image (image) and reads it
(ocr, vision). The facts and embed steps come later. See design/database.md for what each step
extracts. MATCH is not per scan: once new scans have settled, lindley.assembler.assemble runs once
for the Inbox.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from lindley.config import Settings
from lindley.db.database import connect
from lindley.providers import allowance
from lindley.providers.base import ProviderError, VisionProvider
from lindley.providers.registry import get_provider
from lindley.worker import image as pageimage
from lindley.worker.ocr.base import OcrEngine, PageResult, marked_confidence
from lindley.worker.ocr.tesseract import ORIENTATION_MIN_CONF, TesseractEngine
from lindley.worker.ocr.vision import VisionEngine


class Step(StrEnum):
    """Intake steps, in order. Matches the CHECK list on intake_steps.step."""

    HASH = "hash"  # sha256; a known hash is a duplicate file
    EXIF = "exif"  # scanner, scan time, file times
    SPLIT = "split"  # one page image per page of a PDF or TIFF
    IMAGE = "image"  # size, dpi, perceptual hash, paper colour, blank score, script
    OCR = "ocr"  # Tesseract reading of printed pages, and which way up they are
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


log = logging.getLogger(__name__)

STOP_AFTER_FAILURES = 3  # read_waiting stops after this many vision failures in a row
# When Tesseract is unsure which way up a page is, the turned reading must beat this many points.
TURN_MARGIN = 10

# Each page's latest vision step.
_LAST_VISION = (
    "SELECT s.* FROM intake_steps s WHERE s.step = 'vision' AND s.id ="
    " (SELECT max(id) FROM intake_steps WHERE page_id = s.page_id AND step = 'vision')"
)
# The page `p` is in a document a person completed
_COMPLETED = (
    "coalesce((SELECT d.status = 'complete' FROM documents d WHERE d.id = p.document_id), 0)"
)
# A person checked or corrected the page's text (v is a _LAST_VISION row): the vision model has
# nothing to add, so the page no longer waits for it.
_UNCHECKED = (
    "NOT EXISTS (SELECT 1 FROM v_current_text c WHERE c.page_id = v.page_id AND c.reviewed)"
)


@dataclass
class WaitingRun:
    """What read_waiting did."""

    read: int = 0
    failed: int = 0
    waiting: int = 0  # pages still waiting afterwards
    stopped: str | None = None  # why it stopped early, if it did
    error: str | None = None  # why the last call that failed did


def waiting_for_vision(conn: sqlite3.Connection) -> int:
    """Pages waiting for a person to OK sending them to the vision model."""
    sql = f"SELECT COUNT(*) FROM ({_LAST_VISION}) v WHERE v.status = 'queued' AND {_UNCHECKED}"
    return conn.execute(sql).fetchone()[0]


def vision_queue(conn: sqlite3.Connection, running: bool = False) -> list[sqlite3.Row]:
    """Pages waiting for the vision model, or whose vision call failed, with why and where:
    page_id, status ('queued' or 'failed'; 'running' too with `running`: being read now),
    error, file_name, confidence (the reading in use), document_id, document_name, position."""
    statuses = "('queued', 'failed', 'running')" if running else "('queued', 'failed')"
    return conn.execute(
        "SELECT v.page_id, v.status, v.error, s.original_name AS file_name, t.confidence,"
        " p.document_id, d.name AS document_name, p.position"
        f" FROM ({_LAST_VISION}) v JOIN pages p ON p.id = v.page_id"
        " JOIN scans s ON s.id = p.scan_id"
        " LEFT JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1"
        " LEFT JOIN documents d ON d.id = p.document_id"
        f" WHERE v.status IN {statuses} AND p.set_aside_at IS NULL AND {_UNCHECKED}"
        " ORDER BY p.document_id IS NULL, p.document_id, p.position, s.id, p.page_index"
    ).fetchall()


def vision_failures(conn: sqlite3.Connection) -> tuple[int, str | None]:
    """Pages whose last vision call failed (they wait for a person to retry), and an error."""
    sql = (
        f"SELECT COUNT(*), MAX(v.error) FROM ({_LAST_VISION}) v"
        f" WHERE v.status = 'failed' AND {_UNCHECKED}"
    )
    n, error = conn.execute(sql).fetchone()
    return n, error


def follow_settings(conn: sqlite3.Connection, settings: Settings) -> tuple[int, int]:
    """Bring the pages waiting for the vision model in line with the settings as they are now,
    as Settings shows them: a page whose reading in use is Tesseract's, below
    ocr.confidence_threshold, waits for the vision model; one read better, or checked by a
    person, doesn't. So a new threshold, or a vision model set up for the first time, counts for
    pages read before. With no vision model nothing waits: hard pages wait for a person's
    review instead. Pages in a completed document don't wait either: completing it was a
    person's word that its text is done. Pages being read, and those whose call failed while
    there's a model to try again with, are left as they are. An AI that may run on its own
    takes what's queued here the next time the watcher asks (lindley.assembler.auto).

    Returns (pages queued, pages that no longer wait).
    """
    ocr = settings.ocr
    if ocr.engine == "vision":
        return 0, 0  # every page goes to the vision model: the threshold doesn't come into it
    name = settings.ai.connection_for("vision")
    has_model = ocr.engine != "tesseract" and name is not None
    hard = (
        "c.source = 'tesseract' AND NOT c.reviewed AND coalesce(c.confidence, 0) < ?"
        " AND p.set_aside_at IS NULL AND coalesce(p.blank_score, 0) < ?"
        f" AND NOT {_COMPLETED}"
    )
    limits = (ocr.confidence_threshold, pageimage.BLANK_AT)
    with conn:
        waiting = conn.execute(
            "SELECT v.scan_id, v.page_id, v.status, coalesce(c.reviewed, 0) AS checked,"
            f" coalesce(c.confidence, 0) >= ? AS read_well, {_COMPLETED} AS completed"
            f" FROM ({_LAST_VISION}) v LEFT JOIN v_current_text c ON c.page_id = v.page_id"
            " JOIN pages p ON p.id = v.page_id"
            " WHERE v.status IN ('queued', 'failed')",
            (ocr.confidence_threshold,),
        ).fetchall()
        dropped = []
        for r in waiting:
            if not has_model:
                why = "No vision model is set up"
            elif r["checked"]:
                why = "A person checked the text"
            elif r["status"] == "queued" and r["completed"]:
                why = "Its document is completed"
            elif r["status"] == "queued" and r["read_well"]:
                why = "Read well enough"
            else:
                continue
            record_step(
                conn, r["scan_id"], Step.VISION, StepStatus.SKIPPED, page_id=r["page_id"], error=why
            )
            dropped.append(r["page_id"])
        if not has_model:
            return 0, len(dropped)
        wanted = conn.execute(
            "SELECT p.id, p.scan_id FROM pages p JOIN v_current_text c ON c.page_id = p.id"
            f" LEFT JOIN ({_LAST_VISION}) v ON v.page_id = p.id"
            f" WHERE {hard} AND (v.id IS NULL OR v.status = 'skipped')"
            " ORDER BY p.scan_id, p.page_index",
            limits,
        ).fetchall()
        why = allowance.why_waiting(conn, settings, name)
        conn.executemany(
            "INSERT INTO intake_steps (scan_id, page_id, step, status, error)"
            " VALUES (?, ?, 'vision', 'queued', ?)",
            [(r["scan_id"], r["id"], why) for r in wanted],
        )
    return len(wanted), len(dropped)


def recover_interrupted(conn: sqlite3.Connection) -> tuple[int, int]:
    """After Lindley stopped part way (closed, or the computer slept and was shut down): steps
    left running can't finish now. A vision call goes back in the queue, sent again on its own
    or waiting in Needs AI as any other; any other step is marked failed, and the scan's reading
    is picked up again (unfinished_scans). Run once at start-up, before any work.

    Returns (vision calls queued again, other steps marked failed).
    """
    with conn:
        vision = conn.execute(
            "UPDATE intake_steps SET status = 'queued', started_at = NULL, error = ?"
            " WHERE status = 'running' AND step = 'vision'",
            ("Lindley closed while the vision model was reading this page",),
        ).rowcount
        other = conn.execute(
            "UPDATE intake_steps SET status = 'failed', finished_at = datetime('now'), error = ?"
            " WHERE status = 'running'",
            ("Lindley closed before this step finished",),
        ).rowcount
    if vision or other:
        log.info("Picked up after an interruption: %d vision call(s), %d step(s)", vision, other)
    return vision, other


def unfinished_scans(conn: sqlite3.Connection) -> list[int]:
    """Scans whose reading never finished: interrupted, or failed. Pages waiting for the vision
    model aren't unfinished; they wait in Needs AI."""
    return [
        r[0]
        for r in conn.execute(
            "SELECT s.id FROM scans s WHERE s.status = 'reading'"
            " OR (s.status != 'read' AND EXISTS ("
            " SELECT 1 FROM pages p WHERE p.scan_id = s.id AND NOT EXISTS ("
            "  SELECT 1 FROM transcriptions t WHERE t.page_id = p.id AND t.is_current = 1)"
            " AND coalesce((SELECT v.status FROM intake_steps v WHERE v.page_id = p.id"
            "  AND v.step = 'vision' ORDER BY v.id DESC LIMIT 1), '')"
            "  NOT IN ('queued', 'failed', 'running')))"
            " ORDER BY s.id"
        )
    ]


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


class StepTaken(Exception):
    """Someone else (the watcher, or a person's request) got to this step first."""


@contextmanager
def run_step(
    conn: sqlite3.Connection,
    scan_id: int,
    step: Step,
    *,
    page_id: int | None = None,
    engine_version: str | None = None,
    queued: int | None = None,
    supersedes: int | None = None,
) -> Iterator[None]:
    """Record a step as running while the block runs, then as done, or failed with the error.

    `queued` is the id of a queued row to run, instead of adding a new one; `supersedes` is the
    id of the page's last row for this step, run again in a new one. Either is claimed in one
    statement, and raises StepTaken if another thread has claimed it since. The block should
    commit its own writes (`with conn:`) so a failure rolls them back.
    """
    with conn:
        if queued:
            row = queued
            claimed = conn.execute(
                "UPDATE intake_steps SET status = 'running', engine_version = ?, error = NULL,"
                " started_at = datetime('now') WHERE id = ? AND status = 'queued'",
                (engine_version, row),
            ).rowcount
        else:
            cur = conn.execute(
                "INSERT INTO intake_steps (scan_id, page_id, step, status, engine_version,"
                " started_at) SELECT ?, ?, ?, 'running', ?, datetime('now')"
                " WHERE ? IS NULL"
                " OR (SELECT max(id) FROM intake_steps WHERE page_id = ? AND step = ?) = ?",
                (scan_id, page_id, step, engine_version, supersedes, page_id, step, supersedes),
            )
            row, claimed = cur.lastrowid, cur.rowcount
    if not claimed:
        raise StepTaken(f"{step} for page {page_id} is already being done")
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

    The vision model can cost money, so it's only called on its own when its provider's `allow`
    is "auto", within its daily limit; otherwise pages wait until a person runs read_waiting.
    A vision call that failed is never repeated on its own either: that page waits too.
    """

    def __init__(
        self, settings: Settings, tesseract: OcrEngine, vision: VisionProvider | None = None
    ) -> None:
        self.settings = settings
        self.tesseract = tesseract
        self.vision = vision
        # Scans read side by side (process_scans) ask about the vision model one at a time, so
        # together they never make more calls on their own than its limits allow.
        self._vision_turn = threading.Lock()

    @property
    def vision_name(self) -> str | None:
        """The connection that reads hard pages (ai.jobs.vision)."""
        return self.settings.ai.connection_for("vision")

    @classmethod
    def from_settings(cls, settings: Settings, use_ai: bool = True) -> Pipeline:
        vision = None
        if use_ai and settings.ocr.engine != "tesseract" and settings.ai.connection_for("vision"):
            try:
                vision = get_provider(settings.ai, "vision")
            except ProviderError:
                vision = None
        return cls(settings, TesseractEngine(settings.ocr), vision)

    def process_scans(
        self,
        conn: sqlite3.Connection,
        scan_ids: list[int],
        *,
        stop: threading.Event | None = None,
        on_done: Callable[[int, str | None], None] | None = None,
    ) -> dict[int, str | None]:
        """Read several scans, a few at once (ocr.workers), each on a thread with a connection
        of its own. Returns each scan's new status (process_scan); None for one whose reading
        raised, which is logged and left for unfinished_scans to pick up. on_done(scan_id,
        status) is called on this thread as each finishes. Once `stop` is set, scans not
        started yet aren't, and aren't in the result.
        """
        workers = min(self.settings.ocr.reading_workers(), len(scan_ids))
        done: dict[int, str | None] = {}

        def finish(scan_id: int, status: str | None) -> None:
            done[scan_id] = status
            if on_done:
                on_done(scan_id, status)

        if workers <= 1:
            for scan_id in scan_ids:
                if stop and stop.is_set():
                    break
                finish(scan_id, self._process_logged(conn, scan_id))
            return done

        local, conns, made = threading.local(), [], threading.Lock()

        def read(scan_id: int) -> tuple[bool, str | None]:
            if stop and stop.is_set():
                return False, None
            if not hasattr(local, "conn"):
                local.conn = connect(self.settings.db_path, any_thread=True)
                with made:
                    conns.append(local.conn)
            return True, self._process_logged(local.conn, scan_id)

        try:
            with ThreadPoolExecutor(workers, thread_name_prefix="lindley-read") as pool:
                futures = {pool.submit(read, scan_id): scan_id for scan_id in scan_ids}
                for future in as_completed(futures):
                    started, status = future.result()
                    if started:
                        finish(futures[future], status)
        finally:
            for c in conns:
                c.close()
        return done

    def _process_logged(self, conn: sqlite3.Connection, scan_id: int) -> str | None:
        try:
            return self.process_scan(conn, scan_id)
        except Exception:
            log.exception("Reading scan %d stopped part way", scan_id)
            return None

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

    def check_upside_down(
        self, conn: sqlite3.Connection, stop: threading.Event | None = None
    ) -> tuple[int, int]:
        """Pages read before Lindley tried a poorly read page upside down (_read_any_way_up),
        queued once by the database upgrade to schema 10: each still read poorly by Tesseract,
        the way it was scanned, is tried turned over when the orientation check calls it
        upright, and the turn kept if it reads clearly better. A page a person checked, turned
        or completed, or that reads well now, is left alone. Each queued page is checked once;
        one cut off when Lindley closed isn't checked again.

        Returns (pages checked, pages turned).
        """
        rows = conn.execute(
            "SELECT s.id AS step, s.scan_id, p.id AS page_id, p.image_path, p.dpi,"
            " p.blank_score, p.detected_rotation, p.user_rotation, c.source, c.confidence,"
            f" coalesce(c.reviewed, 0) AS checked, {_COMPLETED} AS completed"
            " FROM intake_steps s JOIN pages p ON p.id = s.page_id"
            " LEFT JOIN v_current_text c ON c.page_id = p.id"
            " WHERE s.step = 'ocr' AND s.status = 'queued' ORDER BY s.id"
        ).fetchall()
        ocr = self.settings.ocr
        checked = turned = 0
        for r in rows:
            if stop and stop.is_set():
                break
            why = _leave_the_way_up(r, ocr)
            if why:
                with conn:
                    conn.execute(
                        "UPDATE intake_steps SET status = 'skipped', error = ?,"
                        " finished_at = datetime('now') WHERE id = ? AND status = 'queued'",
                        (why, r["step"]),
                    )
                continue
            version = getattr(self.tesseract, "version", self.tesseract.name)
            try:
                with run_step(
                    conn,
                    r["scan_id"],
                    Step.OCR,
                    page_id=r["page_id"],
                    engine_version=version,
                    queued=r["step"],
                ):
                    turned += self._turn_if_upside_down(conn, r)
            except StepTaken:
                continue
            except Exception:
                log.exception("Page %d: checking whether it's upside down failed", r["page_id"])
                continue
            checked += 1
        if turned:
            log.info("Turned %d page(s) read upside down before", turned)
            follow_settings(conn, self.settings)  # some may read well enough now
        return checked, turned

    def _turn_if_upside_down(self, conn: sqlite3.Connection, r: sqlite3.Row) -> bool:
        """check_upside_down for one page: True if it was turned over."""
        image = Path(r["image_path"])
        found = self._orientation(r["page_id"], image, 0, r["dpi"])
        if not found or found[0]:  # can't tell, or a turn tried when the page was first read
            return False
        with self._upright(r["page_id"], image, 180, r["dpi"]) as upright:
            reading = self.tesseract.recognize(upright)[0]
        if (reading.confidence or 0) < (r["confidence"] or 0) + TURN_MARGIN:
            return False
        version = getattr(self.tesseract, "version", self.tesseract.name)
        script = pageimage.classify_script(reading.words, r["blank_score"])
        with conn:
            conn.execute(
                "UPDATE transcriptions SET is_current = 0 WHERE page_id = ? AND is_current = 1",
                (r["page_id"],),
            )
            conn.execute(
                "INSERT INTO transcriptions (page_id, source, engine_model, text, confidence,"
                " unsure_spans, words, is_current) VALUES (?, 'tesseract', ?, ?, ?, ?, ?, 1)",
                (
                    r["page_id"],
                    version,
                    reading.text,
                    reading.confidence,
                    json.dumps(reading.unsure_spans()),
                    json.dumps(reading.words) if reading.words else None,
                ),
            )
            conn.execute(
                "UPDATE pages SET detected_rotation = 180, script = ?,"
                " updated_at = datetime('now') WHERE id = ?",
                (script, r["page_id"]),
            )
        return True

    def read_waiting(
        self,
        conn: sqlite3.Connection,
        page_ids: list[int] | None = None,
        retry_failed: bool = False,
        automatic: bool = False,
        limit: int | None = None,
        progress: Callable[[int, int], None] | None = None,
    ) -> WaitingRun:
        """Send the pages waiting for the vision model: a person asked, or, with `automatic`,
        Lindley sends them on its own because the connection may now run on its own (at most
        `limit` calls: what's left of its limits).

        Pages whose vision call failed are sent again only with retry_failed, which only a
        person asks for. It stops after STOP_AFTER_FAILURES failures in a row, so a broken
        provider isn't called page after page. A vision reading becomes current if it's better,
        but never replaces a person's text. `progress(done, of)` is called before each page.
        """
        if self.vision is None:
            raise RuntimeError("No vision model is set up")
        statuses = ("queued", "failed") if retry_failed else ("queued",)
        rows = conn.execute(
            "SELECT v.id, v.status, v.scan_id, v.page_id, p.image_path, p.dpi,"
            f" p.detected_rotation, p.user_rotation FROM ({_LAST_VISION}) v"
            " JOIN pages p ON p.id = v.page_id"
            f" WHERE v.status IN ({', '.join('?' * len(statuses))}) AND {_UNCHECKED}"
            " ORDER BY v.scan_id, p.page_index",
            statuses,
        ).fetchall()
        if page_ids is not None:
            rows = [r for r in rows if r["page_id"] in set(page_ids)]
        of = len(rows) if limit is None else min(len(rows), limit)
        model = getattr(self.vision, "model", None) or "vision"
        run = WaitingRun()
        in_a_row, scans, error = 0, set(), ""
        for done, r in enumerate(rows):
            if progress:
                progress(min(done, of), of)
            if limit is not None and run.read + run.failed >= limit:
                run.stopped = "Stopped at the limit of calls Lindley may make on its own"
                break
            if in_a_row >= STOP_AFTER_FAILURES:
                run.stopped = f"Stopped after {in_a_row} failures in a row: {error}"
                break
            rotation = (r["detected_rotation"] + r["user_rotation"]) % 360
            queued = r["id"] if r["status"] == "queued" else None
            started = time.monotonic()
            try:
                with run_step(
                    conn,
                    r["scan_id"],
                    Step.VISION,
                    page_id=r["page_id"],
                    engine_version=model,
                    queued=queued,
                    supersedes=None if queued else r["id"],
                ):
                    image = Path(r["image_path"])
                    with self._upright(r["page_id"], image, rotation, r["dpi"]) as upright:
                        try:
                            result = self._vision_reader().recognize(upright)[0]
                        except Exception:
                            self._record_call(conn, r["page_id"], automatic, ok=False)
                            raise
                    self._record_call(conn, r["page_id"], automatic, ok=True)
                    self._add_vision_reading(conn, r["page_id"], model, result)
            except StepTaken:
                continue  # the watcher or a person's request is reading it already
            except Exception as e:
                run.failed += 1
                in_a_row += 1
                error = run.error = str(e) or type(e).__name__
                log.warning("Page %d: the vision call failed: %s", r["page_id"], error)
                continue
            log.info(
                "Page %d read by %s in %.0f s", r["page_id"], model, time.monotonic() - started
            )
            run.read += 1
            in_a_row = 0
            scans.add(r["scan_id"])
        for scan_id in scans:
            status = conn.execute("SELECT status FROM scans WHERE id = ?", (scan_id,)).fetchone()
            if status[0] == "queued":
                self._settle(conn, scan_id)
        run.waiting = waiting_for_vision(conn)
        return run

    def _record_call(
        self, conn: sqlite3.Connection, page_id: int, automatic: bool, ok: bool
    ) -> None:
        """A vision call: one a person OKed isn't counted against the limits."""
        with conn:
            allowance.record(conn, self.vision_name, "vision", automatic, page_id, ok)

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
                " unsure_spans, is_current) VALUES (?, 'vision', ?, ?, ?, ?, ?)",
                (
                    page_id,
                    model,
                    result.text,
                    result.confidence,
                    json.dumps(result.unsure_spans()),
                    int(better),
                ),
            )

    def _read_page(self, conn: sqlite3.Connection, scan_id: int, page_id: int, image: Path) -> None:
        page = self._page(conn, page_id)
        if page["blank_score"] is None:  # not checked yet (a retry doesn't check it again)
            with run_step(
                conn, scan_id, Step.IMAGE, page_id=page_id, engine_version=pageimage.ENGINE
            ):
                self._check_image(conn, page_id, image, page["dpi"])
            page = self._page(conn, page_id)
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        first = None
        if self.settings.ocr.engine != "vision":
            rotation, first = self._read_any_way_up(conn, scan_id, page_id, image, page)
        with self._upright(page_id, image, rotation, page["dpi"]) as upright:
            self._read_upright(conn, scan_id, page_id, upright, page["blank_score"], first)

    def _read_any_way_up(
        self, conn: sqlite3.Connection, scan_id: int, page_id: int, image: Path, page: sqlite3.Row
    ) -> tuple[int, PageResult]:
        """Tesseract's reading, and the rotation that puts the page upright.

        Tesseract reads the page whichever way up it is and says which way that was, in one
        run; a page on its side is read again, turned upright. It only turns a page when it's
        sure, so a page it didn't turn that reads poorly may still be the wrong way up: its
        orientation check is asked for a guess, which the reading then decides (_try_turning).
        When the check says the page is upright, the guess is upside down: on typed pages lying
        upside down on the scanner, the check has said "upright" even when it was sure (on 344
        real scans, 4 typed pages read at 19-27 as they were and 56-79 turned). A turn a person
        set is trusted, and the page read that way.
        """
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        dpi = page["dpi"]
        oriented = getattr(self.tesseract, "oriented_reading", None)
        if oriented is None or page["user_rotation"]:
            return rotation, self._tesseract_reading(conn, scan_id, page_id, image, rotation, dpi)
        version = getattr(self.tesseract, "version", self.tesseract.name)
        with (
            run_step(conn, scan_id, Step.OCR, page_id=page_id, engine_version=version),
            self._upright(page_id, image, rotation, dpi) as upright,
        ):
            turn, reading = oriented(upright)
        if turn:
            self._detected(conn, page_id, (page["detected_rotation"] + turn) % 360)
            rotation = (rotation + turn) % 360
            if reading is None:  # on its side
                reading = self._tesseract_reading(conn, scan_id, page_id, image, rotation, dpi)
            return rotation, reading
        if (
            page["blank_score"] >= pageimage.BLANK_AT
            or (reading.confidence or 0) >= self.settings.ocr.confidence_threshold
        ):
            return rotation, reading
        found = self._orientation(page_id, image, rotation, dpi)
        if not found:  # it can't tell: too little text
            return rotation, reading
        return self._try_turning(conn, scan_id, page_id, image, page, reading, found[0] or 180)

    def _try_turning(
        self,
        conn: sqlite3.Connection,
        scan_id: int,
        page_id: int,
        image: Path,
        page: sqlite3.Row,
        as_is: PageResult,
        guess: int,
    ) -> tuple[int, PageResult]:
        """The page reads poorly as it is: read it turned the way Tesseract guessed too, and
        keep the turn only if it reads clearly better. Returns the rotation to use and its
        Tesseract reading."""
        rotation = (page["detected_rotation"] + page["user_rotation"]) % 360
        turned_to = (rotation + guess) % 360
        turned = self._tesseract_reading(conn, scan_id, page_id, image, turned_to, page["dpi"])
        if (turned.confidence or 0) < (as_is.confidence or 0) + TURN_MARGIN:
            return rotation, as_is
        self._detected(conn, page_id, (page["detected_rotation"] + guess) % 360)
        return turned_to, turned

    def _detected(self, conn: sqlite3.Connection, page_id: int, rotation: int) -> None:
        with conn:
            conn.execute(
                "UPDATE pages SET detected_rotation = ?, updated_at = datetime('now') WHERE id = ?",
                (rotation, page_id),
            )

    def _orientation(
        self, page_id: int, image: Path, rotation: int, dpi: int | None
    ) -> tuple[int, float] | None:
        """Tesseract's orientation check, on the page turned `rotation`, if it has one."""
        orientation = getattr(self.tesseract, "orientation", None)
        if orientation is None:
            return None
        with self._upright(page_id, image, rotation, dpi) as upright:
            return orientation(upright)

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
        # Named for this call: the watcher and a person's request may turn the same page.
        work = self.settings.processing_dir / f"page-{page_id}-{uuid.uuid4().hex[:8]}.png"
        return _removed_after(pageimage.upright_copy(image, work, rotation, dpi))

    def _check_image(
        self, conn: sqlite3.Connection, page_id: int, image: Path, dpi: int | None
    ) -> None:
        """Blank score, paper colour and hash. Tesseract finds which way up a page is as it
        reads it (_read_any_way_up); a page only the vision model reads is checked here, and
        turned when Tesseract is sure."""
        info = pageimage.analyse(image)
        rotation = 0
        if self.settings.ocr.engine == "vision" and info.blank_score < pageimage.BLANK_AT:
            found = self._orientation(page_id, image, 0, dpi)
            if found and found[1] >= ORIENTATION_MIN_CONF:
                rotation = found[0]
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
            with self._vision_turn:  # see __init__
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
                elif _last_vision_status(conn, page_id) in ("queued", "failed", "running"):
                    pass  # waiting for a person, or being read; never sent again on its own
                elif not allowance.may_call(
                    conn, self.settings, self.vision_name
                ) or allowance.failing(conn, self.vision_name):
                    with conn:
                        conn.execute(
                            "INSERT INTO intake_steps (scan_id, page_id, step, status, error)"
                            " VALUES (?, ?, 'vision', 'queued', ?)",
                            (
                                scan_id,
                                page_id,
                                allowance.why_waiting(conn, self.settings, self.vision_name),
                            ),
                        )
                else:
                    model = getattr(self.vision, "model", None) or "vision"
                    ok = False
                    try:
                        with run_step(
                            conn, scan_id, Step.VISION, page_id=page_id, engine_version=model
                        ):
                            readings.append(
                                ("vision", model, self._vision_reader().recognize(image)[0])
                            )
                        ok = True
                    except Exception:
                        pass  # recorded as failed; the page waits for a person to try again
                    finally:
                        with conn:
                            allowance.record(conn, self.vision_name, "vision", True, page_id, ok)

        if not readings:
            return  # vision only, and the page is waiting for the vision model
        best = max(range(len(readings)), key=lambda i: _preference(readings[i]))
        with conn:
            for i, (source, model, r) in enumerate(readings):
                conn.execute(
                    "INSERT INTO transcriptions (page_id, source, engine_model, text, confidence,"
                    " unsure_spans, words, is_current) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        page_id,
                        source,
                        model,
                        r.text,
                        r.confidence,
                        json.dumps(r.unsure_spans()),
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


def _leave_the_way_up(r: sqlite3.Row, ocr) -> str | None:
    """Why check_upside_down leaves a page as it is, or None to check it."""
    if ocr.engine == "vision":
        return "Reading is set to the vision model"
    if r["checked"] or r["source"] != "tesseract":
        return "A person checked the text, or it isn't Tesseract's"
    if r["completed"]:
        return "Its document is completed"
    if r["detected_rotation"] or r["user_rotation"]:
        return "It has been turned"
    if (r["confidence"] or 0) >= ocr.confidence_threshold:
        return "It reads well enough"
    if (r["blank_score"] or 0) >= pageimage.BLANK_AT:
        return "The page looks blank"
    return None


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
    """Which reading becomes current: one with text, then the more confident. A vision reading's
    confidence is the share of its words it didn't mark as unsure (marked_confidence), so one
    that's mostly [illegible] doesn't replace Tesseract's."""
    _, _, r = reading
    return bool(r.text.strip()), r.confidence or 0.0


def rate_vision_readings(conn: sqlite3.Connection) -> int:
    """Vision readings made before they had a confidence get one, from the words they marked,
    so a poor one waits for a person's review. Which reading is in use isn't changed. Run at
    start-up; returns the readings rated."""
    rows = conn.execute(
        "SELECT id, text FROM transcriptions WHERE source = 'vision' AND confidence IS NULL"
    ).fetchall()
    rated = [(c, r["id"]) for r in rows if (c := marked_confidence(r["text"] or "")) is not None]
    with conn:
        conn.executemany("UPDATE transcriptions SET confidence = ? WHERE id = ?", rated)
    return len(rated)
