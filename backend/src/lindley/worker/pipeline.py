"""Intake pipeline: each scan runs a series of steps, each recorded in the intake_steps table.

Stub: implemented in the worker phase. See design/database.md for what each step extracts.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum

from lindley.config import Settings


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
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def process_scan(self, scan_id: int) -> StepStatus:
        # TODO(worker phase): run each Step in order for the scan and its pages, writing an
        # intake_steps row per step. Hybrid reading: when settings.ocr.engine == "hybrid" and
        # Tesseract's confidence is below settings.ocr.confidence_threshold, run VISION and make
        # the better reading current. On failure, record the error on the step and the scan,
        # move the source to settings.quarantine_dir, and never alter the original file.
        # MATCH is not per scan: once new scans have settled (no new file for ~20 s), call
        # lindley.assembler.assemble(conn, settings.assembler, chat) once for the whole Inbox.
        raise NotImplementedError
