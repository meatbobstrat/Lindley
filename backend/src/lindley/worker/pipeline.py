"""Intake pipeline: each scan runs a series of steps, each recorded in the intake_steps table.

Stub: implemented in the worker phase. See design/database.md for what each step extracts.
"""

from __future__ import annotations

from enum import StrEnum

from lindley.config import Settings


class Step(StrEnum):
    """Intake steps, in order. Matches the CHECK list on intake_steps.step."""

    HASH = "hash"      # sha256; a known hash is a duplicate file
    EXIF = "exif"      # scanner, scan time, file times
    SPLIT = "split"    # one page image per page of a PDF or TIFF
    IMAGE = "image"    # size, dpi, perceptual hash, paper colour, blank score, rotation, script
    OCR = "ocr"        # Tesseract reading of printed pages
    VISION = "vision"  # vision model for handwriting and low-confidence pages
    FACTS = "facts"    # dates, names, places, letterheads, page markers, first and last lines
    EMBED = "embed"    # text embedding for similarity
    MATCH = "match"    # lindley.assembler.assemble: evidence, Lindley documents, hints


class StepStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


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
