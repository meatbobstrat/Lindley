"""Job pipeline: queued -> ocr -> pdf -> indexed, or -> quarantined on failure.

Stub: implemented in the worker phase.
"""

from __future__ import annotations

from enum import StrEnum

from lindley.config import Settings


class JobStatus(StrEnum):
    QUEUED = "queued"
    OCR = "ocr"
    PDF = "pdf"
    INDEXED = "indexed"
    QUARANTINED = "quarantined"


class Pipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def process_job(self, job_id: int) -> JobStatus:
        # TODO(worker phase):
        # 1. OCR each page with Tesseract (worker.ocr.tesseract).
        # 2. Hybrid routing: when settings.ocr.engine == "hybrid" and a page's confidence is
        #    below settings.ocr.confidence_threshold, re-transcribe it with the vision
        #    provider (worker.ocr.vision) and keep the better result.
        # 3. Write the searchable PDF into settings.library_dir.
        # 4. Store pages in the DB (the FTS index updates via triggers).
        # 5. On any failure, move the source to settings.quarantine_dir and record the error.
        raise NotImplementedError
