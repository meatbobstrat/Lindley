"""Local OCR via Tesseract / OCRmyPDF (requires Tesseract and Ghostscript installed).

Stub: implemented in the worker phase.
"""

from __future__ import annotations

from pathlib import Path

from lindley.config import OcrSettings
from lindley.worker.ocr.base import PageResult


class TesseractEngine:
    name = "tesseract"

    def __init__(self, settings: OcrSettings) -> None:
        self.settings = settings

    def recognize(self, image_path: Path) -> list[PageResult]:
        raise NotImplementedError

    def make_searchable_pdf(self, source: Path, output: Path) -> None:
        raise NotImplementedError
