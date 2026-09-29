"""OCR via a vision-capable AI provider, used for handwriting and low-confidence pages."""

from __future__ import annotations

from pathlib import Path

from lindley.providers.base import VisionProvider
from lindley.worker.ocr.base import PageResult


class VisionEngine:
    name = "vision"

    def __init__(self, provider: VisionProvider) -> None:
        self.provider = provider

    def recognize(self, image_path: Path) -> list[PageResult]:
        result = self.provider.transcribe(image_path.read_bytes())
        return [
            PageResult(
                page_number=1, text=result.text, confidence=result.confidence, engine=self.name
            )
        ]
