from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class PageResult:
    page_number: int
    text: str
    confidence: float | None  # 0-100
    engine: str
    words: list[dict] | None = None  # [{"text", "conf", "bbox": [x, y, w, h]}], when known


class OcrEngine(Protocol):
    name: str

    def recognize(self, image_path: Path) -> list[PageResult]: ...
