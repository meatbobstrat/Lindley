from __future__ import annotations

import re
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

    def unsure_spans(self) -> list[list[int]]:
        return unsure_spans(self.text, self.words)


# A word Tesseract read with less confidence than this is marked as one it wasn't sure of.
UNSURE_BELOW = 60
# What the vision model writes, as its prompt asks: "[?]" after a word it's unsure of, and
# "[illegible]" for a word it can't read.
_MARKED = re.compile(r"(\S+?)[ \t]*\[\?\]|\[illegible\]")


def unsure_spans(text: str, words: list[dict] | None = None) -> list[list[int]]:
    """Where in `text` the words its reader wasn't sure of are, as [start, end] character
    offsets in order: words Tesseract gave less than UNSURE_BELOW, and words the vision model
    marked. The UI highlights them for a person to check."""
    spans = []
    at = 0
    for w in words or ():
        start = text.find(w["text"], at)
        if start < 0:
            continue
        at = start + len(w["text"])
        if 0 <= w["conf"] < UNSURE_BELOW:
            spans.append([start, at])
    for m in _MARKED.finditer(text):
        spans.append(list(m.span(1)) if m.group(1) else list(m.span()))
    return sorted(spans)


class OcrEngine(Protocol):
    name: str

    def recognize(self, image_path: Path) -> list[PageResult]: ...
