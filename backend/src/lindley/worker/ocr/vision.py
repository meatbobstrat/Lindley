"""OCR via a vision-capable AI provider, used for handwriting and low-confidence pages.

Pages are sent no bigger than they need to be: big scans are reduced so their longer side is
at most `max_side` pixels and sent as JPEG. That's plenty for reading, quicker to upload, and
keeps under providers' image limits (and their per-image charges).
"""

from __future__ import annotations

import io
from pathlib import Path

from PIL import Image

from lindley.providers.base import VisionProvider
from lindley.worker.image import open_upright
from lindley.worker.ocr.base import PageResult

MAX_SIDE = 2000
SEND_AS_IS = frozenset({"JPEG", "PNG", "WEBP"})  # formats every provider takes
MAX_BYTES = 4_000_000  # under the smallest provider limit (5 MB)
JPEG_QUALITY = 90


def image_bytes(path: Path, max_side: int = MAX_SIDE) -> bytes:
    """The page as it's sent: the file itself if it's small enough, otherwise a reduced JPEG."""
    with Image.open(path) as img:
        fits = max(img.size) <= max_side and img.format in SEND_AS_IS
    if fits and path.stat().st_size <= MAX_BYTES:
        return path.read_bytes()
    img = open_upright(path)
    img.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=JPEG_QUALITY)
    return buf.getvalue()


class VisionEngine:
    name = "vision"

    def __init__(self, provider: VisionProvider, max_side: int = MAX_SIDE) -> None:
        self.provider = provider
        self.max_side = max_side

    def recognize(self, image_path: Path) -> list[PageResult]:
        result = self.provider.transcribe(image_bytes(image_path, self.max_side))
        return [
            PageResult(
                page_number=1, text=result.text, confidence=result.confidence, engine=self.name
            )
        ]
