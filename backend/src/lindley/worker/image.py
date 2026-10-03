"""Image checks for a page: how blank it is, the paper colour, a hash to spot near-duplicates,
and (once Tesseract has read it) whether the writing looks handwritten or printed.

Pillow only. The checks run on a reduced copy with the edges cropped off, so they're quick and
aren't thrown by a scanner's dark borders or shadows. The thresholds are a first guess, to be
tuned on real scans.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageFilter, ImageOps, ImageStat

ENGINE = "image v1"  # intake_steps.engine_version for the image step
WORK_SIZE = 1600  # long side of the reduced copy, in pixels
MARGIN = 0.03  # share of each edge cropped off before looking for ink
INK_LEVEL = 0.7  # darker than this share of the paper's brightness counts as ink
INK_FULL = 0.02  # this share of inked pixels scores 0; a blank page has well under 0.1%
BLANK_AT = 0.97  # a blank_score at or above this is a blank page (as the assembler uses)
DUP_BITS = 4  # hashes this close (out of 64 bits) are the same picture

# Script: a line Tesseract read with at least LINE_PRINTED mean confidence looks printed, one
# below LINE_HAND looks handwritten. MOSTLY of the lines decides; SHARE of each kind is mixed.
LINE_PRINTED = 75
LINE_HAND = 50
MOSTLY = 0.7
SHARE = 0.2


@dataclass(frozen=True)
class ImageInfo:
    phash: str  # 64-bit difference hash, 16 hex digits
    paper_color: str  # #rrggbb
    blank_score: float  # 0 = full page of writing, 1 = blank


def open_upright(path: Path) -> Image.Image:
    """The image as a viewer shows it: EXIF orientation applied, as RGB."""
    with Image.open(path) as img:
        img.load()
        if img.mode.startswith("I"):  # 16-bit grey would clip to white in a plain convert
            img = img.convert("I").point(lambda v: v * (1 / 256)).convert("L")
        return ImageOps.exif_transpose(img).convert("RGB")


def analyse(path: Path) -> ImageInfo:
    img = open_upright(path)
    img.thumbnail((WORK_SIZE, WORK_SIZE))
    w, h = img.size
    dx, dy = round(w * MARGIN), round(h * MARGIN)
    img = img.crop((dx, dy, w - dx, h - dy))
    # Dust goes, thin pen strokes stay: a pixel is dark if 3 of the 9 around it are.
    gray = img.convert("L").filter(ImageFilter.RankFilter(3, 2))

    hist = gray.histogram()
    paper = _percentile(hist, 0.9)
    ink_below = paper * INK_LEVEL
    ink = sum(hist[: int(ink_below)])
    ratio = ink / max(sum(hist), 1)
    blank = round(1 - min(ratio / INK_FULL, 1.0), 3)

    mask = gray.point(lambda v: 255 if v >= paper * 0.9 else 0)
    if not mask.getbbox():
        mask = None
    r, g, b = (round(c) for c in ImageStat.Stat(img, mask).mean[:3])
    return ImageInfo(dhash(gray), f"#{r:02x}{g:02x}{b:02x}", blank)


def dhash(img: Image.Image) -> str:
    """Difference hash: does each cell of a 9x8 grey thumbnail get darker to the right."""
    small = img.convert("L").resize((9, 8), Image.Resampling.BOX)
    px = small.load()
    bits = 0
    for y in range(8):
        for x in range(8):
            bits = bits << 1 | (px[x, y] > px[x + 1, y])
    return f"{bits:016x}"


def hamming(a: str | None, b: str | None) -> int | None:
    """How many of the 64 bits differ, or None if either isn't a hash."""
    try:
        if a is None or b is None or len(a) != 16 or len(b) != 16:
            return None
        return (int(a, 16) ^ int(b, 16)).bit_count()
    except ValueError:
        return None


def classify_script(words: list[dict] | None, blank_score: float | None) -> str | None:
    """'none', 'printed', 'handwritten' or 'mixed' from Tesseract's words; None when unsure.

    Tesseract reads print with high confidence and handwriting poorly, so confidence line by
    line is a fair first guess. A printed letterhead over a handwritten letter is 'handwritten'
    unless the print makes up a real share of the lines. Typed and printed aren't told apart.
    """
    if blank_score is not None and blank_score >= BLANK_AT:
        return "none"
    lines = _line_confidences(words or [])
    if not lines:
        return None  # ink Tesseract couldn't read: handwriting, a drawing or a picture
    printed = sum(c >= LINE_PRINTED for c in lines) / len(lines)
    hand = sum(c < LINE_HAND for c in lines) / len(lines)
    if printed >= MOSTLY:
        return "printed"
    if hand >= MOSTLY:
        return "handwritten"
    if printed >= SHARE and hand >= SHARE:
        return "mixed"
    return None


def _line_confidences(words: list[dict]) -> list[float]:
    """Mean word confidence per line, lines found from the word boxes."""
    lines: list[tuple[float, float, list[float]]] = []  # (centre y, height, confidences)
    for wd in sorted(words, key=lambda wd: wd["bbox"][1] + wd["bbox"][3] / 2):
        _, y, _, hgt = wd["bbox"]
        mid = y + hgt / 2
        if lines and abs(mid - lines[-1][0]) <= max(lines[-1][1], hgt) / 2:
            lines[-1][2].append(wd["conf"])
        else:
            lines.append((mid, hgt, [wd["conf"]]))
    return [sum(cs) / len(cs) for _, _, cs in lines]


def _percentile(hist: list[int], q: float) -> int:
    target = sum(hist) * q
    seen = 0
    for level, n in enumerate(hist):
        seen += n
        if seen >= target:
            return level
    return len(hist) - 1
