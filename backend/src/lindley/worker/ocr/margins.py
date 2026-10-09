"""Page numbers in the margins, looked at again on their own.

Reading a whole page, Tesseract often drops a number standing alone above or below the
writing, or garbles it: a typed "5" came out as "WwW", a "2" and a "7" not at all. Its layout
step takes a lone mark at the edge for a speck, and one threshold for the whole page can lose a
faint one. Read alone, the same marks come out right: a typed "2" at 97%.

So the ink above the first line of writing and below the last is found, in the top and bottom
of the page (MARGIN), marks close together are taken as one (a "12"), and those the size of
typing are read again on their own, cropped and on white, as digits only: set one under another
on a sheet, for one more run of Tesseract (about 0.2 s a page). Pillow alone: the margins are
searched at a quarter of their size.

Only a number read at READ_AT or more is kept. On the first hand test's typescripts it read
every typed number that shows (7, 2, 7, 8) at 88-97% and nothing else above 26%; a typed "5"
whose top has faded reads as a "2" at up to 86% read other ways, so how the marks are made black
and white (CUT) and read (a sheet, --psm 6) were chosen against that. Numbers written by hand
beside them, in pencil or red ink, don't read: that's for the reading AI.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass

from PIL import Image, ImageFilter, ImageOps

MARGIN = 0.12  # page numbers sit in the top or bottom 12% of the page (clues.MARKER_EDGE)
SCALE = 4  # the margins are searched at a quarter of their size
INK = 25  # this much darker than the paper is ink, for finding marks...
CUT = 30  # ...and this much, for reading them: a faint typed digit keeps its shape
TALL = (0.8, 2.0)  # a mark's height, as a share of the writing's usual word height
WIDE = 4.0  # at most this many word heights wide: a number, not a line of writing
NEAR = 1.0  # marks closer than a word height apart on a row are one (see _joined)
SPECK = 0.3  # a mark under this share of a word height both ways is a speck, and left out
MOST = 8  # marks read on a page at most: more is a margin full of notes, not a number
PAD = 30  # white around each mark, as Tesseract reads best
READ_AT = 85  # a number read with less confidence than this isn't kept: a stray mark read
# as a "3" at 82% on the dev library, while every true one read at 85% or more


@dataclass(frozen=True)
class Mark:
    box: tuple[int, int, int, int]  # x, y, w, h on the upright page
    top: bool  # above the writing, else below it


def _paper(gray: Image.Image) -> int:
    """The paper's shade: most of a margin is paper, and the scanner's white bed beside a
    page lies above it, so it's taken low (the 40th centile), not at the middle."""
    hist = gray.histogram()
    want, seen = 0.4 * sum(hist), 0
    for shade, n in enumerate(hist):
        seen += n
        if seen >= want:
            return shade
    return 255


def _blobs(mask: Image.Image) -> list[tuple[int, int, int, int]]:
    """Boxes (x0, y0, x1, y1) of the groups of touching ink in a 1-bit mask."""
    w, h = mask.size
    px = mask.load()
    seen: set[tuple[int, int]] = set()
    out = []
    for y in range(h):
        for x in range(w):
            if not px[x, y] or (x, y) in seen:
                continue
            seen.add((x, y))
            todo, x0, y0, x1, y1 = [(x, y)], x, y, x, y
            while todo:
                a, b = todo.pop()
                x0, y0, x1, y1 = min(x0, a), min(y0, b), max(x1, a), max(y1, b)
                for c, d in ((a + 1, b), (a - 1, b), (a, b + 1), (a, b - 1)):
                    if 0 <= c < w and 0 <= d < h and px[c, d] and (c, d) not in seen:
                        seen.add((c, d))
                        todo.append((c, d))
            out.append((x0, y0, x1 + 1, y1 + 1))
    return out


def _joined(boxes: list[tuple[int, int, int, int]], usual: int) -> list[tuple[int, int, int, int]]:
    """Marks side by side on a row, close as letters, joined into one: the digits of a "12",
    or a signature's letters, which then come out too wide to be a number. A page number
    stands alone."""
    boxes = list(boxes)
    joined = True
    while joined:
        joined = False
        for i, a in enumerate(boxes):
            for j in range(i + 1, len(boxes)):
                b = boxes[j]
                gap = max(a[0], b[0]) - min(a[0] + a[2], b[0] + b[2])
                if gap <= NEAR * usual and a[1] < b[1] + b[3] and b[1] < a[1] + a[3]:
                    x, y = min(a[0], b[0]), min(a[1], b[1])
                    r, d = max(a[0] + a[2], b[0] + b[2]), max(a[1] + a[3], b[1] + b[3])
                    boxes[i] = (x, y, r - x, d - y)
                    del boxes[j]
                    joined = True
                    break
            if joined:
                break
    return boxes


def find(gray: Image.Image, words: list[dict]) -> list[Mark]:
    """Marks the size of typing in the margins of an upright page: above its first line of
    writing and below its last, within MARGIN of its top and bottom. `words`: the page's
    reading, for where the writing is and how tall its words are. Marks already read well
    (a word with letters, at 80% or more) are left out."""
    w, h = gray.size
    # Writing: rows of words read well, two or more, or one long one. A signature's typed name
    # under it is writing; a number or a pencil mark read as "Wo" or "erk" isn't
    writing = [x for x in words if x["conf"] >= 60 and sum(c.isalpha() for c in x["text"]) >= 2]
    lines = [
        ws for ws in _rows(writing) if len(ws) >= 2 or sum(c.isalpha() for c in ws[0]["text"]) >= 4
    ]
    if not lines:
        return []
    heights = sorted(x["bbox"][3] for ws in lines for x in ws)
    usual = heights[len(heights) // 2]
    first = min(x["bbox"][1] for x in lines[0])
    last = max(x["bbox"][1] + x["bbox"][3] for x in lines[-1])
    clear = [x["bbox"] for x in words if x["conf"] >= 80 and any(c.isalpha() for c in x["text"])]
    out = []
    for top, y0, y1 in (
        (True, 0, min(first - 8, int(MARGIN * h))),
        (False, max(last + 8, int((1 - MARGIN) * h)), h),
    ):
        if y1 - y0 < SCALE * 2:
            continue
        band = gray.crop((0, y0, w, y1)).filter(ImageFilter.MedianFilter(3))
        ink = _paper(band) - INK
        small = band.resize((max(1, w // SCALE), max(1, (y1 - y0) // SCALE)), Image.BOX)
        mask = small.point(lambda v, ink=ink: 255 if v < ink else 0).filter(
            ImageFilter.MaxFilter(3)
        )
        blobs = [
            (a * SCALE, y0 + b * SCALE, (c - a) * SCALE, (d - b) * SCALE)
            for a, b, c, d in _blobs(mask.convert("1"))
        ]
        for box in _joined([b for b in blobs if max(b[2], b[3]) >= SPECK * usual], usual):
            # Cut off where the margin starts: the tail of a letter, or of a signature, above it
            if (box[1] + box[3] >= y1 - SCALE) if top else (box[1] <= y0 + SCALE):
                continue
            if not TALL[0] * usual <= box[3] <= TALL[1] * usual or box[2] > WIDE * usual:
                continue
            if any(overlap(box, x) for x in clear):
                continue
            out.append(Mark(box, top))
    return out if len(out) <= MOST else []


def sheet(gray: Image.Image, marks: list[Mark]) -> tuple[Image.Image, list[int]]:
    """The marks cropped, made black on white, and set one under another on a sheet of their
    own, for one reading of them all. Returns the sheet and where each mark's row starts."""
    w, h = gray.size

    def around(box, by):  # within the page: Pillow fills beyond it with black
        x, y, bw, bh = box
        return max(0, x - by), max(0, y - by), min(w, x + bw + by), min(h, y + bh + by)

    crops = []
    for m in marks:
        crop = gray.crop(around(m.box, 6))
        cut = _paper(gray.crop(around(m.box, 60))) - CUT
        crops.append(
            ImageOps.expand(crop.point(lambda v, cut=cut: 0 if v < cut else 255), PAD, 255)
        )
    width = max(c.size[0] for c in crops)
    rows, y = [], 0
    for c in crops:
        rows.append(y)
        y += c.size[1]
    out = Image.new("L", (width, y), 255)
    for c, top in zip(crops, rows, strict=True):
        out.paste(c, (0, top))
    return out, rows


def numbers(marks: list[Mark], rows: list[int], read: list[dict]) -> list[tuple[Mark, str, float]]:
    """Each mark's number, from the reading of their sheet (`read`: its words, with boxes on
    the sheet), when it's a page number (1-300, as clues.read_number) read at READ_AT or more."""
    found: dict[int, list[dict]] = {}
    for w in read:
        i = bisect_right(rows, w["bbox"][1] + w["bbox"][3] / 2) - 1
        found.setdefault(i, []).append(w)
    out = []
    for i, ws in sorted(found.items()):
        text = "".join(w["text"] for w in sorted(ws, key=lambda w: w["bbox"][0]))
        conf = min(w["conf"] for w in ws)
        if 0 <= i < len(marks) and text.isdigit() and 0 < int(text) <= 300 and conf >= READ_AT:
            out.append((marks[i], text, conf))
    return out


def overlap(a, b) -> bool:
    return a[0] < b[0] + b[2] and b[0] < a[0] + a[2] and a[1] < b[1] + b[3] and b[1] < a[1] + a[3]


def _rows(words: list[dict]) -> list[list[dict]]:
    """Words in rows by their height on the page, top to bottom (as clues.rows)."""
    out: list[tuple[float, float, list[dict]]] = []
    for x in sorted(words, key=lambda x: x["bbox"][1] + x["bbox"][3] / 2):
        cy, bh = x["bbox"][1] + x["bbox"][3] / 2, x["bbox"][3]
        if out and abs(cy - out[-1][0]) <= 0.6 * max(out[-1][1], bh, 1):
            c, hh, ws = out[-1]
            ws.append(x)
            out[-1] = ((c * (len(ws) - 1) + cy) / len(ws), max(hh, bh), ws)
        else:
            out.append((cy, bh, [x]))
    return [ws for _, _, ws in out]
