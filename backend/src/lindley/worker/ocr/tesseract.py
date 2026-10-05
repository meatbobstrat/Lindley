"""Local OCR with the Tesseract command-line program (no Python wrapper needed).

Tesseract's TSV output gives each word with its box and confidence; the words are what the
assembler uses to find page numbers standing alone at the top or bottom of a page.

A page is read whichever way up it is in one run (--psm 1: Tesseract's orientation check, then
its reading), and its hOCR output says which way that was. That's quicker than checking first
(--psm 0) and reading after: the check alone takes about as long as a third of the reading.

Tesseract turns a page black and white with one threshold for the whole page. A scan laid on a
white page (a PDF made from scans often does this) can fool it: the threshold falls between the
white edge and the tan paper, and faint typing vanishes. When a reading finds next to nothing,
the page is read again with a threshold that adapts across the page, and the fuller reading kept.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from collections import Counter
from functools import cache
from pathlib import Path

from PIL import Image, ImageOps

from lindley.config import OcrSettings
from lindley.worker.ocr.base import PageResult

# Where the UB-Mannheim installer puts it when it isn't added to PATH.
WINDOWS_DEFAULT = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
TIMEOUT_S = 300
# Below this orientation confidence, Tesseract's answer is only a guess (see orientation).
ORIENTATION_MIN_CONF = 2.0
# A reading with fewer words than this is tried again with an adaptive threshold.
FEW_WORDS = 20
ADAPTIVE = ["-c", "thresholding_method=1"]  # Leptonica's adaptive Otsu


class TesseractNotFound(RuntimeError):
    pass


def parse_tsv(tsv: str) -> tuple[str, list[dict], float | None]:
    """Tesseract TSV -> (text, words, mean word confidence 0-100 or None if no words).

    Words are {"text", "conf", "bbox": [x, y, w, h]}. Text keeps Tesseract's lines, with a blank
    line between paragraphs.
    """
    lines: dict[tuple[int, int, int, int], list[str]] = {}
    words: list[dict] = []
    for row in tsv.splitlines()[1:]:
        cols = row.split("\t")
        if len(cols) < 12 or cols[0] != "5":  # level 5 is a word
            continue
        text = cols[11].strip()
        conf = float(cols[10])
        if not text or conf < 0:
            continue
        page, block, par, line = (int(c) for c in cols[1:5])
        x, y, w, h = (int(c) for c in cols[6:10])
        lines.setdefault((page, block, par, line), []).append(text)
        words.append({"text": text, "conf": round(conf, 1), "bbox": [x, y, w, h]})

    out: list[str] = []
    last_par = None
    for key, ws in lines.items():  # TSV rows are already in reading order
        if last_par is not None and key[:3] != last_par:
            out.append("")
        out.append(" ".join(ws))
        last_par = key[:3]
    mean = round(sum(w["conf"] for w in words) / len(words), 1) if words else None
    return "\n".join(out), words, mean


def parse_osd(out: str) -> tuple[int, float] | None:
    """--psm 0 output -> (degrees clockwise to turn the page upright, confidence), if it says."""
    rotate = re.search(r"^Rotate:\s*(\d+)", out, re.M)
    conf = re.search(r"^Orientation confidence:\s*([\d.]+)", out, re.M)
    if not (rotate and conf) or int(rotate.group(1)) not in (0, 90, 180, 270):
        return None
    return int(rotate.group(1)), float(conf.group(1))


_HOCR_PAGE = re.compile(r"class='ocr_page'[^>]*?bbox (\d+) (\d+) (\d+) (\d+)")
_HOCR_LINE = re.compile(r"class='ocr_(?:line|header|textfloat|caption)'[^>]*title=\"([^\"]*)\"")


def parse_hocr_turn(hocr: str) -> tuple[int, tuple[int, int] | None]:
    """hOCR -> (degrees clockwise to turn the page upright, the page's (width, height)).

    Each line read turned says so (`textangle 180`); upright lines don't. Tesseract turns the
    whole page one way, so the lines' most common angle is the page's.
    """
    page = _HOCR_PAGE.search(hocr)
    size = (int(page[3]) - int(page[1]), int(page[4]) - int(page[2])) if page else None
    angles = Counter(
        int(m[1]) if (m := re.search(r"textangle (\d+)", title)) else 0
        for title in _HOCR_LINE.findall(hocr)
    )
    turn = angles.most_common(1)[0][0] if angles else 0
    return (turn if turn in (90, 180, 270) else 0), size


def find_tesseract(configured: Path | None = None) -> Path | None:
    if configured:
        return configured if configured.exists() else None
    if found := shutil.which("tesseract"):
        return Path(found)
    return WINDOWS_DEFAULT if WINDOWS_DEFAULT.exists() else None


@cache
def _version(exe: Path) -> str:
    out = subprocess.run(
        [str(exe), "--version"], capture_output=True, text=True, encoding="utf-8", check=False
    )
    first = (out.stdout or out.stderr).splitlines()[:1]
    return first[0].strip() if first else "tesseract"


class TesseractEngine:
    name = "tesseract"

    def __init__(self, settings: OcrSettings) -> None:
        self.settings = settings
        self.exe = find_tesseract(settings.tesseract_path)

    def is_available(self) -> bool:
        return self.exe is not None

    @property
    def version(self) -> str:
        """For transcriptions.engine_model, e.g. 'tesseract v5.4.0 eng'."""
        if not self.exe:
            raise TesseractNotFound(self.missing_help())
        return f"{_version(self.exe)} {'+'.join(self.settings.languages)}"

    def recognize(self, image_path: Path) -> list[PageResult]:
        if not self.exe:
            raise TesseractNotFound(self.missing_help())
        text, words, conf = self._read(image_path)
        if len(words) < FEW_WORDS:
            again = self._read(image_path, ADAPTIVE)
            if len(again[1]) > len(words):
                text, words, conf = again
        return [PageResult(1, text, conf, self.name, words)]

    def mirrored_reading(self, image_path: Path) -> PageResult:
        """Read the page turned round left to right, as a mirror image must be: the back of a
        carbon copy, or a page scanned through the paper. The words' boxes are those of the
        turned-round page."""
        with tempfile.TemporaryDirectory(prefix="lindley-ocr-", ignore_cleanup_errors=True) as d:
            turned = Path(d) / "mirrored.png"
            with Image.open(image_path) as img:
                dpi = {"dpi": img.info["dpi"]} if "dpi" in img.info else {}
                ImageOps.mirror(ImageOps.exif_transpose(img)).save(turned, **dpi)
            return self.recognize(turned)[0]

    def oriented_reading(self, image_path: Path) -> tuple[int, PageResult | None]:
        """Read the page whichever way up it is, in one run. Returns the turn, degrees
        clockwise, that puts the page upright, and the reading as if it were: upside down, the
        words' boxes are turned with the page. On its side the reading is None, for the page to
        be read again turned upright: Tesseract reads a sideways page's lines as vertical, and
        their boxes don't turn back cleanly.

        Tesseract only turns a page when it's sure (its min_orientation_margin); one it isn't
        sure about is read as it is, turn 0.
        """
        if not self.exe:
            raise TesseractNotFound(self.missing_help())
        turn, size, (text, words, conf) = self._read_oriented(image_path)
        if len(words) < FEW_WORDS:
            again = self._read_oriented(image_path, ADAPTIVE)
            if len(again[2][1]) > len(words):
                turn, size, (text, words, conf) = again
        if turn == 180 and size:
            w, h = size
            words = [
                wd | {"bbox": [w - x - bw, h - y - bh, bw, bh]}
                for wd in words
                for x, y, bw, bh in [wd["bbox"]]
            ]
        elif turn:
            return turn, None
        return turn, PageResult(1, text, conf, self.name, words)

    def _read(self, image_path: Path, extra: list[str] | None = None):
        return parse_tsv(self._run(image_path, "stdout", (extra or []) + ["tsv"]))

    def _read_oriented(self, image_path: Path, extra: list[str] | None = None):
        """One --psm 1 run: (turn, page size, parsed TSV). TSV and hOCR can't both go to
        stdout, so they're written to a folder of their own."""
        with tempfile.TemporaryDirectory(prefix="lindley-ocr-", ignore_cleanup_errors=True) as d:
            out = Path(d) / "page"
            self._run(image_path, str(out), ["--psm", "1", *(extra or []), "tsv", "hocr"])
            tsv = out.with_suffix(".tsv").read_text(encoding="utf-8", errors="replace")
            hocr = out.with_suffix(".hocr").read_text(encoding="utf-8", errors="replace")
        turn, size = parse_hocr_turn(hocr)
        return turn, size, parse_tsv(tsv)

    def _run(self, image_path: Path, output: str, args: list[str]) -> str:
        run = subprocess.run(
            [str(self.exe), str(image_path), output, "-l", "+".join(self.settings.languages)]
            + args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=TIMEOUT_S,
            check=False,
        )
        if run.returncode != 0:
            raise RuntimeError(f"Tesseract couldn't read {image_path.name}: {run.stderr.strip()}")
        return run.stdout

    def orientation(self, image_path: Path) -> tuple[int, float] | None:
        """(degrees clockwise to turn the page upright, Tesseract's confidence), or None if it
        can't tell: on pages with little text (often handwriting), or without osd.traineddata.

        A separate check, for pages oriented_reading didn't turn but that read poorly, and for
        pages only the vision model reads. Below ORIENTATION_MIN_CONF the answer is only a
        guess, often wrong; the pipeline then lets the reading decide.
        """
        if not self.exe:
            return None
        try:
            run = subprocess.run(
                [str(self.exe), str(image_path), "stdout", "--psm", "0"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=TIMEOUT_S,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None  # the page is still read the way it was scanned
        return parse_osd(run.stdout) if run.returncode == 0 else None

    def missing_help(self) -> str:
        where = self.settings.tesseract_path or "PATH or " + str(WINDOWS_DEFAULT)
        return (
            f"Tesseract wasn't found ({where}). Install it "
            "(winget install UB-Mannheim.TesseractOCR) or set ocr.tesseract_path in settings.json."
        )
