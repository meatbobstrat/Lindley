"""Local OCR with the Tesseract command-line program (no Python wrapper needed).

Tesseract's TSV output gives each word with its box and confidence; the words are what the
assembler uses to find page numbers standing alone at the top or bottom of a page. Its
orientation check (--psm 0) says which way up a page is.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from functools import cache
from pathlib import Path

from lindley.config import OcrSettings
from lindley.worker.ocr.base import PageResult

# Where the UB-Mannheim installer puts it when it isn't added to PATH.
WINDOWS_DEFAULT = Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
TIMEOUT_S = 300
# Tesseract's orientation confidence below which a page is left the way it was scanned.
ORIENTATION_MIN_CONF = 2.0


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
        run = subprocess.run(
            [str(self.exe), str(image_path), "stdout", "-l", "+".join(self.settings.languages)]
            + ["tsv"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=TIMEOUT_S,
            check=False,
        )
        if run.returncode != 0:
            raise RuntimeError(f"Tesseract couldn't read {image_path.name}: {run.stderr.strip()}")
        text, words, conf = parse_tsv(run.stdout)
        return [PageResult(1, text, conf, self.name, words)]

    def orientation(self, image_path: Path) -> int | None:
        """Degrees clockwise to turn the page upright, or None if Tesseract can't tell.

        It can't on pages with little text (often handwriting), or if osd.traineddata is missing.
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
        osd = parse_osd(run.stdout) if run.returncode == 0 else None
        if osd is None or osd[1] < ORIENTATION_MIN_CONF:
            return None
        return osd[0]

    def make_searchable_pdf(self, source: Path, output: Path) -> None:
        raise NotImplementedError

    def missing_help(self) -> str:
        where = self.settings.tesseract_path or "PATH or " + str(WINDOWS_DEFAULT)
        return (
            f"Tesseract wasn't found ({where}). Install it "
            "(winget install UB-Mannheim.TesseractOCR) or set ocr.tesseract_path in settings.json."
        )
