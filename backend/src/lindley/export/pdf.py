"""A searchable PDF: each page's scan, with its words as invisible text over the writing.

The text is drawn in PDF text render mode 3 (invisible), as OCR software does, so a viewer
finds, selects and copies it while showing only the scan. Each word is stretched to its box.

The text uses the PDF viewer's built-in Helvetica, so it needs no font file. That covers
Windows-1252 (English and western European letters, curly quotes and dashes); other
characters are reduced to their plain letter where there is one, else to "?".
"""

from __future__ import annotations

import io
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fpdf import FPDF
from fpdf.enums import TextMode
from PIL import Image

from lindley import __version__
from lindley.export.textlayer import Word, same_line
from lindley.worker.image import exif_orientation, upright_page

ENCODING = "windows-1252"
DEFAULT_DPI = 300
# A page at its scan's dpi bigger than this on its longer side (in inches), or a scan whose dpi
# is missing or implausible, is sized as a page this long instead: a phone photo says 72 dpi.
MAX_INCHES = 17
FALLBACK_INCHES = 11
JPEG_QUALITY = 90


@dataclass
class PdfPage:
    image_path: Path
    rotation: int  # degrees clockwise that turn the image upright, after EXIF
    dpi: int | None
    color_mode: str | None  # rgb, gray, bilevel
    words: list[Word]  # boxes in pixels of the upright image


@dataclass
class PdfInfo:
    title: str
    subject: str | None = None


def build_pdf(pages: list[PdfPage], info: PdfInfo) -> bytes:
    pdf = FPDF(unit="pt")
    pdf.core_fonts_encoding = ENCODING
    pdf.set_auto_page_break(False)
    pdf.set_margin(0)
    pdf.set_title(info.title)
    if info.subject:
        pdf.set_subject(info.subject)
    pdf.set_creator(f"Lindley {__version__}")
    pdf.set_creation_date(datetime.now(UTC))
    pdf.set_font("helvetica")
    for page in pages:
        image, (w_px, h_px) = _image(page)
        scale = 72 / _dpi(page.dpi, max(w_px, h_px))
        width, height = w_px * scale, h_px * scale
        pdf.add_page(format=(width, height))
        pdf.image(image, x=0, y=0, w=width, h=height)
        _text_layer(pdf, page.words, scale)
    return bytes(pdf.output())


def _dpi(dpi: int | None, long_px: int) -> float:
    if dpi and dpi >= 100 and long_px / dpi <= MAX_INCHES:
        return dpi
    if not dpi and long_px / DEFAULT_DPI <= MAX_INCHES:
        return DEFAULT_DPI
    return long_px / FALLBACK_INCHES


def _image(page: PdfPage) -> tuple[bytes | Image.Image, tuple[int, int]]:
    """What to embed, and its size. A JPEG that needs no turning goes in as it is."""
    if page.image_path.suffix.lower() in (".jpg", ".jpeg") and not page.rotation % 360:
        with Image.open(page.image_path) as img:
            if exif_orientation(img) == 1 and img.mode in ("RGB", "L"):
                return page.image_path.read_bytes(), img.size
    img = upright_page(page.image_path, page.rotation)
    if page.color_mode == "bilevel":
        return img.convert("1"), img.size  # one bit a pixel, Flate-compressed
    if page.color_mode == "gray":
        img = img.convert("L")
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=JPEG_QUALITY)
    return buf.getvalue(), img.size


def _lines(words: list[Word]) -> list[list[Word]]:
    """Words in reading order, grouped into the lines they sit on."""
    lines: list[list[Word]] = []
    for w in words:
        if lines and same_line(lines[-1][-1].box, w.box) and w.box[0] >= lines[-1][-1].box[0]:
            lines[-1].append(w)
        else:
            lines.append([w])
    return lines


def _text_layer(pdf: FPDF, words: list[Word], scale: float) -> None:
    pdf.text_mode = TextMode.INVISIBLE
    for line in _lines(words):
        top = min(w.box[1] for w in line)
        bottom = max(w.box[1] + w.box[3] for w in line)
        size = max((bottom - top) * scale, 1.0)
        baseline = bottom * scale - size * 0.2  # Helvetica's descent is about a fifth of its size
        pdf.set_font_size(size)
        for i, w in enumerate(line):
            text = plain(w.text)
            pdf.set_stretching(100)
            natural = pdf.get_string_width(text)
            if not natural:
                continue
            pdf.set_stretching(max(100 * w.box[2] * scale / natural, 1.0))
            # A space after each word but the last, so a viewer copies the line as it reads
            pdf.text(w.box[0] * scale, baseline, text + (" " if i < len(line) - 1 else ""))
    pdf.set_stretching(100)
    pdf.text_mode = TextMode.FILL


def plain(text: str) -> str:
    """The text in Windows-1252: letters it lacks become their plain letter, else "?"."""
    out = []
    for ch in text:
        try:
            ch.encode(ENCODING)
            out.append(ch)
        except UnicodeEncodeError:
            base = unicodedata.normalize("NFKD", ch)
            try:
                base.encode(ENCODING)
                out.append(base)
            except UnicodeEncodeError:
                kept = "".join(c for c in base if c.isascii() and c.isprintable())
                out.append(kept or "?")
    return "".join(out)
