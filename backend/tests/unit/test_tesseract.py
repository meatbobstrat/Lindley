import subprocess
from pathlib import Path

import pytest
from PIL import Image, ImageDraw, ImageFont

from lindley.assembler.clues import page_clues
from lindley.config import OcrSettings
from lindley.worker.ocr import tesseract
from lindley.worker.ocr.tesseract import (
    TesseractEngine,
    TesseractNotFound,
    parse_osd,
    parse_tsv,
)

HEADER = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"
)


def word(block, par, line, n, x, y, conf, text, w=60, h=20):
    return f"5\t1\t{block}\t{par}\t{line}\t{n}\t{x}\t{y}\t{w}\t{h}\t{conf}\t{text}"


TSV = "\n".join(
    [
        HEADER,
        "1\t1\t0\t0\t0\t0\t0\t0\t1000\t1400\t-1\t",
        "2\t1\t1\t0\t0\t0\t100\t60\t800\t40\t-1\t",
        word(1, 1, 1, 1, 100, 60, 96.2, "Dear"),
        word(1, 1, 1, 2, 170, 60, 91.0, "Sister,"),
        word(1, 2, 1, 1, 100, 120, 88.5, "We"),
        word(1, 2, 1, 2, 150, 120, 80.1, "are"),
        word(1, 2, 2, 1, 100, 150, 70.0, "well."),
        word(1, 2, 2, 2, 160, 150, -1, " "),
        word(2, 1, 1, 1, 480, 1340, 90.0, "-"),
        word(2, 1, 1, 2, 500, 1340, 93.0, "2"),
        word(2, 1, 1, 3, 520, 1340, 90.0, "-"),
    ]
)


def test_parse_tsv_rebuilds_lines_and_paragraphs():
    text, words, conf = parse_tsv(TSV)
    assert text == "Dear Sister,\n\nWe are\nwell.\n\n- 2 -"
    assert len(words) == 8  # the blank, conf -1 entry is dropped
    assert words[0] == {"text": "Dear", "conf": 96.2, "bbox": [100, 60, 60, 20]}
    assert conf == pytest.approx(sum([96.2, 91, 88.5, 80.1, 70, 90, 93, 90]) / 8, abs=0.1)


def test_parsed_words_let_the_assembler_find_the_page_number():
    text, words, _ = parse_tsv(TSV)
    clues = page_clues(text, "scan_0002.png", words, 1400)
    assert clues.marker and clues.marker[0] == 2


def test_an_empty_page_has_no_text_and_no_confidence():
    assert parse_tsv(HEADER + "\n1\t1\t0\t0\t0\t0\t0\t0\t10\t10\t-1\t") == ("", [], None)


def test_a_missing_tesseract_is_explained(tmp_path):
    engine = TesseractEngine(OcrSettings(tesseract_path=tmp_path / "nope.exe"))
    assert not engine.is_available()
    with pytest.raises(TesseractNotFound, match="winget install"):
        engine.recognize(tmp_path / "page.png")


def test_recognize_runs_tesseract_and_parses_its_output(tmp_path, monkeypatch):
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        return subprocess.CompletedProcess(args, 0, stdout=TSV, stderr="")

    monkeypatch.setattr(tesseract.subprocess, "run", fake_run)
    engine = TesseractEngine(OcrSettings(tesseract_path=exe, languages=["eng", "deu"]))
    [page] = engine.recognize(tmp_path / "page.png")
    assert seen[0][1:] == [str(tmp_path / "page.png"), "stdout", "-l", "eng+deu", "tsv"]
    assert page.text.startswith("Dear Sister,") and page.engine == "tesseract"
    assert page.words and page.confidence and page.confidence > 80


def test_a_tesseract_error_is_reported(tmp_path, monkeypatch):
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    monkeypatch.setattr(
        tesseract.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 1, stdout="", stderr="bad image"),
    )
    with pytest.raises(RuntimeError, match="bad image"):
        TesseractEngine(OcrSettings(tesseract_path=exe)).recognize(tmp_path / "page.png")


OSD = """Page number: 0
Orientation in degrees: 270
Rotate: 90
Orientation confidence: 6.42
Script: Latin
Script confidence: 3.10
"""


def test_parse_osd_reads_the_rotation_and_its_confidence():
    assert parse_osd(OSD) == (90, 6.42)
    assert parse_osd("Too few characters. Skipping this page") is None
    assert parse_osd("Rotate: 45\nOrientation confidence: 9.0") is None


def fake_tesseract(tmp_path, monkeypatch, code, stdout):
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        return subprocess.CompletedProcess(args, code, stdout=stdout, stderr="")

    monkeypatch.setattr(tesseract.subprocess, "run", fake_run)
    return TesseractEngine(OcrSettings(tesseract_path=exe)), seen


def test_orientation_runs_the_check_and_trusts_only_a_confident_answer(tmp_path, monkeypatch):
    engine, seen = fake_tesseract(tmp_path, monkeypatch, 0, OSD)
    assert engine.orientation(tmp_path / "page.png") == 90
    assert seen[0][1:] == [str(tmp_path / "page.png"), "stdout", "--psm", "0"]
    unsure = OSD.replace("6.42", "0.71")
    assert fake_tesseract(tmp_path, monkeypatch, 0, unsure)[0].orientation(tmp_path / "p") is None


def test_orientation_is_unknown_when_tesseract_cannot_tell(tmp_path, monkeypatch):
    engine, _ = fake_tesseract(tmp_path, monkeypatch, 1, "Too few characters")
    assert engine.orientation(tmp_path / "page.png") is None
    missing = TesseractEngine(OcrSettings(tesseract_path=tmp_path / "nope.exe"))
    assert missing.orientation(tmp_path / "page.png") is None


real = TesseractEngine(OcrSettings())


@pytest.mark.skipif(not real.is_available(), reason="Tesseract isn't installed")
def test_real_tesseract_reads_printed_text(tmp_path: Path):
    img = Image.new("L", (1200, 400), 255)
    font = ImageFont.load_default(size=64)
    ImageDraw.Draw(img).text((60, 120), "Dear Sister, we are well.", fill=0, font=font)
    img.save(tmp_path / "page.png", dpi=(300, 300))
    [page] = real.recognize(tmp_path / "page.png")
    assert "Sister" in page.text and page.confidence and page.confidence > 50
    assert real.version.startswith("tesseract")


def test_an_orientation_check_that_times_out_is_just_unknown(tmp_path, monkeypatch):
    exe = tmp_path / "tesseract.exe"
    exe.touch()

    def slow(args, **kw):
        raise subprocess.TimeoutExpired(args, kw["timeout"])

    monkeypatch.setattr(tesseract.subprocess, "run", slow)
    engine = TesseractEngine(OcrSettings(tesseract_path=exe))
    assert engine.orientation(tmp_path / "page.png") is None
