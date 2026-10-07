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
    find_tesseract,
    parse_hocr_turn,
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
    with pytest.raises(TesseractNotFound, match="Install it"):
        engine.recognize(tmp_path / "page.png")


def test_the_tesseract_lindley_brings_comes_before_another(tmp_path, monkeypatch):
    ours = tmp_path / "tesseract" / "tesseract.exe"
    monkeypatch.setattr(tesseract.sys, "prefix", str(tmp_path))
    monkeypatch.setattr(tesseract.shutil, "which", lambda name: str(tmp_path / "other.exe"))
    assert find_tesseract() == tmp_path / "other.exe"  # not installed with Lindley
    ours.parent.mkdir()
    ours.write_bytes(b"")
    assert find_tesseract() == ours
    assert find_tesseract(tmp_path / "chosen.exe") is None  # one a person chose, or none


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


def test_a_page_read_as_almost_nothing_is_read_again_with_an_adaptive_threshold(
    tmp_path, monkeypatch
):
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        adaptive = "thresholding_method=1" in args
        return subprocess.CompletedProcess(args, 0, stdout=TSV if adaptive else "", stderr="")

    monkeypatch.setattr(tesseract.subprocess, "run", fake_run)
    [page] = TesseractEngine(OcrSettings(tesseract_path=exe)).recognize(tmp_path / "page.png")
    assert len(seen) == 2 and page.text.startswith("Dear Sister,")


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


def test_orientation_runs_the_check_and_gives_its_confidence(tmp_path, monkeypatch):
    engine, seen = fake_tesseract(tmp_path, monkeypatch, 0, OSD)
    assert engine.orientation(tmp_path / "page.png") == (90, 6.42)
    assert seen[0][1:] == [str(tmp_path / "page.png"), "stdout", "--psm", "0"]


def test_orientation_is_unknown_when_tesseract_cannot_tell(tmp_path, monkeypatch):
    engine, _ = fake_tesseract(tmp_path, monkeypatch, 1, "Too few characters")
    assert engine.orientation(tmp_path / "page.png") is None
    missing = TesseractEngine(OcrSettings(tesseract_path=tmp_path / "nope.exe"))
    assert missing.orientation(tmp_path / "page.png") is None


def hocr(angle: int | None, lines: int = 3, size=(1000, 1400)) -> str:
    """Tesseract's hOCR for a page whose lines were read turned `angle` (None: upright)."""
    turned = f"; textangle {angle}" if angle is not None else ""
    spans = "".join(
        f"<span class='ocr_line' id='line_1_{i}' title=\"bbox 10 {i * 40} 900 {i * 40 + 30}"
        f'{turned}; x_size 30">words</span>'
        for i in range(lines)
    )
    return (
        f"<div class='ocr_page' id='page_1' title='image \"page.png\"; bbox 0 0 {size[0]}"
        f" {size[1]}; ppageno 0'>{spans}</div>"
    )


def test_parse_hocr_turn_reads_which_way_up_the_page_was_read():
    assert parse_hocr_turn(hocr(None)) == (0, (1000, 1400))
    assert parse_hocr_turn(hocr(180)) == (180, (1000, 1400))
    assert parse_hocr_turn(hocr(90, size=(1400, 1000))) == (90, (1400, 1000))
    upright_line = "<span class='ocr_line' id='line_1_9' title=\"bbox 1 2 3 4\">a</span>"
    mixed = hocr(180, lines=3).replace("</div>", upright_line + "</div>")
    assert parse_hocr_turn(mixed)[0] == 180  # most lines decide
    assert parse_hocr_turn("") == (0, None)


def oriented_tesseract(tmp_path, monkeypatch, angle, adaptive_tsv=None):
    """A Tesseract that writes the TSV and hOCR files --psm 1 asks for."""
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        out = Path(args[2])
        adaptive = "thresholding_method=1" in args
        tsv = adaptive_tsv if adaptive_tsv is not None and not adaptive else TSV
        out.with_suffix(".tsv").write_text(tsv, encoding="utf-8")
        out.with_suffix(".hocr").write_text(hocr(angle), encoding="utf-8")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(tesseract.subprocess, "run", fake_run)
    return TesseractEngine(OcrSettings(tesseract_path=exe)), seen


def test_an_upright_page_is_read_and_oriented_in_one_run(tmp_path, monkeypatch):
    engine, seen = oriented_tesseract(tmp_path, monkeypatch, None)
    turn, page = engine.oriented_reading(tmp_path / "page.png")
    assert turn == 0 and page and page.text.startswith("Dear Sister,")
    assert page.words[0]["bbox"] == [100, 60, 60, 20]
    # one run makes both files (a second, adaptive one follows: the made-up page has few words)
    assert seen[0][3:] == ["-l", "eng", "--psm", "1", "tsv", "hocr"]


def test_an_upside_down_page_has_its_words_turned_with_it(tmp_path, monkeypatch):
    engine, _ = oriented_tesseract(tmp_path, monkeypatch, 180)
    turn, page = engine.oriented_reading(tmp_path / "page.png")
    assert turn == 180 and page and page.text.startswith("Dear Sister,")
    assert page.words[0] == {"text": "Dear", "conf": 96.2, "bbox": [840, 1320, 60, 20]}


def test_a_sideways_page_is_left_to_be_read_again_upright(tmp_path, monkeypatch):
    engine, _ = oriented_tesseract(tmp_path, monkeypatch, 270)
    assert engine.oriented_reading(tmp_path / "page.png") == (270, None)


def test_an_oriented_reading_of_almost_nothing_is_tried_with_an_adaptive_threshold(
    tmp_path, monkeypatch
):
    engine, seen = oriented_tesseract(tmp_path, monkeypatch, None, adaptive_tsv=HEADER)
    turn, page = engine.oriented_reading(tmp_path / "page.png")
    assert len(seen) == 2 and page and page.text.startswith("Dear Sister,")


def test_an_oriented_reading_needs_tesseract(tmp_path):
    engine = TesseractEngine(OcrSettings(tesseract_path=tmp_path / "nope.exe"))
    with pytest.raises(TesseractNotFound):
        engine.oriented_reading(tmp_path / "page.png")


real = TesseractEngine(OcrSettings())


@pytest.mark.skipif(not real.is_available(), reason="Tesseract isn't installed")
def test_real_tesseract_reads_an_upside_down_page_and_says_so(tmp_path: Path):
    img = Image.new("L", (1700, 2200), 255)
    font = ImageFont.load_default(size=48)
    draw = ImageDraw.Draw(img)
    for i in range(14):
        draw.text((120, 200 + i * 110), "We are all well and the river came up again.", 0, font)
    img.rotate(180).save(tmp_path / "page.png", dpi=(300, 300))
    turn, page = real.oriented_reading(tmp_path / "page.png")
    assert turn == 180 and page and "river" in page.text
    top = min(page.words, key=lambda w: w["bbox"][1])
    assert top["bbox"][1] < 400  # the first line is at the top again


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
