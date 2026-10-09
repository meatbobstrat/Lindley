import subprocess

from PIL import Image, ImageDraw

from lindley.assembler.clues import page_clues
from lindley.config import OcrSettings
from lindley.worker.ocr import margins, tesseract
from lindley.worker.ocr.tesseract import TesseractEngine, joined, tsv_lines, with_numbers

PAPER, INK = 225, 60
W, H = 1000, 1400
HEADER = "\t".join(
    [
        "level",
        "page_num",
        "block_num",
        "par_num",
        "line_num",
        "word_num",
        "left",
        "top",
        "width",
        "height",
        "conf",
        "text",
    ]
)


def page(*marks, lines=(300, 340, 380)):
    """A page of paper with three lines of writing, and dark marks where asked: (x, y, w, h)."""
    img = Image.new("L", (W, H), PAPER)
    d = ImageDraw.Draw(img)
    words = []
    for y in lines:
        for i, x in enumerate(range(150, 750, 150)):
            d.rectangle((x, y, x + 110, y + 30), fill=INK)
            words.append({"text": f"word{i}", "conf": 95.0, "bbox": [x, y, 110, 30]})
    for x, y, w, h in marks:
        d.rectangle((x, y, x + w, y + h), fill=INK)
    return img, words


def test_a_mark_alone_in_the_top_margin_is_found():
    img, words = page((490, 100, 20, 36))
    [m] = margins.find(img, words)
    x, y, w, h = m.box
    assert m.top and x <= 490 < x + w and y <= 100 < y + h and w < 60


def test_a_mark_below_the_writing_is_found_at_the_bottom():
    img, words = page((490, 1300, 20, 36))
    [m] = margins.find(img, words)
    assert not m.top


def test_marks_beside_other_ink_on_their_row_are_writing_not_a_number():
    # A signature's letters, close together: joined, they're far too wide for a page number
    img, words = page(*[(300 + i * 40, 100, 25, 36) for i in range(10)])
    assert margins.find(img, words) == []


def test_specks_and_marks_too_small_for_typing_are_left_out():
    img, words = page((490, 100, 4, 4), (600, 120, 10, 8))
    assert margins.find(img, words) == []


def test_a_mark_already_read_well_as_a_word_isnt_looked_at_again():
    img, words = page((480, 100, 60, 36))
    words.append({"text": "Notes", "conf": 92.0, "bbox": [480, 100, 60, 36]})
    assert margins.find(img, words) == []


def test_a_page_with_no_writing_has_no_margins_to_look_in():
    img, _ = page((490, 100, 20, 36))
    assert margins.find(img, []) == []


def test_each_mark_gets_the_digits_read_on_its_row_of_the_sheet():
    marks = [margins.Mark((10, 10, 20, 30), True), margins.Mark((500, 1300, 20, 30), False)]
    rows = [0, 100]
    read = [
        {"text": "1", "conf": 95.0, "bbox": [30, 40, 10, 30]},
        {"text": "2", "conf": 91.0, "bbox": [45, 40, 10, 30]},
        {"text": "7", "conf": 60.0, "bbox": [30, 140, 10, 30]},  # read with doubt: not kept
    ]
    assert margins.numbers(marks, rows, read) == [(marks[0], "12", 91.0)]


def test_a_number_read_in_the_margin_replaces_what_the_page_reading_made_of_it():
    lines = [
        ((1, 1, 1), [{"text": "WwW", "conf": 49.7, "bbox": [480, 95, 40, 40]}]),
        ((1, 2, 1), [{"text": "Was", "conf": 96.0, "bbox": [150, 300, 60, 30]}]),
    ]
    found = [(margins.Mark((485, 100, 30, 36), True), "5", 93.0)]
    text, words, _ = joined(with_numbers(lines, found))
    assert text.splitlines()[0] == "5" and "WwW" not in text
    assert [w["text"] for w in words] == ["5", "Was"]


def test_the_engine_reads_a_margin_number_and_the_assembler_finds_it(tmp_path, monkeypatch):
    img, words = page((490, 100, 20, 36))
    path = tmp_path / "page.png"
    img.save(path)

    def row(block, line, n, w):
        x, y, bw, bh = w["bbox"]
        return f"5\t1\t{block}\t1\t{line}\t{n}\t{x}\t{y}\t{bw}\t{bh}\t{w['conf']}\t{w['text']}"

    page_tsv = "\n".join([HEADER] + [row(1, i // 5 + 1, i % 5 + 1, w) for i, w in enumerate(words)])
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        if "margins.png" in str(args[1]):
            assert "tessedit_char_whitelist=0123456789" in args
            out = "\n".join([HEADER, "5\t1\t1\t1\t1\t1\t40\t40\t20\t36\t94.0\t5"])
        else:
            out = page_tsv
        return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

    monkeypatch.setattr(tesseract.subprocess, "run", fake_run)
    exe = tmp_path / "tesseract.exe"
    exe.touch()
    [result] = TesseractEngine(OcrSettings(tesseract_path=exe)).recognize(path)
    # The page (and again, adaptively: it has few words), then its margins: one run for those
    assert [str(a[1]).endswith("margins.png") for a in seen] == [False, False, True]
    assert result.text.splitlines()[0] == "5"
    clues = page_clues(result.text, "page.png", result.words, H)
    assert clues.marker == (5, None) and clues.marker_sure


def test_tsv_lines_keep_tesseracts_lines_for_the_reading():
    tsv = "\n".join(
        [
            HEADER,
            "5\t1\t1\t1\t1\t1\t0\t0\t10\t10\t90\tDear",
            "5\t1\t1\t1\t1\t2\t20\t0\t10\t10\t90\tSister,",
            "5\t1\t1\t2\t1\t1\t0\t30\t10\t10\t90\tWe",
        ]
    )
    lines = tsv_lines(tsv)
    assert [[w["text"] for w in ws] for _, ws in lines] == [["Dear", "Sister,"], ["We"]]
    assert joined(lines)[0] == "Dear Sister,\n\nWe"
