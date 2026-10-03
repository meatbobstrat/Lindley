import random

import pytest
from PIL import Image, ImageDraw

from lindley.worker.image import (
    BLANK_AT,
    DUP_BITS,
    analyse,
    classify_script,
    hamming,
    open_upright,
)


def page(size=(1000, 1400), paper="white", lines=0, seed=1):
    """A sheet of paper with `lines` lines of 'writing': runs of dark word-shaped blocks."""
    img = Image.new("RGB", size, paper)
    draw = ImageDraw.Draw(img)
    rnd = random.Random(seed)
    for i in range(lines):
        y = 120 + i * 45
        x = 90
        while x < size[0] - 160:
            w = rnd.randint(30, 110)
            draw.rectangle([x, y, x + w, y + 14], fill=(40, 30, 30))
            x += w + 18
    return img


def save(img, tmp_path, name="p.png", **kw):
    path = tmp_path / name
    img.save(path, **kw)
    return path


def test_a_white_page_is_blank(tmp_path):
    info = analyse(save(page(), tmp_path))
    assert info.blank_score == 1.0 and info.paper_color == "#ffffff"


def test_specks_and_a_dark_scanner_border_still_count_as_blank(tmp_path):
    img = page()
    draw = ImageDraw.Draw(img)
    rnd = random.Random(7)
    for _ in range(300):  # dust
        x, y = rnd.randrange(1000), rnd.randrange(1400)
        draw.point((x, y), fill="black")
    draw.rectangle([0, 0, 999, 15], fill="black")  # edge of the scanner lid
    assert analyse(save(img, tmp_path)).blank_score >= BLANK_AT


def test_a_page_of_writing_is_not_blank(tmp_path):
    full = analyse(save(page(lines=26), tmp_path, "full.png")).blank_score
    note = analyse(save(page(lines=2), tmp_path, "note.png")).blank_score
    assert full < 0.2
    assert full < note < BLANK_AT


def test_a_faint_pencil_note_is_not_blank(tmp_path):
    # Two short lines of thin grey scribble on a 300 dpi letter page.
    img = Image.new("RGB", (2550, 3300), "white")
    draw = ImageDraw.Draw(img)
    rnd = random.Random(3)
    for y0 in (400, 470):
        pts = [(300 + 6 * i, y0 + rnd.randint(-18, 18)) for i in range(140)]
        draw.line(pts, fill=(120, 120, 120), width=2)
    assert analyse(save(img, tmp_path)).blank_score < BLANK_AT


def test_paper_colour_is_the_background_not_the_ink(tmp_path):
    info = analyse(save(page(paper=(255, 255, 240), lines=20), tmp_path))
    assert info.paper_color == "#fffff0"


def test_near_copies_hash_alike_and_different_pages_do_not(tmp_path):
    a = page(lines=12, seed=1)
    ha = analyse(save(a, tmp_path, "a.png")).phash
    smaller = a.resize((700, 980))
    hb = analyse(save(smaller, tmp_path, "b.jpg", quality=70)).phash
    hc = analyse(save(page(lines=20, seed=9), tmp_path, "c.png")).phash
    assert len(ha) == 16
    assert hamming(ha, hb) <= DUP_BITS
    assert hamming(ha, hc) > DUP_BITS


def test_hamming_needs_two_hashes():
    assert hamming("00000000000000ff", "000000000000000f") == 4
    assert hamming(None, "000000000000000f") is None
    assert hamming("not a hash", "000000000000000f") is None
    assert hamming("zzzzzzzzzzzzzzzz", "000000000000000f") is None


def test_exif_orientation_is_applied(tmp_path):
    img = Image.new("RGB", (100, 140), "white")
    exif = img.getexif()
    exif[0x0112] = 6  # rotated 90° clockwise to view
    path = save(img, tmp_path, "phone.jpg", exif=exif)
    assert open_upright(path).size == (140, 100)


def test_sixteen_bit_grey_is_not_washed_out(tmp_path):
    img = Image.new("I;16", (100, 140), 30000)
    info = analyse(save(img, tmp_path, "grey.png"))
    assert info.paper_color in ("#757575", "#747474")


def line(y, confs, h=20):
    return [{"text": "w", "conf": c, "bbox": [10 + 70 * i, y, 60, h]} for i, c in enumerate(confs)]


@pytest.mark.parametrize(
    "words, blank, expect",
    [
        ([], 1.0, "none"),
        (line(10, [95, 91]) + line(40, [88, 90]) + line(70, [93]), 0.6, "printed"),
        (line(10, [20, 35]) + line(40, [12, 40]) + line(70, [30]), 0.7, "handwritten"),
        (line(10, [92, 96]) + line(40, [20, 15]) + line(70, [30, 25]), 0.7, "mixed"),
        (line(10, [60, 65]) + line(40, [62]), 0.7, None),
        # Typewriting on old paper: many middling lines, one handwritten correction.
        (
            line(10, [80])
            + line(40, [85])
            + line(70, [66])
            + line(100, [70])
            + line(130, [90])
            + line(160, [30]),
            0.6,
            "printed",
        ),
        ([], 0.5, None),
    ],
)
def test_script_from_tesseract_confidence(words, blank, expect):
    assert classify_script(words, blank) == expect


def test_words_on_one_line_are_judged_together():
    # Slightly uneven baselines are still one line: one confident line, so printed.
    words = line(10, [90]) + line(14, [92]) + line(8, [88])
    assert classify_script(words, 0.8) == "printed"
