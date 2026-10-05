import json

import pytest

from lindley.db.database import connect, init_db
from lindley.export.textlayer import Word, align, lay_out, page_text, reframe


def line(*words, y=100, h=40, x=100, gap=30):
    """Tesseract-style words along one line, each 20 px a character."""
    out = []
    for t in words:
        out.append(Word(t, (x, y, len(t) * 20, h)))
        x += len(t) * 20 + gap
    return out


def inside(box, slot):
    return (
        slot[0] - 0.01 <= box[0]
        and box[0] + box[2] <= slot[0] + slot[2] + 0.01
        and box[1] == slot[1]
        and box[3] == slot[3]
    )


def test_matching_words_take_tesseracts_boxes_with_the_new_spelling():
    tess = line("Dear", "Sistr,", "I", "write")
    words = align("Dear Sister, I write", tess)
    assert [w.text for w in words] == ["Dear", "Sister,", "I", "write"]
    assert words[0].box == tess[0].box
    assert words[2].box == tess[2].box and words[3].box == tess[3].box
    assert inside(words[1].box, tess[1].box)


def test_a_word_read_as_two_shares_its_box_in_order():
    tess = line("Come", "tomorrow")
    words = align("Come to morrow", tess)
    to, morrow = words[1], words[2]
    assert inside(to.box, tess[1].box) and inside(morrow.box, tess[1].box)
    assert to.box[0] + to.box[2] <= morrow.box[0]
    assert morrow.box[2] > to.box[2]  # the longer word gets more of the box


def test_two_words_read_as_one_take_their_place():
    tess = line("to", "morrow", "we")
    words = align("tomorrow we", tess)
    assert [w.text for w in words] == ["tomorrow", "we"]
    assert words[1].box == tess[2].box


def test_a_word_tesseract_missed_goes_between_its_neighbours():
    tess = line("Dear", "Sister", "write", gap=120)
    words = align("Dear Sister I write", tess)
    i = words[2]
    assert i.text == "I"
    assert tess[1].box[0] + tess[1].box[2] <= i.box[0]
    assert i.box[0] + i.box[2] <= tess[2].box[0]


def test_a_word_missed_at_the_end_of_a_line_goes_after_it():
    tess = line("Your", "loving") + line("John", y=200)
    words = align("Your loving son John", tess)
    son = words[2]
    assert son.box[0] > tess[1].box[0] + tess[1].box[2]
    assert son.box[1] == tess[1].box[1]
    assert words[3].box == tess[2].box


def test_a_word_missed_before_everything_goes_before_the_first():
    tess = line("Sister", x=400)
    words = align("Dear Sister", tess)
    assert words[0].box[0] + words[0].box[2] <= tess[0].box[0]
    assert words[0].box[0] >= 0


def test_specks_tesseract_read_as_words_are_dropped():
    tess = line("Dear", "~", "Sister")
    assert [w.text for w in align("Dear Sister", tess)] == ["Dear", "Sister"]


def test_align_needs_boxes():
    with pytest.raises(ValueError):
        align("Dear Sister", [])


def test_lay_out_spaces_lines_down_the_page():
    words = lay_out("Dear Sister\n\nI write to you\nJohn", (2400, 3300))
    assert [w.text for w in words] == ["Dear", "Sister", "I", "write", "to", "you", "John"]
    ys = sorted({w.box[1] for w in words})
    assert len(ys) == 3
    for w in words:
        x, y, bw, bh = w.box
        assert x >= 0 and y >= 0 and x + bw <= 2400 and y + bh <= 3300


def test_lay_out_fits_a_long_line_across_the_page():
    words = lay_out(" ".join(["word"] * 200), (1000, 1400))
    last = words[-1].box
    assert last[0] + last[2] <= 1000


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def page(conn) -> int:
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES ('h', 'scan_1.jpg', 'D:/scans/scan_1.jpg', 'watched', 'copy', 'read')"
    ).lastrowid
    return conn.execute("INSERT INTO pages (scan_id) VALUES (?)", (scan,)).lastrowid


def reading(conn, pid, source, text, words=None, current=True):
    if current:
        conn.execute("UPDATE transcriptions SET is_current = 0 WHERE page_id = ?", (pid,))
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, words, is_current)"
        " VALUES (?, ?, ?, ?, ?)",
        (pid, source, text, json.dumps(words) if words else None, int(current)),
    )


TESS = [
    {"text": "Dear", "conf": 91.0, "bbox": [100, 100, 80, 40]},
    {"text": "Sistr", "conf": 40.0, "bbox": [210, 100, 100, 40]},
]


def test_page_text_uses_tesseracts_own_words(conn):
    pid = page(conn)
    reading(conn, pid, "tesseract", "Dear Sistr", TESS)
    pt = page_text(conn, pid, (2400, 3300))
    assert pt.placed
    assert pt.words == [Word("Dear", (100, 100, 80, 40)), Word("Sistr", (210, 100, 100, 40))]


def test_page_text_lays_a_correction_over_tesseracts_boxes(conn):
    pid = page(conn)
    reading(conn, pid, "tesseract", "Dear Sistr", TESS)
    reading(conn, pid, "vision", "Dear Sister,")
    reading(conn, pid, "user", "Dear Sister, Mary")
    pt = page_text(conn, pid, (2400, 3300))
    assert pt.placed
    assert [w.text for w in pt.words] == ["Dear", "Sister,", "Mary"]
    assert pt.words[0].box == (100, 100, 80, 40)


def test_page_text_without_boxes_is_searchable_but_unplaced(conn):
    pid = page(conn)
    reading(conn, pid, "vision", "Dear Sister,\nI write")
    pt = page_text(conn, pid, (2400, 3300))
    assert not pt.placed
    assert [w.text for w in pt.words] == ["Dear", "Sister,", "I", "write"]


def test_page_text_of_an_unread_or_empty_page_has_no_words(conn):
    pid = page(conn)
    assert page_text(conn, pid, (2400, 3300)).words == []
    reading(conn, pid, "tesseract", "   ")
    assert page_text(conn, pid, (2400, 3300)).words == []


FRAMES = [(r, m) for r in (0, 90, 180, 270) for m in (False, True)]


@pytest.mark.parametrize("was", FRAMES)
@pytest.mark.parametrize("now", FRAMES)
def test_boxes_turn_with_the_page(tmp_path, was, now):
    """Against the page images themselves: a mark found on the page turned one way is where
    reframe says, on the page turned any other way."""
    from PIL import Image, ImageDraw

    from lindley.worker.image import upright_page

    scan = tmp_path / "scan.png"
    img = Image.new("L", (60, 40), 255)
    ImageDraw.Draw(img).rectangle((5, 8, 14, 11), fill=0)  # 10 wide, 4 high
    img.save(scan)

    def mark(frame):
        page = upright_page(scan, *frame)
        x0, y0, x1, y1 = page.point(lambda v: 255 - v).getbbox()
        return (x0, y0, x1 - x0, y1 - y0), page.size

    box, _ = mark(was)
    expected, size = mark(now)
    assert reframe(box, size, was, now) == expected


def test_a_page_turned_since_it_was_read_has_its_words_turned_too(conn):
    pid = page(conn)
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, words, read_rotation, read_mirror,"
        " is_current) VALUES (?, 'tesseract', 'Dear', ?, 0, 0, 1)",
        (pid, json.dumps(TESS[:1])),
    )
    # Read upright, 2400 x 3300; turned a quarter clockwise since: 3300 x 2400
    [word] = page_text(conn, pid, (3300, 2400), (90, False)).words
    assert word.box == (3300 - 100 - 40, 100, 40, 80)
    # A reading from before Lindley kept how pages were turned is left as it was
    conn.execute("UPDATE transcriptions SET read_rotation = NULL, read_mirror = NULL")
    assert page_text(conn, pid, (3300, 2400), (90, False)).words[0].box == (100, 100, 80, 40)
