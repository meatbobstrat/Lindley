import random

import pytest
from PIL import Image, ImageDraw

from lindley.db.database import connect, init_db
from lindley.duplicates import find_duplicates

SENTENCES = [
    "We left Eldorado in the spring with two wagons and the cattle went ahead of us.",
    "Father had sold the farm to a man named Hale who paid in gold and silver coin.",
    "The river was high at the crossing and we waited three days for the water to fall.",
    "Mother kept a diary of the journey which I still have in a tin box upstairs.",
    "At Virginia City the mines were working night and day and wages were good.",
    "I found work setting type for the newspaper at four dollars a day.",
    "The editor was a quarrelsome man but he paid on Saturday without fail.",
    "In the winter of that year the snow lay six feet deep along the main street.",
    "We bought a small house near the church and planted a garden behind it.",
    "My brother went north to Alaska and wrote to us twice from the Yukon.",
    "The town grew quickly when the railroad came through from the south.",
    "I was elected justice of the peace and married more couples than I can count.",
]
OTHER = [
    "Received of John Branson the sum of three dollars for one barrel of flour delivered.",
    "The county commissioners met on Tuesday and approved the new bridge over the creek.",
    "Notice is hereby given that the annual meeting of the stockholders will be held in May.",
    "The weather this week has been fair with light winds from the west and cool nights.",
    "Prices at the market were steady and eggs sold for twelve cents a dozen on Friday.",
    "The school board has hired a new teacher from Ohio who will begin in September.",
]
LOOKALIKE = {
    "e": "c",
    "c": "e",
    "h": "b",
    "n": "u",
    "u": "n",
    "a": "o",
    "o": "a",
    "l": "i",
    "i": "l",
}


def ocr_noise(text: str, share: float = 0.06, seed: int = 1) -> str:
    """Letters misread the way Tesseract misreads old typewriting."""
    rnd = random.Random(seed)
    return "".join(LOOKALIKE[ch] if ch in LOOKALIKE and rnd.random() < share else ch for ch in text)


PAGE = " ".join(SENTENCES)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def add(conn, text, image=None, blank=0.0, aside=False):
    n = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES (?, ?, ?, 'watched', 'copy', 'read')",
        (f"h{n}", f"scan_{n}.jpg", f"D:/scans/scan_{n}.jpg"),
    ).lastrowid
    page = conn.execute(
        "INSERT INTO pages (scan_id, image_path, blank_score, set_aside_at) VALUES (?, ?, ?, ?)",
        (scan, str(image) if image else None, blank, "2026-10-01" if aside else None),
    ).lastrowid
    read(conn, page, text)
    return page


def read(conn, page, text):
    conn.execute("UPDATE transcriptions SET is_current = 0 WHERE page_id = ?", (page,))
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
        " VALUES (?, 'tesseract', ?, 80, 1)",
        (page, text),
    )
    conn.commit()


def pairs(conn):
    return {
        (r["page_a"], r["page_b"]): r["kind"]
        for r in conn.execute("SELECT page_a, page_b, kind FROM duplicates")
    }


def test_a_rescan_with_ocr_errors_is_the_same_page(conn):
    a = add(conn, PAGE)
    b = add(conn, ocr_noise(PAGE))
    report = find_duplicates(conn)
    assert report.checked == 2 and pairs(conn) == {(a, b): "same_page"}
    [(_, _, match)] = report.found
    assert match.score >= 60 and "words match" in match.evidence["reasons"][0]


def test_another_draft_is_very_similar(conn):
    a = add(conn, PAGE)
    draft = " ".join(SENTENCES[:5] + OTHER)  # about 40% shared, the rest rewritten
    b = add(conn, draft)
    find_duplicates(conn)
    assert pairs(conn) == {(a, b): "similar"}


def test_a_page_and_a_longer_version_of_it_are_only_similar(conn):
    # The same words, but one page has a lot more: part of it, or a retyped page with more added.
    a = add(conn, " ".join(SENTENCES[:7]))
    b = add(conn, ocr_noise(" ".join(SENTENCES)))
    report = find_duplicates(conn)
    assert pairs(conn) == {(a, b): "similar"}
    assert "shorter page may be part of the other" in report.found[0][2].evidence["reasons"][1]


def test_unrelated_pages_are_not_duplicates(conn):
    add(conn, PAGE)
    add(conn, " ".join(OTHER * 2))
    assert find_duplicates(conn).found == [] and pairs(conn) == {}


def test_short_texts_are_not_compared_by_their_words(conn):
    add(conn, SENTENCES[0])
    add(conn, SENTENCES[0])
    find_duplicates(conn)
    assert pairs(conn) == {}


def test_blank_pages_are_never_compared(conn):
    add(conn, PAGE, blank=0.99)
    add(conn, PAGE, blank=0.99)
    assert find_duplicates(conn).checked == 0 and pairs(conn) == {}


def test_three_copies_make_three_pairs(conn):
    pages = [add(conn, ocr_noise(PAGE, seed=s)) for s in (1, 2, 3)]
    find_duplicates(conn)
    assert set(pairs(conn)) == {(pages[0], pages[1]), (pages[0], pages[2]), (pages[1], pages[2])}


def test_each_reading_is_checked_once_and_a_new_one_again(conn):
    a = add(conn, PAGE)
    b = add(conn, " ".join(OTHER * 2))
    find_duplicates(conn)
    assert find_duplicates(conn).checked == 0
    read(conn, b, ocr_noise(PAGE))  # e.g. a better reading from the vision model
    assert find_duplicates(conn).checked == 1 and pairs(conn) == {(a, b): "same_page"}


def test_a_pair_a_person_decided_on_is_not_raised_again(conn):
    add(conn, PAGE)
    b = add(conn, ocr_noise(PAGE))
    find_duplicates(conn)
    conn.execute("UPDATE duplicates SET status = 'not_duplicate'")
    read(conn, b, ocr_noise(PAGE, seed=5))
    assert find_duplicates(conn).found == []
    assert conn.execute("SELECT status FROM duplicates").fetchone()[0] == "not_duplicate"


def test_copies_already_set_aside_are_left_out(conn):
    a = add(conn, PAGE)
    b = add(conn, ocr_noise(PAGE), aside=True)
    conn.execute(
        "INSERT INTO duplicates (page_a, page_b, kind, score, status, kept_page)"
        " VALUES (?, ?, 'same_page', 90, 'resolved', ?)",
        (a, b, a),
    )
    c = add(conn, ocr_noise(PAGE, seed=9))
    find_duplicates(conn)
    assert set(pairs(conn)) == {(a, b), (a, c)}  # not (b, c)


def drawing(path, paper=255, ink=0, scale=1.0, margin=0):
    """A sketch with a few words of writing: too little text to compare by its words."""
    w, h = int(800 * scale), int(1000 * scale)
    img = Image.new("L", (w + 2 * margin, h + 2 * margin), paper)
    d = ImageDraw.Draw(img)
    s = scale
    d.ellipse(
        [margin + 100 * s, margin + 120 * s, margin + 500 * s, margin + 520 * s],
        outline=ink,
        width=int(8 * s) or 1,
    )
    d.rectangle([margin + 300 * s, margin + 600 * s, margin + 700 * s, margin + 900 * s], fill=ink)
    d.line(
        [margin + 80 * s, margin + 950 * s, margin + 720 * s, margin + 60 * s],
        fill=ink,
        width=int(10 * s) or 1,
    )
    img.save(path)
    return path


def test_pages_with_little_text_are_compared_by_their_image(conn, tmp_path):
    a = add(conn, "Sketch", image=drawing(tmp_path / "a.png"))
    b = add(
        conn, "Skctch", image=drawing(tmp_path / "b.png", paper=225, ink=60, scale=0.7, margin=60)
    )
    find_duplicates(conn)
    assert pairs(conn) == {(a, b): "similar"}
    evidence = conn.execute("SELECT evidence FROM duplicates").fetchone()[0]
    assert "too little text" in evidence


def test_a_different_drawing_is_not_a_duplicate(conn, tmp_path):
    add(conn, "Sketch", image=drawing(tmp_path / "a.png"))
    other = Image.open(drawing(tmp_path / "b.png")).transpose(Image.Transpose.ROTATE_180)
    other.save(tmp_path / "c.png")
    add(conn, "Map", image=tmp_path / "c.png")
    find_duplicates(conn)
    assert pairs(conn) == {}
