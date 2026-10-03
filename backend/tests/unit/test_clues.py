import pytest

from lindley.assembler.clues import (
    Line,
    file_series,
    find_dates,
    is_noise,
    page_clues,
    parse_marker,
    read_number,
    text_lines,
    trim_noise,
)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("- 2 -", (2, None)),
        ("3", (3, None)),
        ("Page 2 of 3", (2, 3)),
        ("p. 4", (4, None)),
        ("2 of 5", (2, 5)),
        ("(7)", (7, None)),
        ("ii", (2, None)),
        ("IV", (4, None)),
    ],
)
def test_page_markers_found(line, expected):
    assert parse_marker(line) == expected


@pytest.mark.parametrize(
    "line", ["in 1892", "2 bu. seed oats", "1892", "I", "Page 4 of 3", "0", "Dear John,"]
)
def test_page_markers_not_found(line):
    assert parse_marker(line) is None


def test_number_alone_at_top_or_bottom_is_a_page_number_but_not_in_running_text():
    assert page_clues("3\nand so we went home.\nMother is well.").marker == (3, None)
    assert page_clues("We went home.\nMother is well.\n- 2 -").marker == (2, None)
    assert page_clues("We went home\n2 bu. seed oats and\n3 bales of hay.").marker is None


def test_page_number_from_word_positions():
    words = [
        {"text": "12", "bbox": [290, 760, 20, 14]},
        {"text": "Mother", "bbox": [40, 300, 60, 14]},
        {"text": "is", "bbox": [110, 300, 14, 14]},
    ]
    assert page_clues("Mother is well.\n12", words=words, height=800).marker == (12, None)
    words = [{"text": "2", "bbox": [40, 760, 10, 14]}, {"text": "bales", "bbox": [55, 760, 40, 14]}]
    assert page_clues("Mother is well and\n2 bales", words=words, height=800).marker is None


def w(text, x, y, conf=95.0, width=None, h=30):
    return {"text": text, "conf": conf, "bbox": [x, y, width or 18 * len(text), h]}


def line_of(text, y, x=200, conf=95.0):
    out = []
    for t in text.split():
        out.append(w(t, x, y, conf))
        x += 18 * len(t) + 14
    return out


@pytest.mark.parametrize(
    ("token", "expected"),
    [("12", (12, False)), ("-12-", (12, False)), ("1l", (11, True)), ("2O", (20, True))],
)
def test_reading_a_page_number_allows_for_ocr_slips(token, expected):
    assert read_number(token) == expected


@pytest.mark.parametrize("token", ["I", "O", "l", "1892", "0", "and"])
def test_reading_a_page_number_needs_a_real_digit(token):
    assert read_number(token) is None


def body(*rows):
    """A page of typed rows, starting a quarter of the way down."""
    words = []
    for i, r in enumerate(rows):
        words += line_of(r, 800 + 40 * i)
    return words


def page(words, height=3300):
    """Clues for a page read as these words, one line per row."""
    rows = _rows_of(words)
    text = "\n".join(" ".join(x["text"] for x in ws) for ws in rows)
    return page_clues(text, words=[x for ws in rows for x in ws], height=height)


def _rows_of(words):
    by = {}
    for x in words:
        by.setdefault(x["bbox"][1], []).append(x)
    return [by[k] for k in sorted(by)]


def test_a_page_number_among_specks_is_still_found_and_its_line_dropped():
    words = [w("/$", 1500, 200, 75), w("5", 1560, 200, 45)] + body(
        "that is worth at least $7000 per ton and more", "and so the mill was built."
    )
    c = page(words)
    assert c.marker == (5, None) and not c.marker_sure  # read at 45%: a likely page number
    assert c.first_line.startswith("that is worth")
    words = [w("OTF", 2000, 200, 24), w("ee,", 2100, 200, 19), w("63", 1250, 200, 90)] + body(
        "Held John Tenerio for trial for knocking him", "down in the street."
    )
    assert page(words).marker == (63, None) and page(words).marker_sure


def test_a_number_in_a_sentence_near_the_top_is_not_a_page_number():
    words = body("paper at Goldfield. At Reno 7 the Journal was boxing up")
    for x in words:
        x["bbox"][1] = 300  # inside the top band
    assert page(words).marker is None


def test_a_clear_number_beside_a_doubtful_one_is_the_page_number():
    words = [w("95", 1250, 200, 94), w("25", 1400, 200, 51)] + body("The mine was sold in May.")
    assert page(words).marker == (95, None)


def test_a_byline_starts_a_document():
    c = page_clues("By Lindley C.Branson\nThey had adventures in the rough in the old days.")
    assert c.starts_doc and "By Lindley" in c.starts_doc
    assert not page_clues("by the river we sat down and wept.\nIt was cold.").starts_doc


def test_noise_lines_at_the_edges_are_dropped():
    lines = [
        Line("te | ae", [w("te", 200, 100, 34), w("|", 260, 100, 44), w("ae", 300, 100, 30)]),
        Line("id", [w("id", 900, 160, 54)]),
        Line("The Tonopah Sun began", line_of("The Tonopah Sun began", 220)),
        Line("Will", [w("Will", 200, 900, 96)]),  # a signature stays
        Line("~ =", [w("~", 200, 960, 10), w("=", 240, 960, 5)]),
    ]
    kept = trim_noise(lines)
    assert [ln.text for ln in kept] == ["The Tonopah Sun began", "Will"]
    assert is_noise(Line("~~ ==")) and not is_noise(Line("Yours truly"))


def test_specks_before_the_first_word_dont_make_a_page_start_mid_sentence():
    words = [w("ae", 120, 800, 30)] + line_of("The Sun began publication in May.", 800)
    c = page(words + line_of("It was a daily paper.", 840))
    assert c.first_line.startswith("The Sun") and not c.starts_mid


def test_a_scrap_tesseract_read_out_of_order_goes_back_into_its_line():
    stage = [w("stage", 200, 300)]
    rest = line_of("Fellow passengers were Tom and Bill", 300, x=320)
    first = line_of("We took the morning train out of town", 260)
    lines = text_lines(
        "stage\nWe took the morning train out of town\nFellow passengers were Tom and Bill",
        stage + first + rest,
    )
    assert [ln.text for ln in lines] == [
        "We took the morning train out of town",
        "stage Fellow passengers were Tom and Bill",
    ]


@pytest.mark.parametrize(
    ("text", "iso"),
    [
        ("Xenia, O., March 4th, 1892", "1892-03-04"),
        ("Apr 4 1892", "1892-04-04"),
        ("the 4th of July, 1876", "1876-07-04"),
        ("Sept. 1901", "1901-09"),
        ("4/3/92", "1892-04-03"),
        ("in the year 1868 or so", "1868"),
    ],
)
def test_dates(text, iso):
    assert find_dates(text)[0][1] == iso


def test_letter_start_and_end():
    c = page_clues(
        "Bellbrook, O., Jan. 6 1892\nDear Mother,\nWe are all well.\nYour loving son\nJohn"
    )
    assert c.salutation == "Dear Mother"
    assert c.closing == "Your loving son" and c.signature == "John"
    assert c.kind == "letter" and "Bellbrook" in c.places and "John" in c.people
    assert c.starts_doc and c.ends_doc


def test_friend_salutation_gives_a_name():
    c = page_clues("Friend Branson,\nI have your letter of the 3rd.")
    assert c.salutation == "Friend Branson" and "Branson" in c.people


def test_mid_sentence_breaks():
    a = page_clues("We had a letter from Clara and she is teaching school again this")
    b = page_clues("winter at the Hale school.\nFather is well.")
    assert a.ends_mid and not a.starts_mid
    assert b.starts_mid
    assert not page_clues("We are all well.\nYours truly\nWill").ends_mid


def test_receipts_deeds_diaries_notes_and_blanks():
    receipt = (
        "XENIA FEED & SEED CO.\nApr 4 1892\nSold to J. Branson\n"
        "2 bu. seed oats .... 1.10\nTOTAL .... 1.10\nPaid."
    )
    c = page_clues(receipt)
    assert c.kind == "receipt" and c.letterhead == "XENIA FEED & SEED CO" and c.ends_form == "Paid"
    deed = (
        "KNOW ALL MEN BY THESE PRESENTS,\n"
        "that the grantor does hereby convey forty acres\nPage 1 of 3"
    )
    c = page_clues(deed)
    assert c.kind == "deed" and c.heading and c.marker == (1, 3)
    assert page_clues("Monday, Jan. 5.\nCold and clear.").kind == "diary"
    assert page_clues("Ask Clara about the Dayton letters.").kind == "notes"
    assert page_clues("   \n ").kind == "blank"
    assert page_clues("Lots of text here", blank_score=0.99).kind == "blank"


@pytest.mark.parametrize(
    ("name", "series"),
    [
        ("Image.jpg", ("image", 1)),  # Windows numbers only the files after the first
        ("Image (2).jpg", ("image", 2)),
        ("Image 3.jpg", ("image", 3)),  # the same on a Mac
        ("scan_0042.tif", ("scan_", 42)),
        ("Letter to Clara.jpg", ("letter to clara", 1)),
        ("", ("", None)),
    ],
)
def test_a_file_name_gives_its_series_and_number(name, series):
    assert file_series(name) == series


def test_paid_ends_a_receipt_but_not_a_sentence_in_a_story():
    tail = "the old cook went on with his work.\n"
    assert page_clues("Sold to J. Hale\n" + tail + "Paid in full, T. Hale").ends_form == "Paid"
    story = "He walked into the saloon\nAnd paid no attention to the drunken Bobo on the floor."
    assert page_clues(story).ends_form is None


def test_a_doubtful_page_number_one_doesnt_start_a_document():
    sure = page([w("1", 1250, 200, 95)] + body("The mine was sold in May to Goldfield men."))
    doubtful = page([w("1", 1250, 200, 45)] + body("The mine was sold in May to Goldfield men."))
    assert sure.marker == doubtful.marker == (1, None)
    assert sure.starts_doc == "is numbered page 1" and doubtful.starts_doc is None
