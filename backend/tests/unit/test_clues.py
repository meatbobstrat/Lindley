import pytest

from lindley.assembler.clues import find_dates, page_clues, parse_marker


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


def test_file_sequence_numbers():
    c = page_clues("x" * 40, "scan_0042.jpg")
    assert c.file_seq == 42 and c.file_prefix == "scan_"
