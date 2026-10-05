import json
import re
import sqlite3

import pytest

from lindley import organise
from lindley.assembler import assemble
from lindley.assembler.ai import refine
from lindley.assembler.bench import TruePage, load, make_batch, proposed_groups, score
from lindley.assembler.evidence import pair
from lindley.assembler.model import Group, Page
from lindley.assembler.run import load_inbox
from lindley.config import AssemblerSettings
from lindley.db.database import SCHEMA_VERSION, connect, init_db
from lindley.duplicates.resolve import quoted

LETTER = [
    "Xenia, O., March 4 1892\nDear Sister,\nWe are all well and the river came up over the",
    "low road again last week.\nFather sold the bay mare at the sale.\n- 2 -",
    "- 3 -\nThe corn is in and the wheat looks better than it did last year.",
]
LAST = "- 4 -\nPlease write soon.\nYour loving brother\nWill"

# Passing a chat AI to assemble() is the OK to call it; these pages need not wait for a person
AI_ON = AssemblerSettings(ask_ai_after_days=0)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def pages(texts, doc="x"):
    return [TruePage(t, doc, i, "letter") for i, t in enumerate(texts)]


def placement(conn):
    return [
        tuple(r) for r in conn.execute("SELECT id, document_id, position FROM pages ORDER BY id")
    ]


def open_suggestions(conn, kind=None):
    sql = "SELECT * FROM suggestions WHERE status = 'open'" + (" AND kind = ?" if kind else "")
    return conn.execute(sql, (kind,) if kind else ()).fetchall()


def test_a_clear_letter_becomes_one_lindley_document(conn):
    ids = list(load(conn, pages(LETTER + [LAST])))
    report = assemble(conn)
    assert report.documents_created == 1 and report.inbox_left == 0
    doc = conn.execute("SELECT * FROM documents").fetchone()
    assert doc["origin"] == "lindley" and doc["name_source"] == "lindley"
    assert doc["name"] == "Letter from Will, March 1892" and doc["doc_date"] == "1892-03-04"
    assert doc["grouping_confidence"] >= 75
    assert any("Page numbers" in r for r in json.loads(doc["reasons"]))
    assert [r[0] for r in conn.execute("SELECT id FROM pages ORDER BY position")] == ids
    log = conn.execute("SELECT * FROM history").fetchone()
    assert log["actor"] == "lindley" and log["action"] == "group_pages"
    assert conn.execute("SELECT COUNT(*) FROM facts WHERE kind = 'page_marker'").fetchone()[0] == 3
    assert (
        conn.execute("SELECT COUNT(*) FROM page_links WHERE relation = 'continues'").fetchone()[0]
        >= 2
    )


def test_pages_scanned_in_the_wrong_order_are_put_right(conn):
    load(conn, pages([LETTER[0], LETTER[2], LETTER[1], LAST]))
    assemble(conn)
    order = [
        r[0]
        for r in conn.execute(
            "SELECT t.text FROM pages p JOIN transcriptions t ON t.page_id = p.id"
            " ORDER BY p.position"
        )
    ]
    assert order == LETTER + [LAST]


def test_a_blank_page_is_only_suggested_for_set_aside(conn):
    (blank,) = load(conn, [TruePage("", "b", 0, "blank")])
    assemble(conn)
    assert placement(conn) == [(blank, None, None)]
    (s,) = open_suggestions(conn, "set_aside")
    assert s["page_id"] == blank


def test_a_late_page_joins_an_untouched_lindley_document(conn):
    load(conn, pages(LETTER))
    assert assemble(conn).documents_created == 1
    (late,) = load(conn, pages([LAST]), start_seq=90)
    report = assemble(conn)
    assert report.pages_added == 1 and report.documents_created == 0
    assert conn.execute("SELECT position FROM pages WHERE id = ?", (late,)).fetchone()[0] == 4
    assert conn.execute("SELECT action FROM history ORDER BY id DESC").fetchone()[0] == "add_pages"


def test_a_late_page_is_only_suggested_for_a_document_a_person_named(conn):
    load(conn, pages(LETTER))
    assemble(conn)
    conn.execute("UPDATE documents SET name = 'Letters from Will', name_source = 'user'")
    (late,) = load(conn, pages([LAST]), start_seq=90)
    assemble(conn)
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] is None
    (s,) = open_suggestions(conn, "add_to_document")
    assert s["page_id"] == late and s["confidence"] >= 75 and json.loads(s["reasons"])
    # The hint lists where the page most likely belongs, best first, for a person to choose
    (first, *_) = json.loads(s["payload"])["candidates"]
    assert first["document"] == s["document_id"] and first["name"] == "Letters from Will"
    assert first["confidence"] == s["confidence"] and first["at"] == "end"


@pytest.mark.parametrize("work", ["gave its type", "took a page out and put it back"])
def test_a_late_page_is_only_suggested_for_a_document_a_person_worked_on(conn, work):
    ids = load(conn, pages(LETTER))
    assemble(conn)
    doc = conn.execute("SELECT id FROM documents").fetchone()[0]
    if work == "gave its type":
        organise.update_document(conn, doc, {"doc_type": "diary"})
    else:
        organise.move_pages(conn, [list(ids)[-1]], "aside")
        organise.move_pages(conn, [list(ids)[-1]], "document", doc)
    (late,) = load(conn, pages([LAST]), start_seq=90)
    assert assemble(conn).pages_added == 0
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] != doc


def test_completed_documents_are_never_changed(conn):
    load(conn, pages(LETTER))
    assemble(conn)
    conn.execute("UPDATE documents SET status = 'complete'")
    before = conn.execute("SELECT id, position FROM pages WHERE document_id IS NOT NULL").fetchall()
    load(conn, pages([LAST]), start_seq=90)
    assemble(conn)
    doc_id = conn.execute("SELECT id FROM documents WHERE status = 'complete'").fetchone()[0]
    after = conn.execute(
        "SELECT id, position FROM pages WHERE document_id = ?", (doc_id,)
    ).fetchall()
    assert [tuple(r) for r in after] == [tuple(r) for r in before]


def test_a_dismissed_suggestion_is_not_made_again(conn):
    (blank,) = load(conn, [TruePage("", "b", 0, "blank")])
    assemble(conn)
    conn.execute("UPDATE suggestions SET status = 'dismissed', resolved_at = datetime('now')")
    assemble(conn)
    assert open_suggestions(conn) == []


def test_running_again_changes_nothing(conn):
    truth = load(conn, make_batch(3).pages)
    assemble(conn)
    first = (
        placement(conn),
        len(open_suggestions(conn)),
        conn.execute("SELECT COUNT(*) FROM documents").fetchone(),
    )
    report = assemble(conn)
    assert (
        placement(conn),
        len(open_suggestions(conn)),
        conn.execute("SELECT COUNT(*) FROM documents").fetchone(),
    ) == first
    assert report.documents_created == 0 and report.pages_added == 0
    assert score(conn, truth).wrong_documents == 0


# ---------------------------------------------------------------- The AI step

AMBIGUOUS = [
    "The corn is in and the wheat looks better than it did last year.\n"
    "The new preacher is a young man from Cincinnati and the ladies think him very fine.",
    "Prices are so low this year that it hardly pays to haul the hogs to Dayton.\n"
    "It has rained every day this week and the lane is nothing but mud.",
]


class Scripted:
    def __init__(self, reply):
        self.reply, self.calls = reply, 0

    def chat(self, messages):
        self.calls += 1
        return self.reply(json.loads(messages[-1].content)) if callable(self.reply) else self.reply

    def chat_stream(self, messages):
        yield self.chat(messages)


def test_a_valid_ai_reply_settles_an_uncertain_break(conn):
    a, b = load(conn, pages(AMBIGUOUS))
    ai = Scripted(
        lambda req: json.dumps(
            {
                "documents": [
                    {
                        "pages": [b, a],
                        "name": "Farm notes, 1890s",
                        "type": "notes",
                        "date": None,
                        "confidence": 88,
                        "reasons": ["Both pages are about the farm's crops and prices"],
                    }
                ],
                "unplaced": [],
            }
        )
    )
    report = assemble(conn, AI_ON, chat=ai)
    assert report.ai_calls >= 1 and report.documents_created == 1
    doc = conn.execute("SELECT * FROM documents").fetchone()
    assert doc["name"] == "Farm notes, 1890s"
    assert json.loads(doc["reasons"]) == ["Both pages are about the farm's crops and prices"]
    assert [r[0] for r in conn.execute("SELECT id FROM pages ORDER BY position")] == [b, a]


def test_a_group_the_ai_names_nothing_gets_the_rules_name(conn):
    a, b = load(conn, pages(AMBIGUOUS))
    reply = {"pages": [a, b], "name": "", "confidence": 88, "reasons": ["Farm"]}
    ai = Scripted(json.dumps({"documents": [reply], "unplaced": []}))
    # One call only: none is left to ask for a better name
    assert assemble(conn, AI_ON, chat=ai, max_ai_calls=1).documents_created == 1
    name = conn.execute("SELECT name FROM documents").fetchone()[0]
    assert name.startswith("Pages starting “The corn is in")


def test_a_group_the_ai_is_less_sure_of_is_suggested_and_marked_as_its(conn):
    a, b = load(conn, pages(AMBIGUOUS))
    reply = {"pages": [a, b], "name": "Farm notes", "confidence": 65, "reasons": ["Farm"]}
    ai = Scripted(json.dumps({"documents": [reply], "unplaced": []}))
    report = assemble(conn, AI_ON, chat=ai)
    assert report.documents_created == 0 and report.inbox_left == 2
    [s] = open_suggestions(conn, "group_pages")
    assert s["confidence"] == 65 and json.loads(s["payload"])["checked_by_ai"] is True


def test_proposed_groups_are_judged_against_the_answer_key(conn):
    truth = load(conn, pages(LETTER + [LAST]) + pages(AMBIGUOUS, "notes"))
    a, b = (pid for pid, tp in truth.items() if tp.doc == "notes")
    reply = {"pages": [a, b], "name": "Farm notes", "confidence": 65, "reasons": ["Farm"]}
    assemble(conn, AI_ON, chat=Scripted(json.dumps({"documents": [reply], "unplaced": []})))
    groups = sorted(proposed_groups(conn, truth), key=lambda g: g.made)
    assert [(g.made, g.by_ai, g.pure, g.exact) for g in groups] == [
        (False, True, True, True),  # the AI's hint
        (True, False, True, True),  # the letter
    ]


def test_a_letter_with_no_greeting_is_not_run_on_into_the_story_before_it(conn):
    """A carbon of a letter with a date line and who it's to, but no "Dear ...", scanned
    after a typescript: as in the dev library, where it was added to the typescript."""
    story = TruePage(
        "TELEGRAPH CREEK\nBy Lindley C. Branson\nTelegraph Creek is the name of a camp on the"
        " Stikine.\nThe wire was to reach Europe by way of Canada and Alaska.",
        "story",
        0,
        "page",
    )
    letter = TruePage(
        "COPY -------For your information\nEly ,Nevada,June 24,1940\nHonorable Grey Mashburn,\n"
        "Attorney General,\nCarson City,Nevada\nOn the morning of April 26 I was at the bar.\n"
        "Respectfully Yours,",
        "letter",
        0,
        "letter",
    )
    a, b = load(conn, [story, letter])
    assemble(conn)
    placed = dict(conn.execute("SELECT id, document_id FROM pages").fetchall())
    grouped = [json.loads(s["payload"])["pages"] for s in open_suggestions(conn, "group_pages")]
    assert placed[a] is None or placed[a] != placed[b]
    assert [a, b] not in grouped and [b, a] not in grouped


def test_without_ai_the_same_pages_wait_in_the_inbox(conn):
    load(conn, pages(AMBIGUOUS))
    report = assemble(conn)
    assert report.ai_calls == 0 and report.documents_created == 0 and report.inbox_left == 2
    # What the AI would have been asked about is still counted, to measure the rules by
    assert report.ai_windows == 1 and report.ai_pages == 2


def _pages():
    return [Page(1, 1, "scan_0001.jpg", AMBIGUOUS[0]), Page(2, 2, "scan_0002.jpg", AMBIGUOUS[1])]


@pytest.mark.parametrize(
    "reply",
    [
        "Sure! These pages go together.",
        json.dumps({"documents": [{"pages": [1, 2, 3], "confidence": 90}], "unplaced": []}),
        json.dumps({"documents": [{"pages": [1, 1], "confidence": 90}], "unplaced": [2]}),
        json.dumps({"documents": [{"pages": [1], "confidence": 90}], "unplaced": []}),
        json.dumps({"documents": [{"pages": ["1", 2]}], "unplaced": []}),
        # as Qwen3.5 4B answered: pages it couldn't place, as objects
        json.dumps({"documents": [{"pages": [1]}], "unplaced": [{"id": 2, "reason": "?"}]}),
    ],
    ids=["not json", "unknown id", "id twice", "page left out", "id not a number", "unplaced"],
)
def test_bad_ai_replies_are_rejected(reply):
    ps = _pages()
    result = refine(Scripted(reply), ps, [Group(ps, 60)])
    assert result.groups is None and result.problem


def test_a_failing_ai_leaves_the_rules_in_charge(conn):
    load(conn, pages(LETTER + [LAST]))

    class Broken:
        def chat(self, messages):
            raise ConnectionError("no route to host")

    report = assemble(conn, AI_ON, chat=Broken())
    assert report.documents_created == 1


def _farm_notes(req):
    a, b = (p["id"] for p in req["pages"])
    return {
        "documents": [{"pages": [a, b], "name": "Farm notes", "confidence": 60}],
        "unplaced": [],
    }


def test_the_same_question_is_never_paid_for_twice(conn):
    load(conn, pages(AMBIGUOUS))
    ai = Scripted(lambda req: json.dumps(_farm_notes(req)))
    first = assemble(conn, AI_ON, chat=ai)
    assert first.ai_calls == 1 and ai.calls == 1
    # Not confident enough to make a document, so the pages wait in the Inbox; the next run
    # uses the AI's earlier answer instead of asking again, even with no AI allowed now.
    for again in (assemble(conn, AI_ON, chat=ai), assemble(conn)):
        assert again.ai_calls == 0 and again.ai_reused == 1
    assert ai.calls == 1
    assert conn.execute("SELECT purpose FROM ai_answers").fetchall()[0][0] == "assemble"


def test_a_page_read_again_is_a_new_question(conn):
    a, _ = load(conn, pages(AMBIGUOUS))
    ai = Scripted(lambda req: json.dumps(_farm_notes(req)))
    assemble(conn, AI_ON, chat=ai)
    conn.execute(
        "UPDATE transcriptions SET text = text || ' Wheat was dear.' WHERE page_id = ?", (a,)
    )
    assert assemble(conn, AI_ON, chat=ai).ai_calls == 1 and ai.calls == 2


def test_a_rejected_reply_is_kept_but_a_failed_call_is_not(conn):
    load(conn, pages(AMBIGUOUS))
    ai = Scripted("Sure! These pages go together.")
    assemble(conn, AI_ON, chat=ai)
    assert assemble(conn, AI_ON, chat=ai).ai_reused == 1 and ai.calls == 1

    conn.execute("DELETE FROM ai_answers")

    class Broken:
        def chat(self, messages):
            raise ConnectionError("no route to host")

    assert assemble(conn, AI_ON, chat=Broken()).ai_calls == 1
    assert not conn.execute("SELECT 1 FROM ai_answers").fetchone()


def test_the_ai_is_asked_no_more_than_it_may_be(conn):
    load(conn, pages(AMBIGUOUS))
    ai = Scripted("{}")
    assert assemble(conn, chat=ai, max_ai_calls=0).ai_calls == 0
    assert ai.calls == 0
    assert assemble(conn).ai_calls == 0  # no chat AI given: the rules decide alone


# ---------------------------------------------------------------- Schema and bench


def test_v1_database_gains_documents_reasons(tmp_path):
    from importlib.resources import files

    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    v1 = "\n".join(ln for ln in schema.splitlines() if "reasons             TEXT" not in ln)
    # Nor history.batch, which came in version 4.
    v1 = re.sub(r"(after\s+TEXT),([^\n]*)\n\s+batch\s+INTEGER[^\n]*", r"\1 \2", v1)
    v1 = re.sub(r"CREATE INDEX IF NOT EXISTS idx_history_batch[^\n]*\n", "", v1)
    db = tmp_path / "old.db"
    c = sqlite3.connect(db)
    c.executescript(v1 + "\nPRAGMA user_version = 1;")
    c.close()
    init_db(db)
    c = connect(db)
    assert "reasons" in {r["name"] for r in c.execute("PRAGMA table_info(documents)")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_bench_rules_only(tmp_path):
    f1s, made, wrong = [], 0, 0
    for seed in range(12):
        db = tmp_path / f"bench{seed}.db"
        init_db(db)
        c = connect(db)
        batch = make_batch(seed)
        truth = load(c, batch.pages)
        assemble(c)
        if batch.late:
            truth |= load(c, batch.late, start_seq=len(batch.pages) + 50)
            assemble(c)
        s = score(c, truth)
        c.close()
        f1s.append(s.f1)
        made, wrong = made + s.documents_made, wrong + s.wrong_documents
    assert sum(f1s) / len(f1s) >= 0.9
    assert wrong <= 0.02 * made


def test_near_identical_scans_are_linked_as_duplicates():
    a = Page(1, 1, "scan_0001.jpg", LETTER[1], phash="a2d4b0ccd2c8d0a8")
    b = Page(2, 2, "scan_0007.jpg", LETTER[1], phash="a2d4b0ccd2c8d0ab")  # 2 bits apart
    c = Page(3, 3, "scan_0009.jpg", LETTER[1], phash="5d2b4f332d372f57")
    assert "duplicate" in [lk.relation for lk in pair(a, b).links]
    assert "duplicate" not in [lk.relation for lk in pair(a, c).links]


def test_near_empty_pages_are_not_called_duplicates():
    # A line or two of writing hashes to almost all zeros, so the hashes say nothing.
    a = Page(1, 1, "a.jpg", "Received of J. Branson three dollars", phash="a000000000000000")
    b = Page(2, 2, "b.jpg", "Paid in full, with thanks, T. Hale", phash="a200000000000000")
    assert "duplicate" not in [lk.relation for lk in pair(a, b).links]


def test_a_turned_page_is_measured_upright(conn):
    load(conn, pages(LETTER[:2]))
    conn.execute("UPDATE pages SET width_px = 1000, height_px = 1400")
    conn.execute("UPDATE pages SET detected_rotation = 90 WHERE id = (SELECT min(id) FROM pages)")
    conn.commit()
    assert [p.height for p in load_inbox(conn)] == [1000, 1400]


# ---------------------------------------------------------------- Copies of a page


def test_a_page_scanned_twice_never_shares_a_document_with_its_copy(conn):
    ids = list(load(conn, pages([LETTER[0], LETTER[1], LETTER[1], LETTER[2], LAST])))
    conn.execute(
        "INSERT INTO duplicates (page_a, page_b, kind, score) VALUES (?, ?, 'same_page', 95)",
        (ids[1], ids[2]),
    )
    conn.commit()
    assemble(conn)
    docs = {r[0]: r[1] for r in conn.execute("SELECT id, document_id FROM pages")}
    assert docs[ids[1]] is None or docs[ids[1]] != docs[ids[2]]


def test_a_page_scanned_again_is_set_aside_and_the_pages_around_it_still_join(conn):
    ids = list(load(conn, pages([LETTER[0], LETTER[1], LETTER[1], LETTER[2], LAST])))
    conn.execute(
        "INSERT INTO duplicates (page_a, page_b, kind, score) VALUES (?, ?, 'same_page', 88)",
        (ids[1], ids[2]),
    )
    conn.commit()
    assemble(conn)
    letter = [r[0] for r in conn.execute("SELECT id FROM pages WHERE document_id IS NOT NULL")]
    assert sorted(letter) == [ids[0], ids[1], ids[3], ids[4]]
    aside = open_suggestions(conn, "set_aside")
    assert [s["page_id"] for s in aside] == [ids[2]]
    assert aside[0]["confidence"] == 88  # as sure as it is that the two are one page
    # Scanners name every scan alike in each folder, so it says which folder
    assert "It looks like scan_0002.jpg (in Scans) scanned again" in aside[0]["reasons"]


def test_a_page_whose_copy_is_already_in_a_document_is_set_aside_not_grouped(conn):
    # A document scanned again later, after the first scan was sorted: no second document.
    first = list(load(conn, pages(LETTER + [LAST])))
    assemble(conn)
    again = list(load(conn, pages(LETTER + [LAST], "again"), start_seq=90))
    conn.executemany(
        "INSERT INTO duplicates (page_a, page_b, kind, score) VALUES (?, ?, 'same_page', 95)",
        list(zip(first, again, strict=True)),
    )
    conn.commit()
    assemble(conn)
    assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 1
    aside = open_suggestions(conn, "set_aside")
    assert sorted(s["page_id"] for s in aside) == sorted(again)
    (name,) = conn.execute("SELECT name FROM documents").fetchone()
    reasons = [r for s in aside for r in json.loads(s["reasons"])]
    assert f"It looks like page 1 of {quoted(name)} scanned again" in reasons


def test_copies_are_a_hard_break():
    a = Page(1, 1, "scan_0001.jpg", LETTER[0], copies=frozenset({2}))
    b = Page(2, 1, "scan_0001.jpg", LETTER[1], page_index=1)
    p = pair(a, b)
    assert p.score == 0 and p.breaks == ["the two scans are copies of the same page"]


def test_a_page_is_not_added_to_a_document_holding_its_copy():
    from lindley.assembler.run import DocEnds, _best_match

    first, last = Page(1, 1, "a.jpg", LETTER[0]), Page(2, 2, "b.jpg", LETTER[1])
    doc = DocEnds(9, "Letter", first, last, False, frozenset({1, 2}))
    late = Page(3, 3, "c.jpg", LETTER[2], copies=frozenset({2}))
    assert _best_match(Group([late], 60), [doc]) is None
    stranger = Page(4, 4, "d.jpg", LETTER[2])
    assert _best_match(Group([stranger], 60), [doc]) is not None


def test_file_numbers_count_only_within_one_folder():
    from lindley.assembler.evidence import adjacent
    from lindley.assembler.segment import scan_order

    def scan(pid, name, folder):
        return Page(pid, pid, name, LETTER[1], folder=folder)

    first, second = scan(1, "Image.jpg", "D:/Scans/Abe"), scan(2, "Image (2).jpg", "D:/Scans/Abe")
    other = scan(3, "Image (2).jpg", "D:/Scans/Burbanks")
    assert adjacent(first, second) and not adjacent(first, other)
    third = scan(4, "Image (3).jpg", "D:/Scans/Abe")
    assert [p.id for p in scan_order([other, third, second, first])] == [1, 2, 4, 3]


def test_a_doubtful_page_number_doesnt_move_a_page():
    from lindley.assembler.segment import order

    stream = [Page(i, i, f"scan_{i:04d}.jpg", LETTER[1]) for i in (1, 2, 3)]
    stream[0].clues.marker, stream[0].clues.marker_sure = (9, None), False
    assert [p.id for p in order(stream)[0]] == [1, 2, 3]
    stream[0].clues.marker_sure = True
    assert [p.id for p in order(stream)[0]] == [2, 3, 1]


def test_a_gap_in_the_page_numbers_says_which_page_is_missing():
    from lindley.assembler.model import Page
    from lindley.assembler.segment import missing_pages

    def numbered(n):
        return Page(n, n, f"scan_{n:04d}.jpg", f"{n}\nand so the story went on and on.")

    assert missing_pages([numbered(1), numbered(2), numbered(4)]) == "Page 3 seems to be missing"
    assert missing_pages([numbered(1), numbered(4)]) == "Pages 2 and 3 seem to be missing"
    assert missing_pages([numbered(1), numbered(2), numbered(3)]) is None


# ---------------------------------------------------------------- Linking pages scanned apart

RECEIPT = (
    "BELLBROOK MERCANTILE\nSold to J. Hale\n2 bu. seed oats .......... 1.20\n"
    "TOTAL ...... 1.20\nPaid"
)


def _stream(texts):
    return [Page(i, i, f"scan_{i:04d}.jpg", t) for i, t in enumerate(texts, 1)]


def test_parts_of_a_letter_scanned_apart_are_joined():
    from lindley.assembler.segment import segment

    first = LETTER[0] + "\n- 1 -"  # numbered, and ends mid-sentence: page 2 clearly follows
    groups, _, _ = segment(_stream([first, RECEIPT, LETTER[1], LETTER[2], LAST]))
    letter = next(g for g in groups if 1 in g.ids)
    assert letter.ids == [1, 3, 4, 5]
    assert "Parts scanned apart were joined" in letter.reasons


def test_a_page_two_that_could_follow_either_page_one_joins_neither():
    from lindley.assembler.segment import segment

    body = "The road ran north over the summit and down the long grade toward the camp"
    one = body + "\nand the wagons came on over the"
    two = "- 2 -\nlong bridge at noon and went on.\n" + body
    groups, _, _ = segment(_stream([one, RECEIPT, one + " old", RECEIPT, two, RECEIPT, two]))
    assert not any(len([p for p in g.pages if "summit" in p.text]) > 1 for g in groups)


def test_folders_a_person_sorted_are_an_answer_key(conn):
    from lindley.assembler.bench import real_answers

    def scan(folder, name, blank=0.1):
        sid = conn.execute(
            "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
            " VALUES (?, ?, ?, 'watched', 'copy', 'read')",
            (f"{folder}/{name}", name, f"D:/Sorted/{folder}/{name}"),
        ).lastrowid
        pid = conn.execute(
            "INSERT INTO pages (scan_id, blank_score) VALUES (?, ?)", (sid, blank)
        ).lastrowid
        conn.execute(
            "INSERT INTO transcriptions (page_id, source, text, is_current)"
            " VALUES (?, 'tesseract', 'Some words', 1)",
            (pid,),
        )
        return pid

    with conn:
        b2, b1 = scan("Burbanks", "Image (2).jpg"), scan("Burbanks", "Image.jpg")
        scan("Burbanks", "Image (3).jpg", blank=0.99)
        a = [scan("Abe", f"Image ({n}).jpg") for n in (10, 2)]
        scan("Notes", "Image.jpg")
    # Abe, then Burbanks, each in file name order; no blank pages, and no one-page folders
    assert real_answers(conn) == [[a[1], a[0]], [b1, b2]]


def test_a_pdf_made_from_scans_gives_their_order(conn):
    from lindley.assembler.bench import real_answers

    def page(folder, name, text, mime="image/jpeg", scan=None, index=0):
        if scan is None:
            scan = conn.execute(
                "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode,"
                " status, mime_type) VALUES (?, ?, ?, 'watched', 'copy', 'read', ?)",
                (f"{folder}/{name}", name, f"D:/Scans/{folder}/{name}", mime),
            ).lastrowid
        pid = conn.execute(
            "INSERT INTO pages (scan_id, page_index, blank_score) VALUES (?, ?, 0.1)",
            (scan, index),
        ).lastrowid
        conn.execute(
            "INSERT INTO transcriptions (page_id, source, text, is_current)"
            " VALUES (?, 'tesseract', ?, 1)",
            (pid, text),
        )
        return scan, pid

    texts = [
        f"{w} " * 3 + "the wagons came over the summit road toward the mining camp at noon"
        for w in ("Monday morning early", "Tuesday after supper", "Wednesday in the rain")
    ]
    with conn:
        # The scans were named out of order; the PDF has the pages as they're read
        _, third = page("Mill", "Image.jpg", texts[2])
        _, first = page("Mill", "Image (2).jpg", texts[0])
        page("Mill", "Image (3).jpg", texts[0] + " again")  # scanned again: not in the PDF
        _, second = page("Mill", "Image (4).jpg", texts[1])
        pdf, _ = page("Mill", "Mill.pdf", texts[0], "application/pdf")
        for i, text in enumerate(texts[1:], 1):
            page("Mill", "Mill.pdf", text, scan=pdf, index=i)
        b1, b2 = (page("Notes", f"Image{n}.jpg", texts[0])[1] for n in ("", " (2)"))
        page("", "loose.jpg", texts[1]), page("", "loose (2).jpg", texts[2])  # not sorted yet
    assert real_answers(conn) == [[first, second, third], [b1, b2]]

    # Learning from a person's answers: the PDF as its scans; folders alone aren't answers, and
    # a PDF read in without its scans is its own pages
    from lindley.assembler.bench import assembled_answers

    with conn:
        alone, p1 = page("Elsewhere", "Letter.pdf", texts[2], "application/pdf")
        _, p2 = page("Elsewhere", "Letter.pdf", texts[1], scan=alone, index=1)
    assert assembled_answers(conn) == [[first, second, third], [p1, p2]]


def test_the_bench_files_scans_by_each_habit():
    from lindley.assembler.bench import folder_plan

    docs = ["a", "a", "b", "a", "c"]
    assert folder_plan(docs, "one_folder") == [
        ("D:/Scans", f"scan_{n:04d}.jpg") for n in range(1, 6)
    ]
    assert folder_plan(docs, "per_document") == [
        ("D:/Scans/a", "Image.jpg"),
        ("D:/Scans/a", "Image (2).jpg"),
        ("D:/Scans/b", "Image.jpg"),
        ("D:/Scans/a", "Image (3).jpg"),
        ("D:/Scans/c", "Image.jpg"),
    ]
    mixed = [folder_plan(docs, "mixed", seed) for seed in range(20)]
    for plan in mixed:  # a document's scans all go in one place; loose ones are numbered apart
        for d in "abc":
            assert len({f for (f, _), x in zip(plan, docs, strict=True) if x == d}) == 1
        loose = [n for f, n in plan if f == "D:/Scans"]
        assert loose == [f"scan_{n:04d}.jpg" for n in range(1, len(loose) + 1)]
    assert any(p[0][0] == "D:/Scans" for p in mixed) and any(p[0][0] != "D:/Scans" for p in mixed)
    # Pages of a document scanned later go in the same folder, numbered on
    assert folder_plan(["a"], "per_document", start=90) == [("D:/Scans/a", "Image (90).jpg")]


QUIET = [  # two pages with little on them to say whether they go together
    "The wagons came on over the hill at noon and the men stopped\nto water the horses at"
    " the ford below the old stone mill.",
    "We went down to the river after dinner and sat a while\nin the shade of the sycamores"
    " until the bell rang for supper.",
]


def _filed(folder, size, library=500, texts=QUIET, first=1):
    return [
        Page(i, i, f"Image ({i}).jpg", t, folder=folder, folder_pages=size, library_pages=library)
        for i, t in enumerate(texts, first)
    ]


def test_a_small_folder_counts_towards_pages_going_together_and_a_big_one_hardly_at_all():
    small = pair(*_filed("D:/Scans/Letter to Will", 3), False).score
    big = pair(*_filed("D:/Scans", 400), False).score
    unknown = pair(*_filed("", 0, 0), False).score
    assert small > unknown + 0.1
    assert abs(big - unknown) < 0.03


def test_one_folder_everything_goes_into_says_nothing_however_few_pages_it_holds_yet():
    unknown = pair(*_filed("", 0, 0), False).score
    assert pair(*_filed("D:/Scans", 2, 2), False).score == unknown
    assert pair(*_filed("D:/Scans", 500, 500), False).score == unknown


def test_pages_from_different_folders_are_less_likely_to_go_together():
    a, b = _filed("D:/Scans/Letter to Will", 3)
    b.folder = "D:/Scans/Deed"
    assert pair(a, b, False).features["folder_differs"] == 1.0
    assert pair(a, b, False).score < pair(*_filed("", 0, 0), False).score - 0.1


def test_a_group_that_is_a_whole_small_folder_says_so():
    from lindley.assembler.segment import segment

    letter = _filed("D:/Scans/Letter to Will", 3, texts=LETTER)
    big = _filed(
        "D:/Scans/Receipts", 120, texts=[RECEIPT, RECEIPT.replace("1.20", "2.40")], first=9
    )
    groups = {tuple(g.ids): g for g in segment(letter + big)[0]}
    g = groups[(1, 2, 3)]
    assert g.features["whole_folder"] == 1.0 and g.features["folders_mixed"] == 0.0
    assert "They're every scan in the folder Letter to Will" in g.reasons
    assert g.confidence >= 90  # enough to become a document on its own
    # A big folder may hold many documents, so being all of it being sorted says nothing
    assert not any(g.features["whole_folder"] for ids, g in groups.items() if 9 in ids)


def test_each_page_knows_how_many_pages_its_folder_and_the_library_hold(conn):
    load(conn, pages(LETTER, "will") + pages([RECEIPT], "store"), habit="per_document")
    sizes = {p.text: (p.folder_pages, p.library_pages) for p in load_inbox(conn)}
    assert sizes[LETTER[0]] == (3, 4) and sizes[RECEIPT] == (1, 4)


def test_a_shared_folder_doesnt_carry_pages_over_a_letters_greeting():
    a, b = _filed("D:/Scans/Letters", 3, texts=[QUIET[0], "Dear Mother,\n" + QUIET[1]])
    assert pair(a, b, False).features["folder_shared"] == 0.0
    a, b = _filed("D:/Scans/Letters", 3, texts=[QUIET[0], QUIET[1]])
    assert pair(a, b, False).features["folder_shared"] > 0.5


def test_a_page_scanned_later_lists_its_folders_document_first():
    from lindley.assembler.place import DocEnds, candidates

    def doc(id, folder, texts):
        ps = _filed(folder, len(texts) + 1, texts=texts, first=10 * id)
        return DocEnds(id, folder, ps[0], ps[-1], False, frozenset(p.id for p in ps))

    will = doc(1, "D:/Scans/Will", [LETTER[0], QUIET[0]])
    mill = doc(2, "D:/Scans/Mill", [LETTER[0], QUIET[1]])
    (late,) = _filed("D:/Scans/Mill", 3, texts=[QUIET[0]], first=99)
    ranked = candidates(Group([late], 40), [will, mill])
    assert ranked[0].document.id == 2  # the other folder's document scores lower, if at all
    assert "Both are in the folder Mill" in ranked[0].reasons
    assert ranked[0].payload()["document"] == 2


def test_other_pages_in_the_inbox_can_be_candidates_but_never_a_documents_copy():
    from lindley.assembler.place import DocEnds, candidates

    a, b, c = _filed("D:/Scans/Will", 3, texts=[LETTER[0], LETTER[1], LETTER[2]])
    doc = DocEnds(9, "Letter", a, a, False, frozenset({a.id}))
    rest = Group([c], 40)
    copy = Page(5, 5, "Image (5).jpg", LETTER[1], copies=frozenset({a.id}))
    ranked = candidates(Group([b], 40), [doc], [rest])
    assert {(x.document or x.group) is not None for x in ranked} == {True}
    assert any(x.group is rest for x in ranked) and any(x.document is doc for x in ranked)
    assert all(x.document is not doc for x in candidates(Group([copy], 40), [doc], [rest]))


# Weighed so a page scanned later may well continue the letter, but not surely: the AI's band
UNSURE = {"bias": 0.0}


def _letter_and_a_later_page(conn):
    load(conn, pages(LETTER))
    assemble(conn)
    (late,) = load(conn, pages([QUIET[0]], "later"), start_seq=90)
    return conn.execute("SELECT id FROM documents").fetchone()[0], late


def test_the_ai_is_shown_only_the_likeliest_documents_and_its_choice_is_added(conn):
    doc, late = _letter_and_a_later_page(conn)
    asked = []

    def reply(req):
        asked.append(req)
        return json.dumps({"document": doc, "confidence": 90, "reasons": ["It reads on"]})

    report = assemble(conn, AI_ON, chat=Scripted(reply), weights=UNSURE)
    ((req,),) = [asked]
    assert [d["id"] for d in req["documents"]] == [doc] and req["pages"][0]["id"] == late
    assert report.ai_placed == 1 and report.pages_added == 1
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] == doc
    action, after = conn.execute("SELECT action, after FROM history ORDER BY id DESC").fetchone()
    assert action == "add_pages" and json.loads(after)["checked_by_ai"] is True
    assert json.loads(after)["reasons"] == ["It reads on"]


def test_the_ai_is_never_waited_for_while_the_database_is_held(conn):
    doc, late = _letter_and_a_later_page(conn)
    held = []

    def reply(req):
        held.append(conn.in_transaction)
        return json.dumps({"document": doc, "confidence": 90, "reasons": ["It reads on"]})

    report = assemble(conn, AI_ON, chat=Scripted(reply), weights=UNSURE)
    assert held == [False]
    assert (report.ai_calls, report.ai_reused, report.ai_placed) == (1, 0, 1)
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] == doc


def test_a_document_the_ai_wasnt_shown_is_turned_down(conn):
    _, late = _letter_and_a_later_page(conn)
    reply = json.dumps({"document": 999, "confidence": 90, "reasons": []})
    report = assemble(conn, AI_ON, chat=Scripted(reply), weights=UNSURE)
    assert report.ai_rejected == ["the reply chose a document it wasn't given"]
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] is None


def test_the_ais_choice_of_a_persons_document_is_only_suggested(conn):
    doc, late = _letter_and_a_later_page(conn)
    conn.execute("UPDATE documents SET name_source = 'user'")
    reply = json.dumps({"document": doc, "confidence": 90, "reasons": ["It reads on"]})
    report = assemble(conn, AI_ON, chat=Scripted(reply), weights=UNSURE)
    assert report.ai_placed == 0 and report.pages_added == 0
    (s,) = open_suggestions(conn, "add_to_document")
    assert (s["page_id"], s["document_id"], s["confidence"]) == (late, doc, 90)
    assert json.loads(s["reasons"]) == ["It reads on"]


def test_when_the_ai_may_not_be_asked_the_question_waits_with_its_documents(conn):
    doc, late = _letter_and_a_later_page(conn)
    report = assemble(conn, weights=UNSURE)
    assert report.ai_waiting == 1
    ((pages_, proposal),) = conn.execute("SELECT pages, proposal FROM needs_ai").fetchall()
    (q,) = json.loads(proposal)
    assert json.loads(pages_) == [late] and q["question"] == "place"
    assert [c["document"] for c in q["candidates"]] == [doc]
    # Meanwhile a person is asked, as before
    (s,) = open_suggestions(conn, "add_to_document")
    assert s["page_id"] == late


def test_an_answer_of_none_of_these_is_kept_and_not_paid_for_again(conn):
    _, late = _letter_and_a_later_page(conn)
    ai = Scripted(json.dumps({"document": None, "confidence": 80, "reasons": []}))
    first = assemble(conn, AI_ON, chat=ai, weights=UNSURE)
    again = assemble(conn, AI_ON, chat=ai, weights=UNSURE)
    assert ai.calls == 1 and first.ai_calls == 1 and again.ai_reused == 1
    # It said none of them, so it isn't suggested for the letter either
    assert not open_suggestions(conn, "add_to_document")
    assert conn.execute("SELECT document_id FROM pages WHERE id = ?", (late,)).fetchone()[0] is None
