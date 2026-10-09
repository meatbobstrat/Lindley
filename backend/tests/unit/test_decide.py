"""A person is asked before the AI: hints, a person's answers to them, and when the AI waits."""

import json

import pytest

from lindley import history
from lindley.assembler import assemble
from lindley.assembler.apply import attach
from lindley.assembler.bench import TruePage, load
from lindley.assembler.decide import accept, dismiss
from lindley.assembler.model import Group
from lindley.assembler.run import load_inbox
from lindley.config import AssemblerSettings
from lindley.db.database import connect, init_db

BODY = (
    "The road ran north over the summit and down the long grade toward the camp.\n"
    "There was water at the spring and grass enough for the horses that night."
)
# A heading, then a page with nothing to say it follows or doesn't: the rules aren't sure (53)
STORY = ["BY THE ROAD NORTH\n" + BODY, "Then the stage came in from Tonopah at noon.\n" + BODY]


class Answers:
    """A stand-in AI that puts the pages it's shown together as one document."""

    def __init__(self):
        self.calls = 0

    def chat(self, messages):
        self.calls += 1
        ids = [p["id"] for p in json.loads(messages[-1].content)["pages"]]
        return json.dumps(
            {
                "documents": [{"pages": ids, "name": "By the road north", "confidence": 90}],
                "unplaced": [],
            }
        )


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def story(conn):
    return list(load(conn, [TruePage(t, "story", i, "page") for i, t in enumerate(STORY)]))


def hints(conn, kind):
    return conn.execute(
        "SELECT * FROM suggestions WHERE status = 'open' AND kind = ?", (kind,)
    ).fetchall()


def age(conn, days):
    conn.execute(f"UPDATE pages SET created_at = datetime('now', '-{days} days')")
    conn.commit()


WAIT = AssemblerSettings(ask_ai_after_days=7)  # a person gets a week to answer first


def test_an_ai_that_may_run_on_its_own_is_asked_at_once(conn):
    story(conn)
    ai = Answers()
    assert assemble(conn, AssemblerSettings(), ai).documents_created == 1 and ai.calls >= 1


def test_with_a_wait_uncertain_pages_go_to_a_person_first(conn):
    a, b = story(conn)
    ai = Answers()
    report = assemble(conn, WAIT, ai)
    assert ai.calls == 0 and report.documents_created == 0  # just arrived: a person first
    [h] = hints(conn, "group_pages")
    # How sure it is the pages go together, whole document or not
    assert json.loads(h["payload"])["pages"] == [a, b] and h["confidence"] == 62
    assert any("letterhead" in r for r in json.loads(h["reasons"]))


def test_pages_left_unanswered_go_to_the_ai_on_their_own(conn):
    story(conn)
    age(conn, 8)
    ai = Answers()
    report = assemble(conn, WAIT, ai)
    assert ai.calls >= 1 and report.documents_created == 1


def test_pages_a_person_turned_down_arent_sent_to_the_ai_on_their_own(conn):
    story(conn)
    assemble(conn)
    dismiss(conn, hints(conn, "group_pages")[0]["id"])
    age(conn, 8)
    ai = Answers()
    assemble(conn, AssemblerSettings(), ai)
    assert ai.calls == 0
    assert not hints(conn, "group_pages")  # and the same hint isn't made again


def test_a_person_can_ask_the_ai_about_pages_at_once(conn):
    a, _ = story(conn)
    ai = Answers()
    report = assemble(conn, AssemblerSettings(), ai, asked={a})
    assert ai.calls == 1 and report.documents_created == 1


def test_accepting_a_grouping_makes_the_persons_document_and_undo_takes_it_back(conn):
    a, b = story(conn)
    assemble(conn)
    done = accept(conn, hints(conn, "group_pages")[0]["id"])
    doc = conn.execute("SELECT * FROM documents").fetchone()
    assert doc["id"] == done.document_id and doc["origin"] == "user"
    assert [tuple(r) for r in conn.execute("SELECT id, position FROM pages ORDER BY id")] == [
        (a, 1),
        (b, 2),
    ]
    assert not hints(conn, "group_pages")
    history.undo(conn, done.batch)
    assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM pages WHERE document_id IS NULL").fetchone()[0] == 2


def test_an_out_of_date_hint_is_refused(conn):
    a, _ = story(conn)
    assemble(conn)
    conn.execute("UPDATE pages SET set_aside_at = datetime('now') WHERE id = ?", (a,))
    conn.commit()
    with pytest.raises(ValueError):
        accept(conn, hints(conn, "group_pages")[0]["id"])
    with pytest.raises(LookupError):
        accept(conn, 999)


def _doc(conn, pages, base=1):
    d = conn.execute("INSERT INTO documents (name, origin) VALUES ('Mine', 'user')").lastrowid
    for i, pid in enumerate(pages, base):
        conn.execute("UPDATE pages SET document_id = ?, position = ? WHERE id = ?", (d, i, pid))
    return d


def _hint(conn, pid, doc, pages, at):
    payload = json.dumps({"pages": pages, "at": at})
    return conn.execute(
        "INSERT INTO suggestions (kind, page_id, document_id, payload, confidence)"
        " VALUES ('add_to_document', ?, ?, ?, 60)",
        (pid, doc, payload),
    ).lastrowid


# A document a person has changed counts from 0 (organise.close_gaps); one Lindley made, from 1
@pytest.mark.parametrize("base", [0, 1])
@pytest.mark.parametrize("at", ["end", "start"])
def test_accepting_an_addition_adds_every_page_hinted_and_undo_moves_them_back(conn, at, base):
    texts = [TruePage(f"Page {i} of something. " + BODY, "x", i, "page") for i in range(4)]
    old1, old2, new1, new2 = load(conn, texts)
    d = _doc(conn, [old1, old2], base)
    first = _hint(conn, new1, d, [new1, new2], at)
    _hint(conn, new2, d, [new1, new2], at)
    conn.commit()
    done = accept(conn, first)
    order = [
        r[0]
        for r in conn.execute("SELECT id FROM pages WHERE document_id = ? ORDER BY position", (d,))
    ]
    assert order == ([old1, old2, new1, new2] if at == "end" else [new1, new2, old1, old2])
    assert _no_two_share_a_place(conn, d)
    assert not hints(conn, "add_to_document")
    history.undo(conn, done.batch)
    assert [
        r[0]
        for r in conn.execute("SELECT id FROM pages WHERE document_id = ? ORDER BY position", (d,))
    ] == [old1, old2]


def _no_two_share_a_place(conn, doc):
    places = [
        r[0] for r in conn.execute("SELECT position FROM pages WHERE document_id = ?", (doc,))
    ]
    return len(places) == len(set(places))


@pytest.mark.parametrize("base", [0, 1])
def test_pages_lindley_adds_at_the_start_never_share_a_place(conn, base):
    texts = [TruePage(f"Page {i} of something. " + BODY, "x", i, "page") for i in range(4)]
    old1, old2, new1, new2 = load(conn, texts)
    d = _doc(conn, [old1, old2], base)
    inbox = {p.id: p for p in load_inbox(conn)}
    attach(conn, d, Group([inbox[new1], inbox[new2]], 90), False, 90, [])
    order = [
        r[0]
        for r in conn.execute("SELECT id FROM pages WHERE document_id = ? ORDER BY position", (d,))
    ]
    assert order == [new1, new2, old1, old2]
    assert _no_two_share_a_place(conn, d)
