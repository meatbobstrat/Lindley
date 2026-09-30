import json
import sqlite3

import pytest

from lindley.assembler import assemble
from lindley.assembler.ai import refine
from lindley.assembler.bench import TruePage, load, make_batch, score
from lindley.assembler.model import Group, Page
from lindley.config import AssemblerSettings
from lindley.db.database import connect, init_db

LETTER = [
    "Xenia, O., March 4 1892\nDear Sister,\nWe are all well and the river came up over the",
    "low road again last week.\nFather sold the bay mare at the sale.\n- 2 -",
    "- 3 -\nThe corn is in and the wheat looks better than it did last year.",
]
LAST = "- 4 -\nPlease write soon.\nYour loving brother\nWill"


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
    report = assemble(conn, chat=ai)
    assert report.ai_calls >= 1 and report.documents_created == 1
    doc = conn.execute("SELECT * FROM documents").fetchone()
    assert doc["name"] == "Farm notes, 1890s"
    assert json.loads(doc["reasons"]) == ["Both pages are about the farm's crops and prices"]
    assert [r[0] for r in conn.execute("SELECT id FROM pages ORDER BY position")] == [b, a]


def test_without_ai_the_same_pages_wait_in_the_inbox(conn):
    load(conn, pages(AMBIGUOUS))
    report = assemble(conn)
    assert report.ai_calls == 0 and report.documents_created == 0 and report.inbox_left == 2


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
    ],
    ids=["not json", "unknown id", "id twice", "page left out", "id not a number"],
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

    report = assemble(conn, chat=Broken())
    assert report.documents_created == 1


def test_ai_can_be_turned_off(conn):
    load(conn, pages(AMBIGUOUS))
    ai = Scripted("{}")
    assemble(conn, AssemblerSettings(use_ai=False), chat=ai)
    assert ai.calls == 0


# ---------------------------------------------------------------- Schema and bench


def test_v1_database_gains_documents_reasons(tmp_path):
    from importlib.resources import files

    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    v1 = "\n".join(ln for ln in schema.splitlines() if "reasons             TEXT" not in ln)
    db = tmp_path / "old.db"
    c = sqlite3.connect(db)
    c.executescript(v1 + "\nPRAGMA user_version = 1;")
    c.close()
    init_db(db)
    c = connect(db)
    assert "reasons" in {r["name"] for r in c.execute("PRAGMA table_info(documents)")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == 2
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
