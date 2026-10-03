"""Learning the evidence weights from documents people vouched for, only when it does better."""

import json

import pytest

from lindley.assembler import assemble, evidence, relearn
from lindley.assembler.bench import TruePage, load
from lindley.assembler.weights import WEIGHTS
from lindley.db.database import connect, init_db

from .test_assembler import LAST, LETTER


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()
    evidence.use_weights(None)


def document(conn, origin="user", status="progress", n=2):
    ids = list(
        load(
            conn,
            [TruePage(t, "x", i, "letter") for i, t in enumerate((LETTER + [LAST])[:n])],
            start_seq=1 + 10 * conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0],
        )
    )
    d = conn.execute(
        "INSERT INTO documents (name, origin, status) VALUES ('Letter', ?, ?)", (origin, status)
    ).lastrowid
    for i, pid in enumerate(ids, 1):
        conn.execute("UPDATE pages SET document_id = ?, position = ? WHERE id = ?", (d, i, pid))
    conn.commit()
    return ids


def test_the_answer_key_is_what_people_vouched_for(conn):
    mine = document(conn, origin="user")
    done = document(conn, origin="lindley", status="complete")
    document(conn, origin="lindley")  # Lindley's own, untouched: not an answer
    document(conn, origin="user", n=1)  # one page says nothing about pages going together
    assert relearn.answer_key(conn) == [mine, done]


@pytest.fixture
def quick(monkeypatch):
    """No made-up batches, and at least two answer documents."""
    monkeypatch.setattr(relearn, "_made_up", lambda: ())
    monkeypatch.setattr(relearn, "MIN_DOCUMENTS", 2)


def test_too_few_answers_and_nothing_is_tried(conn, quick):
    document(conn)
    assert relearn.relearn(conn) is None
    assert not conn.execute("SELECT 1 FROM learned_weights").fetchone()


def test_weights_that_rebuild_better_are_adopted_and_used(conn, quick, monkeypatch):
    for _ in range(4):
        document(conn, n=3)
    shipped = dict(WEIGHTS)
    monkeypatch.setattr(relearn, "rebuild", lambda c, docs, w: (1, 0) if w == shipped else (2, 0))
    r = relearn.relearn(conn)
    assert r.adopted and r.documents == 4
    learned = relearn.learned(conn)
    assert learned and set(learned) == set(WEIGHTS)
    assemble(conn)
    assert evidence._active == {**WEIGHTS, **learned}
    assert relearn.relearn(conn) is None  # not again until there are more answers


def test_weights_that_rebuild_worse_are_kept_but_not_used(conn, quick, monkeypatch):
    for _ in range(4):
        document(conn, n=3)
    shipped = dict(WEIGHTS)
    monkeypatch.setattr(relearn, "rebuild", lambda c, docs, w: (2, 0) if w == shipped else (2, 1))
    r = relearn.relearn(conn)
    assert not r.adopted
    assert relearn.learned(conn) is None
    row = conn.execute("SELECT adopted, report FROM learned_weights").fetchone()
    assert row[0] == 0 and json.loads(row[1])["folds"][0]["after"]["wrong"] == 1


def test_the_rebuild_scores_documents_fed_in_as_loose_scans(conn):
    docs = [document(conn, n=4), document(conn, n=4)]
    exact, wrong = relearn.rebuild(conn, docs, dict(WEIGHTS))
    # The two letters are word for word the same, so a swapped feed may mix them up
    assert len(relearn.TRIES) <= exact <= 2 * len(relearn.TRIES) and wrong == 0
