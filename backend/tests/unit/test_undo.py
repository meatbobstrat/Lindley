import pytest

from lindley import history
from lindley.db.database import connect, init_db
from lindley.duplicates.resolve import keep, keep_document, not_duplicates, open_sets

from .test_resolve import doc, dup, page


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def places(conn, *pages):
    return [tuple(history.place(conn, p).values()) for p in pages]


def pair_states(conn):
    return [tuple(r) for r in conn.execute("SELECT status, kept_page FROM duplicates ORDER BY id")]


def test_undo_puts_a_kept_copy_and_its_twin_back(conn):
    d = doc(conn)
    first, old, last = page(conn, d, 0), page(conn, d, 1), page(conn, d, 2)
    better = page(conn, conf=95)
    dup(conn, old, better)
    before = places(conn, first, old, last, better)
    decision = keep(conn, open_sets(conn)[0].id, better)
    assert places(conn, old)[0][2] is not None  # set aside
    done = history.undo(conn, decision.batch)
    assert places(conn, first, old, last, better) == before
    assert pair_states(conn) == [("open", None)] and len(open_sets(conn)) == 1
    assert done.actions == ["keep_duplicate", "replace_with_duplicate", "set_aside_duplicate"]
    assert sorted(done.pages) == sorted([old, better])


def test_undo_brings_back_a_removed_document_and_renumbered_pages(conn):
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    x = page(conn, a, 0)
    y, extra = page(conn, b, 0), page(conn, b, 1)
    conn.execute(
        "INSERT INTO suggestions (kind, document_id, payload) VALUES ('name', ?, '{}')", (b,)
    )
    dup(conn, x, y)
    before = places(conn, x, y, extra)
    decision = keep_document(conn, a, b)
    assert places(conn, extra) == [(b, 0, None)]  # it moved up when its first page went
    history.undo(conn, decision.batch)
    assert places(conn, x, y, extra) == before


def test_undo_brings_back_an_emptied_document_with_its_suggestions(conn):
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    x, y = page(conn, a, 0), page(conn, b, 0)
    conn.execute(
        "INSERT INTO suggestions (kind, document_id, payload) VALUES ('name', ?, '{}')", (b,)
    )
    dup(conn, x, y)
    decision = keep_document(conn, a, b)
    assert conn.execute("SELECT COUNT(*) FROM documents WHERE id = ?", (b,)).fetchone()[0] == 0
    history.undo(conn, decision.batch)
    assert (
        conn.execute("SELECT name FROM documents WHERE id = ?", (b,)).fetchone()[0]
        == "Letter, again"
    )
    assert (
        conn.execute("SELECT kind FROM suggestions WHERE document_id = ?", (b,)).fetchone()[0]
        == "name"
    )
    assert places(conn, y) == [(b, 0, None)]


def test_undo_reopens_pages_that_were_not_duplicates(conn):
    a, b = page(conn), page(conn)
    dup(conn, a, b, kind="similar")
    decision = not_duplicates(conn, open_sets(conn)[0].id)
    assert open_sets(conn) == []
    history.undo(conn, decision.batch)
    assert pair_states(conn) == [("open", None)]


def test_undo_takes_back_the_latest_decision_first(conn):
    a, b, c, d = (page(conn) for _ in range(4))
    dup(conn, a, b)
    dup(conn, c, d)
    first = keep(conn, open_sets(conn)[0].id, a).batch
    second = keep(conn, open_sets(conn)[0].id, c).batch
    assert history.latest(conn) == second
    assert history.undo(conn).batch == second
    assert history.undo(conn).batch == first
    with pytest.raises(history.UndoError, match="nothing to undo"):
        history.undo(conn)
    with pytest.raises(history.UndoError, match="already been undone"):
        history.undo(conn, first)


def test_undo_refuses_if_a_page_moved_since_and_changes_nothing(conn):
    a, b = page(conn), page(conn)
    dup(conn, a, b)
    decision = keep(conn, open_sets(conn)[0].id, a)
    conn.execute("UPDATE pages SET set_aside_at = NULL WHERE id = ?", (b,))  # returned to Inbox
    conn.commit()
    with pytest.raises(history.UndoError, match="has moved since"):
        history.undo(conn, decision.batch)
    assert pair_states(conn) == [("resolved", a)]  # nothing was half undone
    assert history.latest(conn) == decision.batch  # still there to undo once it's put back
