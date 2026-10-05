import pytest

from lindley import history, organise
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


def test_undo_leaves_a_completed_document_alone(conn):
    d = doc(conn)
    first = page(conn, d, 0)
    loose = page(conn)
    moved = organise.move_pages(conn, [loose], "document", d)
    conn.execute("UPDATE documents SET status = 'complete'")
    conn.commit()
    with pytest.raises(history.UndoError, match="completed. Reopen it"):
        history.undo(conn, moved.batch)
    assert places(conn, first, loose) == [(d, 0, None), (d, 1, None)]


def test_a_copy_in_a_completed_document_is_kept_only_where_it_is(conn):
    d = doc(conn, status="complete")
    filed = page(conn, d, 0)
    rescan = page(conn, conf=95)
    dup(conn, filed, rescan)
    with pytest.raises(ValueError, match="completed. Reopen it"):
        keep(conn, open_sets(conn)[0].id, rescan)  # it would take the filed copy's place
    keep(conn, open_sets(conn)[0].id, filed)  # the re-scan is set aside; the document is as it was
    assert places(conn, filed)[0] == (d, 0, None) and places(conn, rescan)[0][2] is not None


def test_undo_brings_back_hints_whose_ids_were_taken_since(conn):
    a = doc(conn)
    x = page(conn, a, 0)
    conn.execute(
        "INSERT INTO suggestions (id, kind, page_id, document_id, confidence)"
        " VALUES (7, 'add_to_document', ?, ?, 60)",
        (x, a),
    )
    conn.commit()
    gone = organise.move_pages(conn, [x], "aside")  # the document goes, and its hint with it
    conn.execute(
        "INSERT INTO suggestions (id, kind, page_id, confidence) VALUES (7, 'set_aside', ?, 50)",
        (x,),
    )
    conn.commit()
    history.undo(conn, gone.batch)
    kinds = sorted(r[0] for r in conn.execute("SELECT kind FROM suggestions"))
    assert kinds == ["add_to_document", "set_aside"]
