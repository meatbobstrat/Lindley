import json

import pytest

from lindley.db.database import connect, init_db
from lindley.duplicates.resolve import (
    document_pairs,
    duplicate_of,
    get_set,
    keep,
    keep_document,
    not_duplicates,
    open_sets,
)


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def page(conn, doc=None, pos=None, conf=80.0, dpi=300, size=(2400, 3300), color="rgb", aside=False):
    n = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status,"
        " imported_at) VALUES (?, ?, ?, 'watched', 'copy', 'read', ?)",
        (f"h{n}", f"scan_{n}.jpg", f"D:/scans/scan_{n}.jpg", f"2026-10-01 10:{n:02d}:00"),
    ).lastrowid
    pid = conn.execute(
        "INSERT INTO pages (scan_id, document_id, position, dpi, width_px, height_px, color_mode,"
        " set_aside_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (scan, doc, pos, dpi, *size, color, "2026-10-01" if aside else None),
    ).lastrowid
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
        " VALUES (?, 'tesseract', 'text', ?, 1)",
        (pid, conf),
    )
    return pid


def doc(conn, name="Letter from Will", **cols):
    keys = ["name", *cols]
    return conn.execute(
        f"INSERT INTO documents ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
        (name, *cols.values()),
    ).lastrowid


def dup(conn, a, b, kind="same_page", score=85):
    a, b = min(a, b), max(a, b)
    conn.execute(
        "INSERT INTO duplicates (page_a, page_b, kind, score, evidence) VALUES (?, ?, ?, ?, ?)",
        (a, b, kind, score, json.dumps({"reasons": ["85% of the words match, in the same order"]})),
    )
    conn.commit()


def where(conn, pid):
    return tuple(
        conn.execute(
            "SELECT document_id, position, set_aside_at IS NOT NULL FROM pages WHERE id = ?",
            (pid,),
        ).fetchone()
    )


def test_pairs_sharing_a_page_make_one_set(conn):
    a, b, c, d, e = (page(conn) for _ in range(5))
    dup(conn, a, b)
    dup(conn, b, c)
    dup(conn, d, e, kind="similar", score=45)
    sets = open_sets(conn)
    assert [sorted(x.page_id for x in s.copies) for s in sets] == [[a, b, c], [d, e]]
    assert [s.kind for s in sets] == ["same_page", "similar"]
    assert sets[0].reasons == ["85% of the words match, in the same order"]


def test_a_similar_pair_never_joins_a_set_of_copies(conn):
    # Two scans of a sheet, and a slip from it scanned on its own: keeping one scan of the
    # sheet must not set the slip aside.
    sheet, rescan, slip = page(conn), page(conn), page(conn)
    dup(conn, sheet, rescan)
    dup(conn, sheet, slip, kind="similar", score=40)
    sets = open_sets(conn)
    assert [(s.kind, sorted(c.page_id for c in s.copies)) for s in sets] == [
        ("same_page", [sheet, rescan]),
        ("similar", [sheet, slip]),
    ]
    assert keep(conn, sets[0].id, sheet).set_aside == [rescan]
    assert where(conn, slip) == (None, None, 0)
    assert [s.kind for s in open_sets(conn)] == ["similar"]


def test_a_set_whose_other_copies_were_set_aside_is_decided_until_one_comes_back(conn):
    a, b, c = page(conn), page(conn, aside=True), page(conn, aside=True)
    dup(conn, a, b)
    dup(conn, b, c)
    assert open_sets(conn) == []
    conn.execute("UPDATE pages SET set_aside_at = NULL WHERE id = ?", (c,))
    assert [sorted(x.page_id for x in s.copies) for s in open_sets(conn)] == [[a, b, c]]


def test_lindley_suggests_the_better_copy_and_says_why(conn):
    d = doc(conn)
    in_doc = page(conn, d, 0, conf=78)
    sharper = page(conn, conf=90, dpi=600, size=(4800, 6600))
    dup(conn, in_doc, sharper)
    s = open_sets(conn)[0]
    assert s.suggested == sharper
    assert s.why == [
        "Read with 90% confidence, against 78%",
        "Sharper scan: 600 dpi, against 300",
        "Keeping it puts it in its place in “Letter from Will”",
    ]


def test_with_copies_as_good_as_each_other_the_one_in_a_document_wins(conn):
    d = doc(conn)
    loose, in_doc = page(conn, conf=80), page(conn, d, 0, conf=81)
    dup(conn, loose, in_doc)
    s = open_sets(conn)[0]
    assert s.suggested == in_doc and s.why == ["It's already in “Letter from Will”"]


def test_keeping_the_inbox_copy_puts_it_in_the_documents_place(conn):
    d = doc(conn)
    first, old = page(conn, d, 0), page(conn, d, 1)
    last = page(conn, d, 2)
    better = page(conn, conf=95)
    dup(conn, old, better)
    set_id = open_sets(conn)[0].id
    assert keep(conn, set_id, better).set_aside == [old]
    assert where(conn, better) == (d, 1, 0)
    assert where(conn, old) == (None, None, 1)
    assert [where(conn, p)[1] for p in (first, last)] == [0, 2]
    assert duplicate_of(conn, old) == better
    assert open_sets(conn) == []
    row = conn.execute("SELECT status, kept_page FROM duplicates").fetchone()
    assert tuple(row) == ("resolved", better)
    actions = [r[0] for r in conn.execute("SELECT action FROM history ORDER BY id")]
    assert actions == ["set_aside_duplicate", "replace_with_duplicate", "keep_duplicate"]
    before = json.loads(
        conn.execute("SELECT before FROM history WHERE action = 'set_aside_duplicate'").fetchone()[
            0
        ]
    )
    assert before == {"document_id": d, "position": 1, "set_aside_at": None}  # enough to undo


def test_keeping_the_document_copy_closes_the_gap_in_the_other_document(conn):
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    keepers = [page(conn, a, i) for i in range(2)]
    others = [page(conn, b, i) for i in range(3)]  # the second scan has an extra page
    dup(conn, keepers[0], others[0])
    keep(conn, open_sets(conn)[0].id, keepers[0])
    assert [where(conn, p)[1] for p in others[1:]] == [0, 1]


def test_a_document_scanned_twice(conn):
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    first = [page(conn, a, i) for i in range(3)]
    again = [page(conn, b, i) for i in range(3)]
    for x, y in zip(first, again, strict=True):
        dup(conn, x, y)
    [dp] = document_pairs(open_sets(conn), conn)
    assert dp.documents == (a, b) and len(dp.set_ids) == 3 and dp.extra == {a: [], b: []}
    assert keep_document(conn, a, b).sets == 3
    assert all(where(conn, p)[2] for p in again) and [where(conn, p)[0] for p in first] == [a] * 3
    assert conn.execute("SELECT COUNT(*) FROM documents WHERE id = ?", (b,)).fetchone()[0] == 0
    removed = conn.execute(
        "SELECT before FROM history WHERE action = 'remove_empty_document'"
    ).fetchone()[0]
    assert json.loads(removed)["document"]["name"] == "Letter, again"


def test_extra_pages_of_the_other_document_stay(conn):
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    x, y = page(conn, a, 0), page(conn, b, 0)
    extra = page(conn, b, 1)
    dup(conn, x, y)
    [dp] = document_pairs(open_sets(conn), conn)
    assert dp.extra == {a: [], b: [extra]}
    keep_document(conn, a, b)
    assert where(conn, extra) == (b, 0, 0)


def test_a_page_only_like_one_of_the_others_stays_too(conn):
    """A3 and B3 are copies; A3 and B9 only look alike, and B9 is a draft only B has."""
    a, b = doc(conn, "Letter"), doc(conn, "Letter, again")
    x, y = page(conn, a, 0), page(conn, b, 0)
    draft = page(conn, b, 1)
    dup(conn, x, y)
    dup(conn, x, draft, kind="similar", score=50)
    decision = keep_document(conn, a, b)
    assert decision.sets == 1 and decision.set_aside == [y]
    assert where(conn, draft) == (b, 0, 0)
    assert where(conn, x) == (a, 0, 0)


def test_keep_all_when_they_are_not_copies(conn):
    a, b = page(conn), page(conn)
    dup(conn, a, b, kind="similar", score=45)
    not_duplicates(conn, open_sets(conn)[0].id)
    assert open_sets(conn) == [] and where(conn, a) == where(conn, b) == (None, None, 0)
    assert conn.execute("SELECT status FROM duplicates").fetchone()[0] == "not_duplicate"


def test_a_set_must_be_open_and_the_page_one_of_its_copies(conn):
    a, b, c = page(conn), page(conn), page(conn)
    dup(conn, a, b)
    s = get_set(conn, open_sets(conn)[0].id)
    with pytest.raises(ValueError):
        keep(conn, s.id, c)
    keep(conn, s.id, a)
    with pytest.raises(LookupError):
        keep(conn, s.id, a)


def test_a_document_name_with_quotes_is_not_quoted_again(conn):
    d = doc(conn, "Pages starting “SKETCH…”")
    a, b = page(conn, d, 0), page(conn)
    dup(conn, a, b)
    assert open_sets(conn)[0].why[0] == "It's already in Pages starting “SKETCH…”"
