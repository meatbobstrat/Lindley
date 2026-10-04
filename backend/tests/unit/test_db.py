import json
import re
import sqlite3

import pytest

from lindley.db.database import SCHEMA_VERSION, connect, init_db
from lindley.db.progress import document_progress


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "lindley.db"
    init_db(db)
    c = connect(db)
    yield c
    c.close()


def add_scan(conn, name="scan_0001.jpg", status="read"):
    return conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES (?, ?, ?, 'watched', 'copy', ?)",
        (name, name, f"D:/Scans/{name}", status),
    ).lastrowid


def add_page(conn, scan_id, doc_id=None, position=None):
    return conn.execute(
        "INSERT INTO pages (scan_id, document_id, position) VALUES (?, ?, ?)",
        (scan_id, doc_id, position),
    ).lastrowid


def add_text(conn, page_id, text, confidence, source="tesseract"):
    conn.execute("UPDATE transcriptions SET is_current = 0 WHERE page_id = ?", (page_id,))
    return conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
        " VALUES (?, ?, ?, ?, 1)",
        (page_id, source, text, confidence),
    ).lastrowid


def add_doc(conn, name="Letter", **cols):
    keys = ["name", *cols]
    return conn.execute(
        f"INSERT INTO documents ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
        (name, *cols.values()),
    ).lastrowid


def test_init_db_is_idempotent_and_versioned(tmp_path):
    db = tmp_path / "sub" / "lindley.db"
    init_db(db)
    init_db(db)
    c = connect(db)
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    c.close()
    assert {
        "scans",
        "pages",
        "transcriptions",
        "facts",
        "page_links",
        "documents",
        "folders",
        "suggestions",
        "exports",
        "intake_steps",
        "history",
    } <= tables


def test_empty_placeholder_schema_is_replaced(tmp_path):
    db = tmp_path / "lindley.db"
    c = sqlite3.connect(db)
    c.executescript(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT);"
        "CREATE TABLE pages (id INTEGER PRIMARY KEY, document_id INTEGER, text TEXT);"
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY, source_path TEXT);"
    )
    c.close()
    init_db(db)
    c = connect(db)
    cols = {r["name"] for r in c.execute("PRAGMA table_info(pages)")}
    c.close()
    assert "scan_id" in cols


def test_placeholder_with_data_is_left_alone(tmp_path):
    db = tmp_path / "lindley.db"
    c = sqlite3.connect(db)
    c.executescript(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT);"
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY, source_path TEXT);"
        "INSERT INTO jobs (source_path) VALUES ('x.jpg');"
    )
    c.close()
    with pytest.raises(RuntimeError, match="old 'jobs' table"):
        init_db(db)


def test_page_lives_in_one_place(conn):
    scan = add_scan(conn)
    doc = add_doc(conn)
    page = add_page(conn, scan, doc, 1)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE pages SET set_aside_at = datetime('now') WHERE id = ?", (page,))
    conn.execute("UPDATE pages SET document_id = NULL, position = NULL WHERE id = ?", (page,))
    loc = conn.execute("SELECT location FROM v_page_location WHERE page_id = ?", (page,))
    assert loc.fetchone()[0] == "inbox"


def test_documents_with_pages_cannot_be_deleted(conn):
    doc = add_doc(conn)
    add_page(conn, add_scan(conn), doc, 1)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM documents WHERE id = ?", (doc,))


def test_one_current_reading_and_search_uses_it(conn):
    page = add_page(conn, add_scan(conn))
    add_text(conn, page, "Dear John Bransen, written in 1892", 71)
    add_text(conn, page, "Dear John Branson, written in 1892", None, source="user")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO transcriptions (page_id, source, text, is_current)"
            " VALUES (?, 'tesseract', 'x', 1)",
            (page,),
        )

    def search(word):
        return conn.execute(
            "SELECT t.page_id FROM transcriptions_fts f JOIN transcriptions t ON t.id = f.rowid"
            " WHERE transcriptions_fts MATCH ? AND t.is_current = 1",
            (word,),
        ).fetchall()

    assert len(search("Branson")) == 1
    assert search("Bransen") == []  # the old reading is kept, but only as history
    one = lambda sql: conn.execute(sql, (page,)).fetchone()[0]  # noqa: E731
    assert one("SELECT COUNT(*) FROM transcriptions WHERE page_id = ?") == 2
    assert one("SELECT reviewed FROM v_current_text WHERE page_id = ?") == 1


def test_fact_belongs_to_exactly_one_owner(conn):
    page = add_page(conn, add_scan(conn))
    doc = add_doc(conn)
    sql = (
        "INSERT INTO facts (page_id, document_id, kind, value, source)"
        " VALUES (?, ?, 'date', 'x', 'ocr')"
    )
    conn.execute(sql, (page, None))
    conn.execute(sql, (None, doc))
    for owners in ((page, doc), (None, None)):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(sql, owners)


def test_page_links_are_stored_once_per_pair(conn):
    scan = add_scan(conn)
    a, b = add_page(conn, scan), add_page(conn, add_scan(conn, "scan_0002.jpg"))
    sql = (
        "INSERT INTO page_links (page_a, page_b, relation, score, source)"
        " VALUES (?, ?, 'adjacent_file', .9, 'names v1')"
    )
    conn.execute(sql, (a, b))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(sql, (b, a))  # always stored smallest id first
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(sql, (a, b))


def test_a_duplicate_pair_is_stored_once(conn):
    a, b = add_page(conn, add_scan(conn)), add_page(conn, add_scan(conn, "scan_0002.jpg"))
    sql = "INSERT INTO duplicates (page_a, page_b, kind, score) VALUES (?, ?, 'same_page', 90)"
    conn.execute(sql, (a, b))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(sql, (b, a))  # smallest id first
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(sql, (a, b))  # a decided pair is never raised again
    row = conn.execute("SELECT status, kept_page FROM duplicates").fetchone()
    assert tuple(row) == ("open", None)


def old_schema(*, duplicates: bool, batch: bool = False) -> str:
    """Today's schema as an older version had it: no ai_calls (v4), no history.batch (v3), no
    Duplicates (v2)."""
    from importlib.resources import files

    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    schema = schema.split("-- " + "-" * 64 + " AI calls")[0]
    if not batch:
        schema = re.sub(r"(after\s+TEXT),([^\n]*)\n\s+batch\s+INTEGER[^\n]*", r"\1 \2", schema)
        schema = re.sub(r"CREATE INDEX IF NOT EXISTS idx_history_batch[^\n]*\n", "", schema)
    if not duplicates:
        rule = "-- " + "-" * 64
        before, after = schema.split(rule + " Duplicates")
        schema = before + after[after.index(rule) :]
    return schema


def test_v2_database_gains_the_duplicate_tables(tmp_path):
    db = tmp_path / "v2.db"
    c = sqlite3.connect(db)
    c.executescript(old_schema(duplicates=False) + "\nPRAGMA user_version = 2;")
    assert "duplicates" not in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    c.close()
    init_db(db)
    c = connect(db)
    names = {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert {"duplicates", "duplicate_checks", "text_sketch"} <= names
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v3_database_gains_history_batches(tmp_path):
    db = tmp_path / "v3.db"
    c = sqlite3.connect(db)
    c.executescript(old_schema(duplicates=True) + "\nPRAGMA user_version = 3;")
    c.execute(
        "INSERT INTO history (actor, action, target_type) VALUES ('user', 'rename', 'document')"
    )
    c.commit()
    c.close()
    init_db(db)
    c = connect(db)
    assert "batch" in {r["name"] for r in c.execute("PRAGMA table_info(history)")}
    assert c.execute("SELECT action, batch FROM history").fetchone()[:] == ("rename", None)
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v4_database_gains_the_ai_call_record(tmp_path):
    db = tmp_path / "v4.db"
    c = sqlite3.connect(db)
    c.executescript(old_schema(duplicates=True, batch=True) + "\nPRAGMA user_version = 4;")
    assert "ai_calls" not in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    c.close()
    init_db(db)
    c = connect(db)
    assert "ai_calls" in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v5_database_gains_the_kept_ai_answers(tmp_path):
    from importlib.resources import files

    db = tmp_path / "v5.db"
    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    c = sqlite3.connect(db)
    c.executescript(schema.split("-- " + "-" * 64 + " AI answers")[0] + "PRAGMA user_version = 5;")
    assert "ai_answers" not in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    c.close()
    init_db(db)
    c = connect(db)
    assert "ai_answers" in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v6_database_gains_learned_weights(tmp_path):
    from importlib.resources import files

    db = tmp_path / "v6.db"
    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    c = sqlite3.connect(db)
    old = schema.split("-- " + "-" * 64 + " Learned weights")[0]
    c.executescript(old + "PRAGMA user_version = 6;")
    c.close()
    init_db(db)
    c = connect(db)
    assert "learned_weights" in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v7_database_gains_the_needs_ai_queue(tmp_path):
    from importlib.resources import files

    db = tmp_path / "v7.db"
    schema = files("lindley.db").joinpath("schema.sql").read_text(encoding="utf-8")
    c = sqlite3.connect(db)
    c.executescript(schema.split("-- " + "-" * 64 + " Needs AI")[0] + "PRAGMA user_version = 7;")
    c.close()
    init_db(db)
    c = connect(db)
    assert "needs_ai" in {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v8_database_gains_seen_files_and_the_page_step_index(tmp_path):
    db = tmp_path / "v8.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.executescript(
        "DROP TABLE seen_files; DROP INDEX idx_intake_steps_page; PRAGMA user_version = 8;"
    )
    c.close()
    init_db(db)
    c = connect(db)
    names = {r[0] for r in c.execute("SELECT name FROM sqlite_master")}
    assert {"seen_files", "idx_intake_steps_page"} <= names
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    c.close()


def test_v9_database_queues_pages_read_before_upside_down_was_tried(tmp_path):
    db = tmp_path / "v9.db"
    init_db(db)
    c = sqlite3.connect(db)
    c.execute("PRAGMA user_version = 9")

    def page(conf, rotated=0, turned=0, checked=None, source="tesseract"):
        sid = c.execute(
            "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
            " VALUES (?, 'a.jpg', 'D:/a.jpg', 'watched', 'copy', 'read')",
            (f"h{c.execute('SELECT count(*) FROM scans').fetchone()[0]}",),
        ).lastrowid
        pid = c.execute(
            "INSERT INTO pages (scan_id, detected_rotation, user_rotation) VALUES (?, ?, ?)",
            (sid, rotated, turned),
        ).lastrowid
        c.execute(
            "INSERT INTO transcriptions (page_id, source, text, confidence, confirmed_at,"
            " is_current) VALUES (?, ?, 'Suyuig', ?, ?, 1)",
            (pid, source, conf, checked),
        )
        return pid

    poor = page(27.0)
    page(40.0, rotated=180), page(40.0, turned=90), page(40.0, checked="2026-10-01")
    page(40.0, source="vision")
    c.commit()
    c.close()
    init_db(db)
    c = connect(db)
    queued = c.execute(
        "SELECT page_id, step, status FROM intake_steps WHERE status = 'queued'"
    ).fetchall()
    assert [tuple(r) for r in queued] == [(poor, "ocr", "queued")]
    assert c.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION == 10
    init_db(db)  # an upgraded database isn't queued again
    assert c.execute("SELECT count(*) FROM intake_steps").fetchone()[0] == 1
    c.close()


def test_suggestion_reasons_round_trip(conn):
    page = add_page(conn, add_scan(conn))
    doc = add_doc(conn)
    reasons = ["Same handwriting as page 2", "Mentions “Mary” and the auction in May"]
    sid = conn.execute(
        "INSERT INTO suggestions (kind, page_id, document_id, confidence, reasons)"
        " VALUES ('add_to_document', ?, ?, 88, ?)",
        (page, doc, json.dumps(reasons)),
    ).lastrowid
    row = conn.execute("SELECT reasons, status FROM suggestions WHERE id = ?", (sid,)).fetchone()
    assert json.loads(row["reasons"]) == reasons
    assert row["status"] == "open"


def test_document_progress(conn):
    doc = add_doc(conn, "Letter, March 1892")
    p1 = add_page(conn, add_scan(conn, "a.jpg"), doc, 1)
    p2 = add_page(conn, add_scan(conn, "b.jpg"), doc, 2)
    add_page(conn, add_scan(conn, "c.jpg", status="reading"), doc, 3)
    add_text(conn, p1, "Dear Sister,", 95)
    add_text(conn, p2, "Your loving brother", 72)

    prog = document_progress(conn, doc, review_below=90)
    state = {c.key: c.done for c in prog.checks}
    assert state == {
        "has_pages": True,
        "read": False,
        "reviewed": False,
        "named": False,
        "dated": False,
        "typed": False,
        "ordered": True,
        "exported": False,
    }
    assert prog.done == 2 and prog.total == 8 and not prog.ready

    # Raising the threshold flags more pages.
    def detail(t):
        return next(c.detail for c in document_progress(conn, doc, t).checks if c.key == "reviewed")

    assert detail(90) == "1 page to review"
    assert detail(99) == "2 pages to review"

    # A person confirms the doubtful page, the last page is read, and the name and date are set.
    conn.execute("UPDATE transcriptions SET confirmed_at = datetime('now') WHERE page_id = ?", [p2])
    conn.execute("UPDATE scans SET status = 'read' WHERE original_name = 'c.jpg'")
    p3 = conn.execute("SELECT id FROM pages WHERE position = 3").fetchone()[0]
    add_text(conn, p3, "Mary", 97)
    conn.execute(
        "UPDATE documents SET name_source = 'user', doc_date = '1892-03', doc_type = 'letter'"
        " WHERE id = ?",
        (doc,),
    )
    prog = document_progress(conn, doc, review_below=90)
    assert prog.ready and prog.done == 7

    conn.execute("INSERT INTO suggestions (kind, document_id) VALUES ('reorder', ?)", (doc,))
    assert not document_progress(conn, doc, 90).ready
