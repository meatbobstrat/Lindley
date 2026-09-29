from lindley.db.database import connect, init_db


def test_init_db_is_idempotent_and_fts_works(tmp_path):
    db = tmp_path / "sub" / "lindley.db"
    init_db(db)
    init_db(db)

    conn = connect(db)
    doc_id = conn.execute("INSERT INTO documents (title) VALUES ('Letter')").lastrowid
    conn.execute(
        "INSERT INTO pages (document_id, page_number, text) VALUES (?, 1, ?)",
        (doc_id, "Dear John Branson, written in 1892"),
    )
    hits = conn.execute("SELECT rowid FROM pages_fts WHERE pages_fts MATCH 'Branson'").fetchall()
    conn.close()
    assert len(hits) == 1
