import io

from PIL import Image

from lindley.db.database import connect


def add_page(conn, tmp_path, text, doc=None, pos=None, conf=95, source="tesseract"):
    n = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    path = tmp_path / f"scan_{n}.jpg"
    Image.new("RGB", (600, 800), "white").save(path)
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES (?, ?, ?, 'watched', 'copy', 'read')",
        (f"h{n}", path.name, str(path)),
    ).lastrowid
    pid = conn.execute(
        "INSERT INTO pages (scan_id, image_path, document_id, position, width_px, height_px, dpi)"
        " VALUES (?, ?, ?, ?, 600, 800, 300)",
        (scan, str(path), doc, pos),
    ).lastrowid
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
        " VALUES (?, ?, ?, ?, 1)",
        (pid, source, text, conf),
    )
    conn.commit()
    return pid


def db(client, settings):
    client.get("/api/health")  # the app has set up the database
    return connect(settings.db_path)


def document(conn, name="Letter from Will", status="progress"):
    d = conn.execute("INSERT INTO documents (name, status) VALUES (?, ?)", (name, status)).lastrowid
    conn.commit()
    return d


def undo(client, body):
    r = client.post(f"/api/undo/{body['undo']}")
    assert r.status_code == 200, r.text


def test_the_overview_counts_each_place(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    add_page(conn, tmp_path, "Dear Sister,", d, 0)
    add_page(conn, tmp_path, "the river came up", d, 1, conf=60)
    add_page(conn, tmp_path, "Received of J. Branson")
    aside = add_page(conn, tmp_path, "")
    conn.execute("UPDATE pages SET set_aside_at = datetime('now') WHERE id = ?", (aside,))
    conn.commit()
    body = client.get("/api/overview").json()
    c = body["counts"]
    assert (c["inbox"], c["aside"], c["review"], c["duplicates"]) == (1, 1, 1, 0)
    [doc] = body["documents"]
    assert doc["name"] == "Letter from Will" and doc["suggested"] is True
    assert (doc["pages"], doc["to_review"], doc["ready"]) == (2, 1, False)


def test_inbox_pages_say_how_well_they_were_read(client, settings, tmp_path):
    conn = db(client, settings)
    good = add_page(conn, tmp_path, "Dear Mother,")
    poor = add_page(conn, tmp_path, "Dcar Mothcr,", conf=60)
    pages = {p["id"]: p for p in client.get("/api/inbox").json()["pages"]}
    assert pages[good]["state"] == "ok" and pages[poor]["state"] == "review"
    assert pages[good]["where"] == "inbox" and pages[good]["image"].startswith("/api/pages/")
    review = client.get("/api/review").json()
    assert review["count"] == 1 and review["groups"][0]["document"] is None


def test_a_reading_with_no_confidence_isnt_waiting_for_review(client, settings, tmp_path):
    conn = db(client, settings)
    blank = add_page(conn, tmp_path, "", conf=None)
    [p] = client.get("/api/inbox").json()["pages"]
    assert p["id"] == blank and p["state"] == "ok"
    assert client.get("/api/review").json()["count"] == 0


def test_only_pages_the_review_queue_has_are_marked_for_review(client, settings, tmp_path):
    conn = db(client, settings)
    going, done = document(conn), document(conn, "Receipt", status="complete")
    open_page = add_page(conn, tmp_path, "the rivcr", going, 0, conf=60)
    done_page = add_page(conn, tmp_path, "Rcceived", done, 0, conf=60)
    aside = add_page(conn, tmp_path, "Dcar", conf=60)
    conn.execute("UPDATE pages SET set_aside_at = datetime('now') WHERE id = ?", (aside,))
    conn.commit()
    [p] = client.get(f"/api/documents/{going}").json()["pages"]
    assert p["id"] == open_page and p["state"] == "review"
    # Completing a document is a person's word that it's done; set aside is out of the way
    [p] = client.get(f"/api/documents/{done}").json()["pages"]
    assert p["id"] == done_page and p["state"] == "ok"
    [p] = client.get("/api/aside").json()["pages"]
    assert p["id"] == aside and p["state"] == "ok"
    assert client.get("/api/review").json()["count"] == 1


def test_a_page_in_full(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    add_page(conn, tmp_path, "first", d, 0)
    pid = add_page(conn, tmp_path, "Dear Sister, the river came up.", d, 1)
    p = client.get(f"/api/pages/{pid}").json()
    assert p["text"] == "Dear Sister, the river came up."
    assert p["document"] == {
        "id": d,
        "name": "Letter from Will",
        "suggested": True,
        "status": "progress",
        "page_number": 2,
    }
    assert client.get("/api/pages/999").status_code == 404


def test_moving_pages_and_undoing_it(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    a = add_page(conn, tmp_path, "one", d, 0)
    b = add_page(conn, tmp_path, "two")
    body = client.post(
        "/api/pages/move", json={"page_ids": [b], "to": "document", "document_id": d}
    )
    assert body.status_code == 200, body.text
    assert [p["id"] for p in client.get(f"/api/documents/{d}").json()["pages"]] == [a, b]
    undo(client, body.json())
    assert client.get("/api/inbox").json()["pages"][0]["id"] == b


def test_setting_aside_the_last_page_removes_the_document_and_undo_brings_it_back(
    client, settings, tmp_path
):
    conn = db(client, settings)
    d = document(conn)
    a = add_page(conn, tmp_path, "one", d, 0)
    body = client.post("/api/pages/move", json={"page_ids": [a], "to": "aside"}).json()
    assert body["removed"] == [d]
    assert client.get(f"/api/documents/{d}").status_code == 404
    assert [p["id"] for p in client.get("/api/aside").json()["pages"]] == [a]
    undo(client, body)
    assert [p["id"] for p in client.get(f"/api/documents/{d}").json()["pages"]] == [a]


def test_a_completed_document_is_left_alone(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn, status="complete")
    a = add_page(conn, tmp_path, "one", d, 0)
    r = client.post("/api/pages/move", json={"page_ids": [a], "to": "inbox"})
    assert r.status_code == 409 and "Reopen" in r.json()["detail"]


def test_putting_pages_in_order(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    a, b, c = (add_page(conn, tmp_path, t, d, i) for i, t in enumerate("abc"))
    body = client.put(f"/api/documents/{d}/order", json={"page_ids": [c, a, b]}).json()
    assert [p["id"] for p in client.get(f"/api/documents/{d}").json()["pages"]] == [c, a, b]
    undo(client, body)
    assert [p["id"] for p in client.get(f"/api/documents/{d}").json()["pages"]] == [a, b, c]
    r = client.put(f"/api/documents/{d}/order", json={"page_ids": [a, b]})
    assert r.status_code == 409


def test_turning_pages(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "one")
    body = client.post("/api/pages/rotate", json={"page_ids": [a], "degrees": -90}).json()
    p = client.get(f"/api/pages/{a}").json()
    assert p["turned"] == 270 and p["image"].endswith("v=270")
    undo(client, body)
    assert client.get(f"/api/pages/{a}").json()["turned"] == 0


def test_an_image_asked_for_without_its_turn_is_checked_each_time(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "one")
    first = client.get(f"/api/pages/{a}/image?max_side=120")
    assert first.status_code == 200 and "no-cache" in first.headers["cache-control"]
    tag = {"if-none-match": first.headers["etag"]}
    assert client.get(f"/api/pages/{a}/image?max_side=120", headers=tag).status_code == 304
    client.post("/api/pages/rotate", json={"page_ids": [a], "degrees": 90})
    turned = client.get(f"/api/pages/{a}/image?max_side=120", headers=tag)
    assert turned.status_code == 200 and turned.headers["etag"] != tag["if-none-match"]
    # With its turn in the address, it may be kept a while
    kept = client.get(client.get(f"/api/pages/{a}").json()["image"])
    assert "max-age=300" in kept.headers["cache-control"]


def test_flipping_a_mirror_image(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "one")
    body = client.post("/api/pages/flip", json={"page_ids": [a]}).json()
    p = client.get(f"/api/pages/{a}").json()
    assert p["mirrored"] and not p["mirror_found"] and p["image"].endswith("v=0m")
    assert client.get(p["image"]).status_code == 200
    undo(client, body)
    assert not client.get(f"/api/pages/{a}").json()["mirrored"]
    # Flipping one Lindley found turns it back
    conn.execute("UPDATE pages SET detected_mirror = 1 WHERE id = ?", (a,))
    conn.commit()
    client.post("/api/pages/flip", json={"page_ids": [a]})
    p = client.get(f"/api/pages/{a}").json()
    assert not p["mirrored"] and p["mirror_found"]


def test_a_turned_page_waits_to_be_read_again(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "one")
    assert client.get("/api/overview").json()["counts"]["reading_again"] == 0
    body = client.post("/api/pages/rotate", json={"page_ids": [a], "degrees": 180}).json()
    assert client.get("/api/overview").json()["counts"]["reading_again"] == 1
    undo(client, body)
    assert client.get("/api/overview").json()["counts"]["reading_again"] == 0


def test_starting_a_document_and_naming_it(client, settings, tmp_path):
    conn = db(client, settings)
    a, b = add_page(conn, tmp_path, "one"), add_page(conn, tmp_path, "two")
    folder = client.post("/api/folders", json={"name": "Branson letters"}).json()["folder_id"]
    made = client.post(
        "/api/documents", json={"page_ids": [b, a], "name": "Letters", "folder_id": folder}
    ).json()
    d = client.get(f"/api/documents/{made['document_id']}").json()
    assert (d["name"], d["suggested"], d["origin"], d["folder_id"]) == (
        "Letters",
        False,
        "user",
        folder,
    )
    assert [p["id"] for p in d["pages"]] == [b, a]
    renamed = client.patch(
        f"/api/documents/{d['id']}", json={"name": "Letters from Will", "doc_date": "1892-03"}
    ).json()
    assert client.get(f"/api/documents/{d['id']}").json()["doc_date"] == "1892-03"
    undo(client, renamed)
    assert client.get(f"/api/documents/{d['id']}").json()["name"] == "Letters"
    undo(client, made)
    assert client.get(f"/api/documents/{d['id']}").status_code == 404
    assert {p["id"] for p in client.get("/api/inbox").json()["pages"]} == {a, b}


def test_a_change_that_changes_nothing_has_nothing_to_undo(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    a = add_page(conn, tmp_path, "one", d, 0)
    folder = client.post("/api/folders", json={"name": "Deeds"}).json()["folder_id"]
    client.patch(f"/api/documents/{d}", json={"name": "Letter from Will"})  # now the person's
    same = [
        client.post("/api/pages/move", json={"page_ids": [a], "to": "document", "document_id": d}),
        client.put(f"/api/documents/{d}/order", json={"page_ids": [a]}),
        client.patch(f"/api/documents/{d}", json={"name": "Letter from Will"}),
        client.patch(f"/api/folders/{folder}", json={"name": "Deeds"}),
    ]
    assert [r.json()["undo"] for r in same] == [None] * 4
    # So the next change's undo is its own
    moved = client.post("/api/pages/move", json={"page_ids": [a], "to": "aside"}).json()
    undo(client, moved)
    assert client.get("/api/folders").json()["folders"][0]["name"] == "Deeds"


def test_giving_the_type_alone_leaves_lindleys_date_as_lindleys(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    conn.execute("UPDATE documents SET doc_date = '1892', date_source = 'lindley'")
    conn.commit()
    client.patch(f"/api/documents/{d}", json={"doc_type": "Letter", "doc_date": "1892"})
    assert conn.execute("SELECT date_source FROM documents").fetchone()[0] == "lindley"
    client.patch(f"/api/documents/{d}", json={"doc_date": "1892-03"})
    assert conn.execute("SELECT date_source FROM documents").fetchone()[0] == "user"


def test_folders(client, settings, tmp_path):
    db(client, settings)
    made = client.post("/api/folders", json={"name": "Land records"}).json()
    sub = client.post("/api/folders", json={"parent_id": made["folder_id"]}).json()
    renamed = client.patch(f"/api/folders/{sub['folder_id']}", json={"name": "Deeds"}).json()
    names = {f["id"]: f["name"] for f in client.get("/api/folders").json()["folders"]}
    assert names == {made["folder_id"]: "Land records", sub["folder_id"]: "Deeds"}
    undo(client, renamed)
    undo(client, sub)
    assert len(client.get("/api/folders").json()["folders"]) == 1


def test_checking_and_correcting_text(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "Dcar Sister,", conf=60)
    fixed = client.put(f"/api/pages/{a}/text", json={"text": "Dear Sister,"}).json()
    p = client.get(f"/api/pages/{a}").json()
    assert (p["text"], p["state"], p["source"], p["readings"]) == (
        "Dear Sister,",
        "checked",
        "user",
        2,
    )
    undo(client, fixed)
    p = client.get(f"/api/pages/{a}").json()
    assert (p["text"], p["state"]) == ("Dcar Sister,", "review")
    checked = client.put(f"/api/pages/{a}/text", json={}).json()
    assert client.get(f"/api/pages/{a}").json()["state"] == "checked"
    undo(client, checked)
    assert client.get(f"/api/pages/{a}").json()["state"] == "review"


def test_a_page_waiting_for_the_ai_that_a_person_types_is_read(client, settings, tmp_path):
    conn = db(client, settings)
    pid = add_page(conn, tmp_path, "unused")
    # Read by the AI alone (ocr.engine "vision"): nothing yet, and the scan waits for it
    conn.execute("DELETE FROM transcriptions")
    conn.execute("UPDATE scans SET status = 'queued'")
    conn.commit()
    assert client.get(f"/api/pages/{pid}").json()["state"] == "reading"
    client.put(f"/api/pages/{pid}/text", json={"text": "Dear Sister"})
    assert client.get(f"/api/pages/{pid}").json()["state"] == "checked"
    assert conn.execute("SELECT status FROM scans").fetchone()[0] == "read"


def test_search_finds_words_on_the_reading_in_use(client, settings, tmp_path):
    conn = db(client, settings)
    d = document(conn)
    add_page(conn, tmp_path, "Nothing here", d, 0)
    a = add_page(conn, tmp_path, "Please tell John Branson his saddle is mended.", d, 1)
    client.put(f"/api/pages/{a}/text", json={"text": "Please tell John Branson it is mended."})
    body = client.get("/api/search", params={"q": "branson mend"}).json()
    [r] = body["results"]
    assert (r["page_id"], r["document_name"], r["page_number"]) == (a, "Letter from Will", 2)
    assert ["Branson", True] in r["snippet"] and ["mended", True] in r["snippet"]
    assert client.get("/api/search", params={"q": "saddle"}).json()["results"] == []
    assert client.get("/api/search", params={"q": '"("'}).json()["results"] == []


def test_one_scan_that_cant_be_added_doesnt_stop_the_others(client, settings, monkeypatch):
    db(client, settings)
    from lindley.api import scans

    real = scans.import_file

    def flaky(conn, s, path, origin):
        if path.name == "bad.png":
            raise RuntimeError("database is locked")
        return real(conn, s, path, origin=origin)

    monkeypatch.setattr(scans, "import_file", flaky)
    files = []
    for name, color in (("bad.png", "white"), ("good.png", "ivory")):
        buf = io.BytesIO()
        Image.new("RGB", (300, 400), color).save(buf, "PNG")
        files.append(("files", (name, buf.getvalue(), "image/png")))
    body = client.post("/api/scans", files=files).json()
    assert [(a["file"], a["status"]) for a in body["added"]] == [
        ("bad.png", "failed"),
        ("good.png", "new"),
    ]
    assert body["added"][0]["error"] == "database is locked"


def test_adding_scans_imports_them_into_the_inbox(client, settings):
    db(client, settings)
    buf = io.BytesIO()
    Image.new("RGB", (300, 400), "white").save(buf, "PNG")
    files = [("files", ("letter.png", buf.getvalue(), "image/png"))]
    body = client.post("/api/scans", files=files).json()
    assert body["added"] == [{"file": "letter.png", "status": "new", "pages": 1, "error": None}]
    [p] = client.get("/api/inbox").json()["pages"]
    assert (p["file"], p["origin"], p["state"]) == ("letter.png", "added", "reading")
    assert not any((settings.processing_dir / "added").iterdir())  # the upload is cleared away
    again = client.post("/api/scans", files=files).json()
    assert again["added"][0]["status"] == "duplicate"
