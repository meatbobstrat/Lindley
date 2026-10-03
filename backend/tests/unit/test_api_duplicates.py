import io
import json

from PIL import Image

from lindley.api.duplicates import differences
from lindley.db.database import connect


def add_page(conn, tmp_path, text, doc=None, pos=None, size=(1200, 1600), rotation=0, exif=1):
    n = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    path = tmp_path / f"scan_{n}.jpg"
    img = Image.new("RGB", size, "white")
    ex = img.getexif()
    ex[0x0112] = exif
    img.save(path, exif=ex)
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES (?, ?, ?, 'watched', 'copy', 'read')",
        (f"h{n}", path.name, str(path)),
    ).lastrowid
    pid = conn.execute(
        "INSERT INTO pages (scan_id, image_path, document_id, position, width_px, height_px, dpi,"
        " color_mode, detected_rotation) VALUES (?, ?, ?, ?, ?, ?, 300, 'rgb', ?)",
        (scan, str(path), doc, pos, *size, rotation),
    ).lastrowid
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
        " VALUES (?, 'tesseract', ?, 85, 1)",
        (pid, text),
    )
    conn.commit()
    return pid


def pair(conn, a, b, kind="same_page"):
    conn.execute(
        "INSERT INTO duplicates (page_a, page_b, kind, score, evidence) VALUES (?, ?, ?, 80, ?)",
        (a, b, kind, json.dumps({"reasons": ["80% of the words match, in the same order"]})),
    )
    conn.commit()


def db(client, settings):
    client.get("/api/health")  # the app has set up the database
    return connect(settings.db_path)


def test_the_queue_lists_sets_with_where_each_copy_is(client, settings, tmp_path):
    conn = db(client, settings)
    d = conn.execute("INSERT INTO documents (name) VALUES ('Letter from Will')").lastrowid
    a = add_page(conn, tmp_path, "Dear Sister, the river came up.", d, 0)
    b = add_page(conn, tmp_path, "Dcar Sister, the rivcr came up.")
    pair(conn, a, b)
    body = client.get("/api/duplicates").json()
    assert body["count"] == 1 and body["documents"] == []
    [s] = body["sets"]
    assert s["kind"] == "same_page" and s["suggested"] == a
    assert s["why"] == ["It's already in “Letter from Will”"]
    where = {c["page_id"]: (c["where"], c["document_name"]) for c in s["copies"]}
    assert where == {a: ("document", "Letter from Will"), b: ("inbox", None)}
    assert s["copies"][0]["excerpt"].startswith("Dear Sister") and "text" not in s["copies"][0]
    assert s["copies"][0]["image"] == f"/api/pages/{a}/image"


def test_a_set_in_full_marks_the_differences(client, settings, tmp_path):
    conn = db(client, settings)
    a = add_page(conn, tmp_path, "Dear Sister, the river came up.")
    b = add_page(conn, tmp_path, "Dear Sister, the rivcr came up.")
    pair(conn, a, b)
    set_id = client.get("/api/duplicates").json()["sets"][0]["id"]
    s = client.get(f"/api/duplicates/{set_id}").json()
    by_page = {c["page_id"]: c for c in s["copies"]}
    assert by_page[b]["segments"] == [
        ["Dear Sister, the ", False],
        ["rivcr ", True],
        ["came up.", False],
    ]
    assert by_page[a]["text"] == "Dear Sister, the river came up."
    assert client.get("/api/duplicates/999").status_code == 404


def test_keep_sets_the_other_copy_aside(client, settings, tmp_path):
    conn = db(client, settings)
    a, b = add_page(conn, tmp_path, "one"), add_page(conn, tmp_path, "one")
    pair(conn, a, b)
    set_id = client.get("/api/duplicates").json()["sets"][0]["id"]
    assert client.post(f"/api/duplicates/{set_id}/keep", json={"page_id": 999}).status_code == 400
    r = client.post(f"/api/duplicates/{set_id}/keep", json={"page_id": b})
    assert {k: v for k, v in r.json().items() if k != "undo"} == {"kept": b, "set_aside": [a]}
    assert client.get("/api/duplicates").json()["count"] == 0
    assert client.post(f"/api/duplicates/{set_id}/keep", json={"page_id": b}).status_code == 404


def test_keep_all_when_they_are_different(client, settings, tmp_path):
    conn = db(client, settings)
    a, b = add_page(conn, tmp_path, "draft"), add_page(conn, tmp_path, "final")
    pair(conn, a, b, kind="similar")
    set_id = client.get("/api/duplicates").json()["sets"][0]["id"]
    assert client.post(f"/api/duplicates/{set_id}/not-duplicates").json()["ok"] is True
    assert client.get("/api/duplicates").json()["count"] == 0


def test_a_document_scanned_twice(client, settings, tmp_path):
    conn = db(client, settings)
    d1 = conn.execute("INSERT INTO documents (name) VALUES ('Letter')").lastrowid
    d2 = conn.execute("INSERT INTO documents (name) VALUES ('Letter, again')").lastrowid
    first = [add_page(conn, tmp_path, f"page {i}", d1, i) for i in range(2)]
    again = [add_page(conn, tmp_path, f"page {i}", d2, i) for i in range(2)]
    for x, y in zip(first, again, strict=True):
        pair(conn, x, y)
    [dp] = client.get("/api/duplicates").json()["documents"]
    assert dp["documents"] == [d1, d2] and dp["names"] == ["Letter", "Letter, again"]
    r = client.post("/api/duplicates/keep-document", json={"keep": d1, "other": d2})
    assert r.json()["sets"] == 2 and sorted(r.json()["set_aside"]) == again
    assert (
        client.post("/api/duplicates/keep-document", json={"keep": d1, "other": d2}).status_code
        == 404
    )


def test_page_images_come_upright_and_reduced(client, settings, tmp_path):
    conn = db(client, settings)
    sideways = add_page(conn, tmp_path, "x", size=(1200, 1600), rotation=90)
    phone = add_page(conn, tmp_path, "y", size=(1600, 1200), exif=6)  # EXIF: turn 90° to view
    r = client.get(f"/api/pages/{sideways}/image?max_side=800")
    assert r.headers["content-type"] == "image/jpeg"
    assert Image.open(io.BytesIO(r.content)).size == (800, 600)
    r = client.get(f"/api/pages/{phone}/image?max_side=800")
    assert Image.open(io.BytesIO(r.content)).size == (600, 800)
    assert client.get("/api/pages/999/image").status_code == 404


def test_differences_keep_spacing_and_ignore_punctuation():
    assert differences("Dear  Sister,\nWe", "dear sister we") == [["Dear  Sister,\nWe", False]]
    assert differences("a b c", "a c") == [["a ", False], ["b ", True], ["c", False]]


def test_a_decision_can_be_undone_through_the_api(client, settings, tmp_path):
    conn = db(client, settings)
    a, b = add_page(conn, tmp_path, "one"), add_page(conn, tmp_path, "one")
    pair(conn, a, b)
    assert client.post("/api/undo").status_code == 404  # nothing to undo yet
    set_id = client.get("/api/duplicates").json()["sets"][0]["id"]
    batch = client.post(f"/api/duplicates/{set_id}/keep", json={"page_id": b}).json()["undo"]
    assert client.get("/api/undo").json() == {
        "batch": batch,
        "actions": ["set_aside_duplicate", "keep_duplicate"],
    }
    r = client.post(f"/api/undo/{batch}")
    assert r.status_code == 200 and r.json()["pages"] == [a]
    assert client.get("/api/duplicates").json()["count"] == 1
    assert client.post(f"/api/undo/{batch}").status_code == 409  # already undone
    assert client.get("/api/undo").json()["batch"] is None
