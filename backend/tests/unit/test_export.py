import json

import pypdfium2 as pdfium
import pytest
from PIL import Image

from lindley import history
from lindley.db.database import connect, init_db
from lindley.db.progress import document_progress
from lindley.export import export_document, latest_export, reopen_document
from lindley.export.document import safe_name


@pytest.fixture
def conn(settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


def page(conn, tmp_path, doc, pos, text, conf=95.0, words=True, status="read"):
    n = conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0]
    path = tmp_path / f"scan_{n}.png"
    Image.new("L", (1275, 1650), 255).save(path, dpi=(150, 150))
    scan = conn.execute(
        "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode, status)"
        " VALUES (?, ?, ?, 'watched', 'copy', ?)",
        (f"h{n}", path.name, str(path), status),
    ).lastrowid
    pid = conn.execute(
        "INSERT INTO pages (scan_id, document_id, position, image_path, width_px, height_px,"
        " dpi, color_mode) VALUES (?, ?, ?, ?, 1275, 1650, 150, 'gray')",
        (scan, doc, pos, str(path)),
    ).lastrowid
    boxes = [
        {"text": t, "conf": conf, "bbox": [100 + i * 200, 200, 150, 40]}
        for i, t in enumerate(text.split())
    ]
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, confidence, words, is_current)"
        " VALUES (?, 'tesseract', ?, ?, ?, 1)",
        (pid, text, conf, json.dumps(boxes) if words else None),
    )
    conn.commit()
    return pid


def document(conn, name="Letter from Will", **cols):
    keys = ["name", *cols]
    doc = conn.execute(
        f"INSERT INTO documents ({', '.join(keys)}) VALUES ({', '.join('?' * len(keys))})",
        (name, *cols.values()),
    ).lastrowid
    conn.commit()
    return doc


def pdf_text(path):
    pdf = pdfium.PdfDocument(path)
    return [p.get_textpage().get_text_range() for p in pdf]


def test_a_document_becomes_one_pdf_of_its_pages_in_order(conn, settings, tmp_path):
    doc = document(conn, doc_type="letter", doc_date="1892-03")
    second = page(conn, tmp_path, doc, 2, "Your loving son")
    first = page(conn, tmp_path, doc, 1, "Dear Sister")
    e = export_document(conn, settings.library_dir, doc)
    assert e.pdf_path == (settings.library_dir / "Exports" / "Letter from Will.pdf").resolve()
    assert e.pages == [first, second]
    assert [t.split() for t in pdf_text(e.pdf_path)] == [
        ["Dear", "Sister"],
        ["Your", "loving", "son"],
    ]
    meta = pdfium.PdfDocument(e.pdf_path).get_metadata_dict()
    assert meta["Title"] == "Letter from Will" and meta["Subject"] == "letter, 1892-03"
    assert not e.to_review and not e.unplaced


def test_exporting_completes_the_document_and_is_recorded(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")
    e = export_document(conn, settings.library_dir, doc)
    assert conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0] == (
        "complete"
    )
    row = latest_export(conn, doc)
    assert row["pdf_path"] == str(e.pdf_path) and json.loads(row["page_ids"]) == [pid]
    checks = {c.key: c.done for c in document_progress(conn, doc, 90).checks}
    assert checks["exported"]
    log = conn.execute("SELECT action, batch FROM history WHERE target_id = ?", (doc,)).fetchone()
    assert tuple(log) == ("export", None)  # recorded, but not a decision for undo


def test_an_export_isnt_undone_by_undoing_the_grouping(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")
    batch = history.new_batch(conn)
    history.log(conn, batch, "group_pages", "new_document", doc, None, {"name": "x"})
    inbox = {"document_id": None, "position": None, "set_aside_at": None}
    history.log(conn, batch, "group_pages", "page", pid, inbox, history.place(conn, pid))
    conn.commit()
    export_document(conn, settings.library_dir, doc)
    assert history.latest(conn) == batch
    with pytest.raises(history.UndoError, match="exported"):
        history.undo(conn, batch)
    assert history.place(conn, pid)["document_id"] == doc


def test_pages_waiting_for_review_or_without_word_positions_are_reported(conn, settings, tmp_path):
    doc = document(conn)
    low = page(conn, tmp_path, doc, 1, "Dear Sistr", conf=60)
    no_boxes = page(conn, tmp_path, doc, 2, "Your son", words=False)
    e = export_document(conn, settings.library_dir, doc)
    assert e.to_review == [low]
    assert e.unplaced == [no_boxes]
    assert pdf_text(e.pdf_path)[1].split() == ["Your", "son"]


def test_a_persons_correction_is_what_the_pdf_says(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sistr")
    conn.execute("UPDATE transcriptions SET is_current = 0 WHERE page_id = ?", (pid,))
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, is_current)"
        " VALUES (?, 'user', 'Dear Sister', 1)",
        (pid,),
    )
    conn.commit()
    e = export_document(conn, settings.library_dir, doc)
    assert pdf_text(e.pdf_path)[0].split() == ["Dear", "Sister"]
    assert not e.unplaced


def test_a_document_still_being_read_or_without_pages_isnt_exported(conn, settings, tmp_path):
    empty = document(conn, "Empty")
    with pytest.raises(ValueError, match="no pages"):
        export_document(conn, settings.library_dir, empty)
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister", status="reading")
    with pytest.raises(ValueError, match="still being read"):
        export_document(conn, settings.library_dir, doc)
    with pytest.raises(LookupError):
        export_document(conn, settings.library_dir, 999)
    assert latest_export(conn, doc) is None
    assert conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0] == (
        "progress"
    )


def test_a_missing_scan_stops_the_export(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")
    conn.execute("UPDATE pages SET image_path = ? WHERE id = ?", (str(tmp_path / "gone.png"), pid))
    conn.commit()
    with pytest.raises(ValueError, match="missing"):
        export_document(conn, settings.library_dir, doc)


def test_exporting_again_replaces_the_pdf_and_a_rename_moves_it(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    first = export_document(conn, settings.library_dir, doc).pdf_path
    reopen_document(conn, doc)
    again = export_document(conn, settings.library_dir, doc).pdf_path
    assert again == first and first.is_file()
    conn.execute("UPDATE documents SET name = 'Will to Mary, 1892' WHERE id = ?", (doc,))
    conn.commit()
    renamed = export_document(conn, settings.library_dir, doc).pdf_path
    assert renamed.name == "Will to Mary, 1892.pdf" and renamed.is_file()
    assert not first.exists()


def test_documents_with_the_same_name_get_numbered_pdfs(conn, settings, tmp_path):
    a, b = document(conn, "Receipt"), document(conn, "Receipt")
    page(conn, tmp_path, a, 1, "Paid")
    page(conn, tmp_path, b, 1, "Paid again")
    pa = export_document(conn, settings.library_dir, a).pdf_path
    pb = export_document(conn, settings.library_dir, b).pdf_path
    assert (pa.name, pb.name) == ("Receipt.pdf", "Receipt (2).pdf")
    # renaming b away and exporting it again doesn't touch a's PDF
    conn.execute("UPDATE documents SET name = 'Receipt for seed' WHERE id = ?", (b,))
    conn.commit()
    export_document(conn, settings.library_dir, b)
    assert pa.is_file() and not pb.exists()


def test_a_file_already_in_exports_is_never_overwritten(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    folder = settings.library_dir / "Exports"
    folder.mkdir(parents=True)
    (folder / "Letter from Will.pdf").write_bytes(b"someone else's")
    e = export_document(conn, settings.library_dir, doc)
    assert e.pdf_path.name == "Letter from Will (2).pdf"
    assert (folder / "Letter from Will.pdf").read_bytes() == b"someone else's"


def test_reopening_keeps_the_pdf(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    e = export_document(conn, settings.library_dir, doc)
    reopen_document(conn, doc)
    assert conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0] == (
        "progress"
    )
    assert e.pdf_path.is_file()
    actions = [r[0] for r in conn.execute("SELECT action FROM history ORDER BY id")]
    assert actions == ["export", "reopen"]


@pytest.mark.parametrize(
    ("name", "stem"),
    [
        ("Letter: Will to Mary?", "Letter Will to Mary"),
        ('"Deed" 12/3/1892', "Deed 12 3 1892"),
        ("CON", "CON (document)"),
        ("Notes...", "Notes"),
        ("x" * 200, "x" * 120),
    ],
)
def test_names_become_safe_file_names(name, stem):
    assert safe_name(name) == stem


def test_the_api_exports_downloads_and_reopens(client, settings, tmp_path):
    c = connect(settings.db_path)
    doc = document(c)
    pid = page(c, tmp_path, doc, 1, "Dear Sister", conf=60)
    c.close()
    assert client.get(f"/api/documents/{doc}/export").status_code == 404
    r = client.post(f"/api/documents/{doc}/export")
    assert r.status_code == 200
    body = r.json()
    assert body["file_name"] == "Letter from Will.pdf"
    assert body["pages"] == [pid] and body["to_review"] == [pid]
    pdf = client.get(f"/api/documents/{doc}/export")
    assert pdf.status_code == 200 and pdf.headers["content-type"] == "application/pdf"
    assert pdf.content.startswith(b"%PDF")
    assert client.post(f"/api/documents/{doc}/reopen").json()["status"] == "progress"


def test_the_api_says_why_it_cant_export(client, settings):
    c = connect(settings.db_path)
    doc = document(c, "Empty")
    c.close()
    assert client.post("/api/documents/999/export").status_code == 404
    r = client.post(f"/api/documents/{doc}/export")
    assert r.status_code == 409 and "no pages" in r.json()["detail"]
    assert client.post("/api/documents/999/reopen").status_code == 404
