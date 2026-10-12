import json
from pathlib import Path

import pypdfium2 as pdfium
import pytest
from PIL import Image

from lindley import history
from lindley.db.database import connect, init_db
from lindley.db.progress import document_progress
from lindley.export import complete_document, export_document, latest_export, reopen_document
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


def export(conn, settings, doc):
    """A person completes the document, then exports it."""
    complete_document(conn, doc)
    return export_document(conn, settings.output_dir, doc)


def pdf_text(path):
    pdf = pdfium.PdfDocument(path)
    return [p.get_textpage().get_text_range() for p in pdf]


def test_a_document_becomes_one_pdf_of_its_pages_in_order(conn, settings, tmp_path):
    doc = document(conn, doc_type="letter", doc_date="1892-03")
    second = page(conn, tmp_path, doc, 2, "Your loving son")
    first = page(conn, tmp_path, doc, 1, "Dear Sister")
    e = export(conn, settings, doc)
    assert e.pdf_path == (settings.output_dir / "Letter from Will.pdf").resolve() and not e.folder
    assert e.pages == [first, second]
    assert [t.split() for t in pdf_text(e.pdf_path)] == [
        ["Dear", "Sister"],
        ["Your", "loving", "son"],
    ]
    meta = pdfium.PdfDocument(e.pdf_path).get_metadata_dict()
    assert meta["Title"] == "Letter from Will" and meta["Subject"] == "letter, 1892-03"
    assert not e.to_review and not e.unplaced


def test_completing_then_exporting_is_recorded(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")

    def status():
        return conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0]

    with pytest.raises(ValueError, match="isn't completed yet"):
        export_document(conn, settings.output_dir, doc)
    complete_document(conn, doc)
    assert status() == "complete" and latest_export(conn, doc) is None
    checks = {c.key: c.done for c in document_progress(conn, doc, 90).checks}
    assert checks["completed"] and not checks["exported"]
    e = export_document(conn, settings.output_dir, doc)
    assert status() == "exported"
    row = latest_export(conn, doc)
    assert row["pdf_path"] == str(e.pdf_path) and json.loads(row["page_ids"]) == [pid]
    assert {c.key: c.done for c in document_progress(conn, doc, 90).checks}["exported"]
    with pytest.raises(ValueError, match="exported already"):
        export_document(conn, settings.output_dir, doc)
    log = conn.execute("SELECT action, batch FROM history WHERE target_id = ? ORDER BY id", (doc,))
    # recorded, but neither is a decision for undo
    assert [tuple(r) for r in log] == [("complete", None), ("export", None)]


def test_an_export_isnt_undone_by_undoing_the_grouping(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")
    batch = history.new_batch(conn)
    history.log(conn, batch, "group_pages", "new_document", doc, None, {"name": "x"})
    inbox = {"document_id": None, "position": None, "set_aside_at": None}
    history.log(conn, batch, "group_pages", "page", pid, inbox, history.place(conn, pid))
    conn.commit()
    export(conn, settings, doc)
    assert history.latest(conn) == batch
    with pytest.raises(history.UndoError, match="completed|exported"):
        history.undo(conn, batch)
    assert history.place(conn, pid)["document_id"] == doc


def test_pages_waiting_for_review_or_without_word_positions_are_reported(conn, settings, tmp_path):
    doc = document(conn)
    low = page(conn, tmp_path, doc, 1, "Dear Sistr", conf=60)
    no_boxes = page(conn, tmp_path, doc, 2, "Your son", words=False)
    e = export(conn, settings, doc)
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
    e = export(conn, settings, doc)
    assert pdf_text(e.pdf_path)[0].split() == ["Dear", "Sister"]
    assert not e.unplaced


def test_a_document_still_being_read_or_without_pages_isnt_exported(conn, settings, tmp_path):
    empty = document(conn, "Empty")
    with pytest.raises(ValueError, match="no pages"):
        export(conn, settings, empty)
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister", status="reading")
    with pytest.raises(ValueError, match="still being read"):
        export(conn, settings, doc)
    with pytest.raises(LookupError):
        export(conn, settings, 999)
    assert latest_export(conn, doc) is None
    assert conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0] == (
        "progress"
    )


def test_a_pdf_open_in_a_viewer_is_said_so(conn, settings, tmp_path, monkeypatch):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")

    def locked(*a):
        raise PermissionError(13, "The process cannot access the file")

    monkeypatch.setattr("lindley.export.document.os.replace", locked)
    with pytest.raises(ValueError, match="open in another program"):
        export(conn, settings, doc)
    assert not list(settings.output_dir.rglob("*.tmp"))
    assert conn.execute("SELECT status FROM documents").fetchone()[0] == "complete"


def test_a_missing_scan_stops_the_export(conn, settings, tmp_path):
    doc = document(conn)
    pid = page(conn, tmp_path, doc, 1, "Dear Sister")
    conn.execute("UPDATE pages SET image_path = ? WHERE id = ?", (str(tmp_path / "gone.png"), pid))
    conn.commit()
    with pytest.raises(ValueError, match="missing"):
        export(conn, settings, doc)


def test_exporting_again_replaces_the_pdf_and_a_rename_moves_it(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    first = export(conn, settings, doc).pdf_path
    reopen_document(conn, doc)
    again = export(conn, settings, doc).pdf_path
    assert again == first and first.is_file()
    reopen_document(conn, doc)
    conn.execute("UPDATE documents SET name = 'Will to Mary, 1892' WHERE id = ?", (doc,))
    conn.commit()
    renamed = export(conn, settings, doc).pdf_path
    assert renamed.name == "Will to Mary, 1892.pdf" and renamed.is_file()
    assert not first.exists()


def test_documents_with_the_same_name_get_numbered_pdfs(conn, settings, tmp_path):
    a, b = document(conn, "Receipt"), document(conn, "Receipt")
    page(conn, tmp_path, a, 1, "Paid")
    page(conn, tmp_path, b, 1, "Paid again")
    pa = export(conn, settings, a).pdf_path
    pb = export(conn, settings, b).pdf_path
    assert (pa.name, pb.name) == ("Receipt.pdf", "Receipt (2).pdf")
    # renaming b away and exporting it again doesn't touch a's PDF
    reopen_document(conn, b)
    conn.execute("UPDATE documents SET name = 'Receipt for seed' WHERE id = ?", (b,))
    conn.commit()
    export(conn, settings, b)
    assert pa.is_file() and not pb.exists()


def test_a_file_already_in_exports_is_never_overwritten(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    folder = settings.output_dir
    folder.mkdir(parents=True)
    (folder / "Letter from Will.pdf").write_bytes(b"someone else's")
    e = export(conn, settings, doc)
    assert e.pdf_path.name == "Letter from Will (2).pdf"
    assert (folder / "Letter from Will.pdf").read_bytes() == b"someone else's"


def test_reopening_keeps_the_pdf(conn, settings, tmp_path):
    doc = document(conn)
    page(conn, tmp_path, doc, 1, "Dear Sister")
    e = export(conn, settings, doc)
    reopen_document(conn, doc)
    assert conn.execute("SELECT status FROM documents WHERE id = ?", (doc,)).fetchone()[0] == (
        "progress"
    )
    assert e.pdf_path.is_file()
    actions = [r[0] for r in conn.execute("SELECT action FROM history ORDER BY id")]
    assert actions == ["complete", "export", "reopen"]


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
    assert client.post(f"/api/documents/{doc}/export").status_code == 409  # not completed
    done = client.post(f"/api/documents/{doc}/complete").json()
    assert done["status"] == "complete" and done["to_review"] == [pid]
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
    r = client.post(f"/api/documents/{doc}/complete")
    assert r.status_code == 409 and "no pages" in r.json()["detail"]
    assert client.post("/api/documents/999/reopen").status_code == 404


# Move mode: the originals Lindley holds go with the PDF, in a folder of the document's own


def taken(conn, settings, path, doc=None, positions=()):
    """Import a scan in move mode, read, its pages in `doc` at `positions` (the rest in the
    Inbox). Returns its page ids."""
    from lindley.worker.intake import import_file

    settings.move_files = True
    r = import_file(conn, settings, path)
    pids = [
        p
        for (p,) in conn.execute(
            "SELECT id FROM pages WHERE scan_id = ? ORDER BY page_index", (r.scan_id,)
        )
    ]
    for pid, pos in zip(pids, positions, strict=False):
        conn.execute("UPDATE pages SET document_id = ?, position = ? WHERE id = ?", (doc, pos, pid))
    for pid in pids:
        conn.execute(
            "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
            " VALUES (?, 'tesseract', 'Dear Sister', 95, 1)",
            (pid,),
        )
    conn.execute("UPDATE scans SET status = 'read' WHERE id = ?", (r.scan_id,))
    conn.commit()
    return pids


def scan_file(path, color="white", frames=1):
    imgs = [Image.new("L", (600, 800), color) for _ in range(frames)]
    imgs[0].save(path, save_all=frames > 1, append_images=imgs[1:], dpi=(150, 150))
    return path


def test_move_mode_gives_the_originals_back_beside_the_pdf(conn, settings, tmp_path):
    inbox = tmp_path / "drop"
    inbox.mkdir()
    a = scan_file(inbox / "scan 1.png", "white")
    b = scan_file(inbox / "scan 2.png", "ivory")
    before = {p.name: p.read_bytes() for p in (a, b)}
    doc = document(conn)
    taken(conn, settings, a, doc, [0])
    taken(conn, settings, b, doc, [1])
    assert not a.exists() and not b.exists()  # held by Lindley meanwhile
    e = export(conn, settings, doc)
    folder = (settings.output_dir / "Letter from Will").resolve()
    assert e.folder == folder and e.pdf_path == folder / "Letter from Will.pdf"
    assert {p.name: p.read_bytes() for p in e.originals} == before
    assert sorted(p.name for p in folder.iterdir()) == [
        "Letter from Will.pdf",
        "scan 1.png",
        "scan 2.png",
    ]
    rows = conn.execute("SELECT original_path, original_state FROM scans").fetchall()
    assert {r["original_state"] for r in rows} == {"exported"}
    assert {Path(r["original_path"]).parent for r in rows} == {folder}
    assert not list((settings.library_dir / "originals").rglob("*.png"))  # none left held
    assert json.loads(latest_export(conn, doc)["originals"]) == [str(p) for p in e.originals]


def test_an_original_with_pages_elsewhere_is_kept_until_the_person_decides(
    conn, settings, tmp_path
):
    inbox = tmp_path / "drop"
    inbox.mkdir()
    doc = document(conn)
    taken(
        conn, settings, scan_file(inbox / "bundle.tif", frames=2), doc, [0]
    )  # page 2 in the Inbox
    e = export(conn, settings, doc)
    assert e.kept == ["bundle.tif"] and e.originals == []
    assert e.folder is not None and e.pdf_path.is_file()
    state = conn.execute("SELECT original_state FROM scans").fetchone()[0]
    assert state == "held" and list((settings.library_dir / "originals").rglob("*.tif"))


def test_exported_again_under_a_new_name_the_originals_go_with_it(conn, settings, tmp_path):
    inbox = tmp_path / "drop"
    inbox.mkdir()
    doc = document(conn)
    taken(conn, settings, scan_file(inbox / "scan.png"), doc, [0])
    first = export(conn, settings, doc)
    reopen_document(conn, doc)
    conn.execute("UPDATE documents SET name = 'Will to Mary' WHERE id = ?", (doc,))
    conn.commit()
    again = export(conn, settings, doc)
    assert again.folder.name == "Will to Mary" and [p.name for p in again.originals] == ["scan.png"]
    assert not first.folder.exists()  # emptied, so removed
    # Exported again as it is, nothing moves
    reopen_document(conn, doc)
    same = export(conn, settings, doc)
    assert same.folder == again.folder and same.originals == again.originals


def test_originals_with_the_same_name_are_both_kept(conn, settings, tmp_path):
    doc = document(conn)
    for i, color in enumerate(("white", "ivory")):
        folder = tmp_path / f"drop{i}"
        folder.mkdir()
        taken(conn, settings, scan_file(folder / "scan.png", color), doc, [i])
    e = export(conn, settings, doc)
    assert [p.name for p in e.originals] == ["scan.png", "scan (2).png"]


def test_an_exact_copy_of_an_exported_original_dropped_in_again_is_removed(
    conn, settings, tmp_path
):
    from lindley.worker.intake import import_file

    inbox = tmp_path / "drop"
    inbox.mkdir()
    doc = document(conn)
    taken(conn, settings, scan_file(inbox / "scan.png"), doc, [0])
    e = export(conn, settings, doc)
    again = scan_file(inbox / "scan again.png")
    assert import_file(conn, settings, again).status == "duplicate"
    assert not again.exists() and e.originals[0].is_file()


def test_the_api_exports_several_at_once(client, settings, tmp_path):
    c = connect(settings.db_path)
    done, todo = document(c, "Done"), document(c, "Not yet")
    page(c, tmp_path, done, 1, "Dear Sister")
    page(c, tmp_path, todo, 1, "Dear Brother")
    c.close()
    client.post(f"/api/documents/{done}/complete")
    r = client.post("/api/documents/export", json={"document_ids": [done, todo, 999]}).json()
    assert [e["file_name"] for e in r["exported"]] == ["Done.pdf"]
    assert [f["document_id"] for f in r["failed"]] == [todo, 999]
    assert "isn't completed yet" in r["failed"][0]["error"]
