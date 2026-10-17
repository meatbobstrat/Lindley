"""After export, Lindley lets go of its own copies of the scans, and brings them back on reopen.

The person's originals are never touched: only Lindley's working copies and page images, in its
library, are removed, and only when the original is where Lindley left it, unchanged.
"""

import hashlib
import shutil
from pathlib import Path

import pytest
from PIL import Image

from lindley.db.database import connect, init_db
from lindley.export import complete_document, export_document, reopen_document
from lindley.export.keeping import OriginalsNeeded, let_go
from lindley.worker.intake import import_file


@pytest.fixture
def conn(settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


@pytest.fixture
def drop(tmp_path):
    folder = tmp_path / "drop"
    folder.mkdir()
    return folder


def scan_file(path, color="white", frames=1):
    imgs = [Image.new("L", (600, 800), color) for _ in range(frames)]
    imgs[0].save(path, save_all=frames > 1, append_images=imgs[1:], dpi=(150, 150))
    return path


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def document(conn, name="Letter from Will"):
    doc = conn.execute("INSERT INTO documents (name) VALUES (?)", (name,)).lastrowid
    conn.commit()
    return doc


def scanned(conn, settings, path, doc=None, positions=(), move=False):
    """Import a scan, read, its pages in `doc` at `positions` (the rest in the Inbox). Returns
    its scan id."""
    settings.move_files = move
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
    return r.scan_id


def export(conn, settings, doc):
    complete_document(conn, doc)
    return export_document(conn, settings.output_dir, doc, library=settings.library_dir)


def copies(conn, scan_id):
    """Lindley's own files for a scan: its working copy and page images."""
    s = conn.execute("SELECT library_path FROM scans WHERE id = ?", (scan_id,)).fetchone()[0]
    images = [
        p for (p,) in conn.execute("SELECT image_path FROM pages WHERE scan_id = ?", (scan_id,))
    ]
    return s, images


def library_files(settings):
    return sorted(p for p in settings.library_dir.rglob("*") if p.is_file())


def test_copy_mode_lets_go_of_its_copies_and_leaves_the_original(conn, settings, drop):
    a = scan_file(drop / "scan 1.png")
    b = scan_file(drop / "scan 2.tif", "ivory", frames=2)  # page images of its own
    before = {p: p.read_bytes() for p in (a, b)}
    doc = document(conn)
    sa = scanned(conn, settings, a, doc, [0])
    sb = scanned(conn, settings, b, doc, [1, 2])
    assert len(library_files(settings)) == 4  # two working copies, two page images
    e = export(conn, settings, doc)
    assert e.pdf_path.is_file() and e.copies_kept == []
    assert library_files(settings) == []
    assert not (settings.library_dir / "pages" / str(sb)).exists()
    assert copies(conn, sa) == (None, [None]) and copies(conn, sb) == (None, [None, None])
    assert {p: p.read_bytes() for p in (a, b)} == before  # the originals, untouched
    # What Lindley knows of them stays: the text, and the fingerprints
    assert conn.execute("SELECT COUNT(*) FROM v_current_text").fetchone()[0] == 3
    assert import_file(conn, settings, a).status == "duplicate"  # not imported again
    assert library_files(settings) == []


def test_move_mode_gives_the_originals_back_and_lets_go_of_its_copies(conn, settings, drop):
    a = scan_file(drop / "scan 1.png")
    want = sha(a)
    doc = document(conn)
    scanned(conn, settings, a, doc, [0], move=True)
    e = export(conn, settings, doc)
    [original] = e.originals
    assert sha(original) == want and original.parent == e.folder
    assert library_files(settings) == []


def test_a_scan_with_pages_elsewhere_is_kept(conn, settings, drop):
    doc = document(conn)
    s = scanned(conn, settings, scan_file(drop / "two.tif", frames=2), doc, [0])  # one in Inbox
    export(conn, settings, doc)
    library, images = copies(conn, s)
    assert library and all(images) and all(Path(p).is_file() for p in images)


def test_an_original_gone_from_where_it_was_keeps_lindleys_copy(conn, settings, drop):
    a = scan_file(drop / "scan 1.png")
    doc = document(conn)
    s = scanned(conn, settings, a, doc, [0])
    a.unlink()  # the person removed it before the export
    e = export(conn, settings, doc)
    assert e.copies_kept == ["scan 1.png"]
    library, _ = copies(conn, s)
    fingerprint = conn.execute("SELECT sha256 FROM scans WHERE id = ?", (s,)).fetchone()[0]
    assert Path(library).is_file() and sha(library) == fingerprint


def test_an_original_changed_since_keeps_lindleys_copy(conn, settings, drop):
    a = scan_file(drop / "scan 1.png")
    doc = document(conn)
    scanned(conn, settings, a, doc, [0])
    scan_file(a, "gray")  # a different file under the same name
    assert export(conn, settings, doc).copies_kept == ["scan 1.png"]
    assert len(library_files(settings)) == 1


def test_only_files_in_the_library_are_ever_removed(conn, settings, drop, tmp_path):
    a = scan_file(drop / "scan 1.png")
    doc = document(conn)
    s = scanned(conn, settings, a, doc, [0])
    elsewhere = tmp_path / "not lindleys.png"
    shutil.copy(a, elsewhere)
    conn.execute("UPDATE scans SET library_path = ? WHERE id = ?", (str(elsewhere), s))
    conn.execute("UPDATE pages SET image_path = ? WHERE scan_id = ?", (str(elsewhere), s))
    conn.commit()
    export(conn, settings, doc)
    assert elsewhere.is_file() and copies(conn, s) == (str(elsewhere), [str(elsewhere)])


def test_documents_exported_before_are_let_go_of_at_start_up(client, settings, drop):
    c = connect(settings.db_path)
    doc = document(c)
    scanned(c, settings, scan_file(drop / "scan 1.png"), doc, [0])
    complete_document(c, doc)
    export_document(c, settings.output_dir, doc)  # before Lindley let go of anything
    assert len(library_files(settings)) == 1
    assert let_go(c, settings.library_dir).scans and library_files(settings) == []
    c.close()


# ---------------------------------------------------------------- Reopening


def test_reopening_brings_the_scans_back_from_the_originals(conn, settings, drop):
    a = scan_file(drop / "scan 1.png")
    b = scan_file(drop / "scan 2.tif", "ivory", frames=2)
    doc = document(conn)
    sa = scanned(conn, settings, a, doc, [0])
    sb = scanned(conn, settings, b, doc, [1, 2])
    sizes = conn.execute("SELECT id, width_px, height_px FROM pages ORDER BY id").fetchall()
    export(conn, settings, doc)
    reopen_document(conn, doc, settings)
    assert conn.execute("SELECT status FROM documents").fetchone()[0] == "progress"
    for s in (sa, sb):
        library, images = copies(conn, s)
        assert Path(library).is_file() and all(Path(p).is_file() for p in images)
    assert conn.execute("SELECT id, width_px, height_px FROM pages ORDER BY id").fetchall() == sizes
    e = export(conn, settings, doc)  # and it can be exported again, then let go again
    assert e.pdf_path.is_file() and library_files(settings) == []


def test_reopening_in_move_mode_finds_the_originals_beside_the_pdf(conn, settings, drop):
    doc = document(conn)
    scanned(conn, settings, scan_file(drop / "scan 1.png"), doc, [0], move=True)
    first = export(conn, settings, doc)
    reopen_document(conn, doc, settings)
    conn.execute("UPDATE documents SET name = 'Will to Mary' WHERE id = ?", (doc,))
    conn.commit()
    again = export(conn, settings, doc)
    assert [p.name for p in again.originals] == ["scan 1.png"]
    assert again.folder != first.folder and not first.folder.exists()
    assert library_files(settings) == []


def test_an_original_that_isnt_there_is_asked_for(conn, settings, drop, tmp_path):
    a = scan_file(drop / "scan 1.png")
    doc = document(conn)
    s = scanned(conn, settings, a, doc, [0])
    export(conn, settings, doc)
    kept = tmp_path / "my copy.png"
    a.rename(kept)  # the person moved it
    with pytest.raises(OriginalsNeeded) as err:
        reopen_document(conn, doc, settings)
    assert err.value.needed == [(s, "scan 1.png")] and "scan 1.png" in str(err.value)
    assert conn.execute("SELECT status FROM documents").fetchone()[0] == "exported"
    wrong = scan_file(tmp_path / "another.png", "gray")
    with pytest.raises(OriginalsNeeded):
        reopen_document(conn, doc, settings, {s: wrong})  # not the same file
    reopen_document(conn, doc, settings, {s: kept})
    assert conn.execute("SELECT status FROM documents").fetchone()[0] == "progress"
    assert kept.is_file()  # copied, never taken: it's the person's


def test_in_move_mode_an_original_shown_again_is_kept_to_give_back(conn, settings, drop, tmp_path):
    a = scan_file(drop / "scan 1.png")
    want = sha(a)
    doc = document(conn)
    s = scanned(conn, settings, a, doc, [0], move=True)
    [given] = export(conn, settings, doc).originals
    shown = tmp_path / "upload" / "scan 1.png"
    shown.parent.mkdir()
    shutil.copy(given, shown)
    given.unlink()  # the person deleted it from Exported
    reopen_document(conn, doc, settings, {s: shown})
    state = conn.execute("SELECT original_state FROM scans WHERE id = ?", (s,)).fetchone()[0]
    assert state == "held"
    [back] = export(conn, settings, doc).originals
    assert sha(back) == want


def test_the_api_says_which_originals_reopening_needs(client, settings, drop):
    c = connect(settings.db_path)
    doc = document(c)
    a = scan_file(drop / "scan 1.png")
    s = scanned(c, settings, a, doc, [0])
    c.close()
    client.post(f"/api/documents/{doc}/complete")
    body = client.post(f"/api/documents/{doc}/export").json()
    assert body["copies_kept"] == []
    full = client.get(f"/api/documents/{doc}").json()
    assert full["export"]["let_go"]
    pid = full["pages"][0]["id"]
    image = client.get(f"/api/pages/{pid}/image")
    assert image.status_code == 200 and image.headers["content-type"] == "image/png"
    a.unlink()
    r = client.post(f"/api/documents/{doc}/reopen")
    assert r.status_code == 409
    assert r.json()["detail"]["needed"] == [{"scan_id": s, "file": "scan 1.png"}]
    assert "scan 1.png" in r.json()["detail"]["message"]
