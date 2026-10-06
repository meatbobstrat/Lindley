import json
from pathlib import Path

import pytest
from PIL import Image

from lindley.config import Settings
from lindley.db.database import connect, init_db
from lindley.worker.intake import import_file, looks_complete, sha256_of


@pytest.fixture
def conn(settings: Settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


@pytest.fixture
def inbox(tmp_path):
    d = tmp_path / "inbox"
    d.mkdir()
    return d


def page(color="white", size=(200, 300)):
    return Image.new("RGB", size, color)


def make_jpeg(path):
    exif = Image.Exif()
    exif[271] = "Epson"
    exif[272] = "Perfection V600"
    exif.get_ifd(0x8769)[36867] = "2024:01:02 03:04:05"
    page().save(path, exif=exif, dpi=(300, 300))
    return path


def make_tiff(path, frames=3):
    imgs = [page(c) for c in ("white", "ivory", "linen")[:frames]]
    imgs[0].save(path, save_all=True, append_images=imgs[1:], dpi=(200, 200))
    return path


def make_pdf(path, pages=2):
    imgs = [page() for _ in range(pages)]
    imgs[0].save(path, "PDF", save_all=True, append_images=imgs[1:], resolution=100)
    return path


def steps(conn, scan_id):
    return [
        (r["step"], r["status"])
        for r in conn.execute("SELECT step, status FROM intake_steps WHERE scan_id = ?", (scan_id,))
    ]


def test_a_jpeg_becomes_one_page_with_its_exif(conn, settings, inbox):
    src = make_jpeg(inbox / "scan_0042.jpg")
    r = import_file(conn, settings, src)
    assert r.status == "new" and r.pages == 1
    scan = conn.execute("SELECT * FROM scans WHERE id = ?", (r.scan_id,)).fetchone()
    assert scan["sha256"] == sha256_of(src) and scan["status"] == "queued"
    assert scan["original_name"] == "scan_0042.jpg" and scan["origin"] == "added"
    assert scan["import_mode"] == "copy" and scan["mime_type"] == "image/jpeg"
    assert scan["scanner_make"] == "Epson" and scan["scanner_model"] == "Perfection V600"
    assert scan["scanned_at"] == "2024-01-02 03:04:05"
    assert json.loads(scan["exif_json"])["Make"] == "Epson"
    assert scan["file_modified_at"] and scan["page_count"] == 1
    p = conn.execute("SELECT * FROM pages WHERE scan_id = ?", (r.scan_id,)).fetchone()
    assert (p["width_px"], p["height_px"], p["dpi"], p["color_mode"]) == (200, 300, 300, "rgb")
    assert p["image_path"] == scan["library_path"]
    assert steps(conn, r.scan_id) == [("hash", "done"), ("exif", "done"), ("split", "done")]


def test_the_library_copy_matches_and_the_original_is_left_alone(conn, settings, inbox):
    src = make_jpeg(inbox / "a.jpg")
    before = src.read_bytes()
    r = import_file(conn, settings, src)
    lib = Path(
        conn.execute("SELECT library_path FROM scans WHERE id = ?", (r.scan_id,)).fetchone()[0]
    )
    assert src.read_bytes() == before
    assert settings.library_dir in lib.parents and lib.read_bytes() == before


def test_move_mode_removes_the_original_only_after_success(conn, settings, inbox):
    settings.move_files = True
    src = make_jpeg(inbox / "a.jpg")
    r = import_file(conn, settings, src)
    assert r.status == "new" and not src.exists()
    scan = conn.execute("SELECT * FROM scans").fetchone()
    assert scan["import_mode"] == "move"
    assert sha256_of(Path(scan["library_path"])) == scan["sha256"]


def test_the_same_file_twice_is_a_duplicate(conn, settings, inbox):
    first = import_file(conn, settings, make_jpeg(inbox / "a.jpg"))
    again = import_file(conn, settings, make_jpeg(inbox / "copy of a.jpg"))
    assert again.status == "duplicate" and again.scan_id == first.scan_id
    assert conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0] == 1


def test_a_multi_page_tiff_is_split_into_page_images(conn, settings, inbox):
    r = import_file(conn, settings, make_tiff(inbox / "letter.tif"))
    assert r.status == "new" and r.pages == 3
    rows = conn.execute("SELECT * FROM pages ORDER BY page_index").fetchall()
    assert [p["page_index"] for p in rows] == [0, 1, 2]
    for p in rows:
        with Image.open(p["image_path"]) as img:
            assert img.size == (200, 300)
        assert p["dpi"] == 200
    assert conn.execute("SELECT page_count FROM scans").fetchone()[0] == 3


def test_a_pdf_is_rendered_to_one_image_per_page(conn, settings, inbox):
    r = import_file(conn, settings, make_pdf(inbox / "deed.pdf"))
    assert r.status == "new" and r.pages == 2
    rows = conn.execute("SELECT * FROM pages ORDER BY page_index").fetchall()
    # 200x300 px at 100 dpi is 2x3 inches: 600x900 px at 300 dpi.
    for p in rows:
        assert abs(p["width_px"] - 600) <= 1 and abs(p["height_px"] - 900) <= 1 and p["dpi"] == 300
    assert all(Path(p["image_path"]).exists() for p in rows)
    assert conn.execute("SELECT mime_type FROM scans").fetchone()[0] == "application/pdf"


def test_a_broken_file_fails_and_is_quarantined(conn, settings, inbox):
    src = inbox / "broken.jpg"
    src.write_bytes(b"this is not a picture")
    r = import_file(conn, settings, src)
    assert r.status == "failed" and r.error
    scan = conn.execute("SELECT * FROM scans").fetchone()
    assert scan["status"] == "failed" and scan["error"]
    assert conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0] == 0
    assert ("exif", "failed") in steps(conn, scan["id"])
    assert src.exists()  # copy mode never removes the original
    assert (settings.quarantine_dir / "broken.jpg").read_bytes() == src.read_bytes()


@pytest.mark.parametrize(
    ("name", "data", "says"),
    [
        ("text.png", b"this is not a picture", "isn't a picture Lindley can open"),
        ("cut.jpg", b"\xff\xd8\xff\xe0 a JPEG start, and no more" + bytes(500), "cut short"),
        ("junk.pdf", b"%PDF-1.4 and nothing else", "The PDF can't be opened"),
    ],
)
def test_why_a_file_cant_be_added_is_said_in_words(conn, settings, inbox, name, data, says):
    src = inbox / name
    src.write_bytes(data)
    r = import_file(conn, settings, src)
    assert r.status == "failed" and says in r.error
    assert conn.execute("SELECT error FROM scans").fetchone()[0] == r.error


def test_a_failed_file_can_be_tried_again(conn, settings, inbox, monkeypatch):
    from lindley.worker import intake

    src = make_tiff(inbox / "a.tif")
    monkeypatch.setattr(intake, "_split", lambda *a: (_ for _ in ()).throw(ValueError("boom")))
    assert import_file(conn, settings, src).status == "failed"
    monkeypatch.undo()
    r = import_file(conn, settings, src)
    assert r.status == "new" and r.pages == 3
    assert conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == 1
    assert conn.execute("SELECT status, error FROM scans").fetchone()[:] == ("queued", None)


def test_unsupported_files_are_ignored(conn, settings, inbox):
    src = inbox / "notes.txt"
    src.write_text("hello")
    assert import_file(conn, settings, src).status == "failed"
    assert conn.execute("SELECT COUNT(*) FROM scans").fetchone()[0] == 0
    assert src.exists() and not settings.quarantine_dir.exists()


def test_a_photo_turned_by_exif_is_measured_as_it_is_shown(conn, settings, inbox):
    exif = Image.Exif()
    exif[0x0112] = 6  # the camera was held sideways
    page(size=(300, 200)).save(inbox / "phone.jpg", exif=exif)
    import_file(conn, settings, inbox / "phone.jpg")
    p = conn.execute("SELECT width_px, height_px FROM pages").fetchone()
    assert (p["width_px"], p["height_px"]) == (200, 300)


def test_a_file_cut_short_doesnt_look_complete(inbox):
    for name, make in (
        ("a.pdf", make_pdf),
        ("a.jpg", make_jpeg),
        ("a.png", lambda p: page().save(p) or p),
        ("a.bmp", lambda p: page().save(p) or p),
        ("a.webp", lambda p: page().save(p) or p),
    ):
        path = make(inbox / name)
        assert looks_complete(path), name
        data = path.read_bytes()
        path.write_bytes(data[: len(data) * 2 // 3])
        assert not looks_complete(path), name
    assert looks_complete(make_tiff(inbox / "a.tif"))  # nothing to check: a steady size does


def test_a_file_that_fails_again_isnt_quarantined_again(conn, settings, inbox):
    src = inbox / "broken.jpg"
    src.write_bytes(b"this is not a picture")
    for _ in range(3):  # copy mode tries it again each time Lindley starts
        assert import_file(conn, settings, src).status == "failed"
    assert [q.name for q in settings.quarantine_dir.iterdir()] == ["broken.jpg"]
    src.write_bytes(b"another broken file, by the same name")
    import_file(conn, settings, src)
    assert len(list(settings.quarantine_dir.iterdir())) == 2


def test_move_mode_keeps_going_when_the_original_cant_be_removed(
    conn, settings, inbox, monkeypatch
):
    settings.move_files = True
    src = make_jpeg(inbox / "a.jpg")
    unlink = Path.unlink

    def locked(self, *a, **k):
        if self == src:
            raise PermissionError("in use by another process")
        return unlink(self, *a, **k)

    monkeypatch.setattr(Path, "unlink", locked)
    r = import_file(conn, settings, src)
    assert r.status == "new" and r.pages == 1 and src.exists()
    monkeypatch.undo()
    again = import_file(conn, settings, src)  # seen again once the other program let go
    assert again.status == "duplicate" and not src.exists()


def test_move_mode_removes_a_file_already_in_the_library(conn, settings, inbox):
    first = make_jpeg(inbox / "a.jpg")
    import_file(conn, settings, first)  # read in copy mode
    settings.move_files = True
    copy = make_jpeg(inbox / "copy of a.jpg")
    assert import_file(conn, settings, copy).status == "duplicate"
    assert not copy.exists()


def test_a_scan_put_in_a_pdf_at_72_dpi_isnt_rendered_enormous(conn, settings, inbox):
    src = inbox / "big.pdf"
    page(size=(2480, 3508)).save(src, "PDF")  # an A4 scan at 300 dpi, on a 34 x 49 inch page
    r = import_file(conn, settings, src)
    p = conn.execute("SELECT width_px, height_px, dpi FROM pages").fetchone()
    assert r.status == "new" and (p["width_px"], p["height_px"], p["dpi"]) == (2480, 3508, 72)


def test_a_huge_pdf_page_without_a_scan_is_rendered_within_the_limit(conn, settings, inbox):
    import pypdfium2 as pdfium

    from lindley.worker import intake

    pdf = pdfium.PdfDocument.new()
    pdf.new_page(72 * 40, 72 * 60)  # a 40 x 60 inch drawing, nothing in it but lines
    pdf.save(inbox / "plan.pdf")
    pdf.close()
    import_file(conn, settings, inbox / "plan.pdf")
    p = conn.execute("SELECT width_px, height_px FROM pages").fetchone()
    assert p["width_px"] * p["height_px"] <= intake.MAX_RENDER_PX


def test_tiff_pages_in_cmyk_are_kept_as_rgb(conn, settings, inbox):
    imgs = [Image.new("CMYK", (200, 300)) for _ in range(2)]
    imgs[0].save(inbox / "print.tif", save_all=True, append_images=imgs[1:])
    r = import_file(conn, settings, inbox / "print.tif")
    assert r.status == "new" and r.pages == 2
    for (path,) in conn.execute("SELECT image_path FROM pages"):
        with Image.open(path) as img:
            assert img.mode == "RGB"


def test_tiff_pages_turned_by_their_orientation_are_saved_upright(conn, settings, inbox):
    exif = Image.Exif()
    exif[0x0112] = 6
    imgs = [page(size=(300, 200)) for _ in range(2)]
    imgs[0].save(inbox / "turned.tif", save_all=True, append_images=imgs[1:], exif=exif)
    import_file(conn, settings, inbox / "turned.tif")
    for p in conn.execute("SELECT image_path, width_px, height_px FROM pages"):
        assert (p["width_px"], p["height_px"]) == (200, 300)
        with Image.open(p["image_path"]) as img:
            assert img.size == (200, 300)


def test_exif_it_cant_make_out_doesnt_stop_the_pages(conn, settings, inbox, monkeypatch):
    from lindley.worker import intake

    monkeypatch.setattr(intake, "_exif", lambda *a: (_ for _ in ()).throw(SyntaxError("bad")))
    r = import_file(conn, settings, make_jpeg(inbox / "a.jpg"))
    assert r.status == "new" and r.pages == 1
    assert ("exif", "failed") in steps(conn, r.scan_id) and ("split", "done") in steps(
        conn, r.scan_id
    )


def test_a_picture_too_big_to_read_safely_says_so(conn, settings, inbox, monkeypatch):
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 1000)
    r = import_file(conn, settings, make_jpeg(inbox / "a.jpg"))
    assert r.status == "failed" and "too big to read safely" in r.error


def test_library_paths_are_absolute_whatever_the_settings_say(
    conn, settings, inbox, tmp_path, monkeypatch
):
    from lindley.worker.intake import absolute_paths

    monkeypatch.chdir(tmp_path)
    settings.library_dir = Path("library")  # relative, as the default settings are
    r = import_file(conn, settings, make_tiff(inbox / "a.tif"))
    rows = conn.execute("SELECT image_path FROM pages").fetchall()
    assert all(Path(p[0]).is_absolute() for p in rows)
    lib = conn.execute("SELECT library_path FROM scans").fetchone()[0]
    assert Path(lib).is_absolute()
    # A database from before: relative paths become absolute while the files are there.
    conn.execute(
        "UPDATE scans SET library_path = ? WHERE id = ?",
        (str(Path(lib).relative_to(tmp_path)), r.scan_id),
    )
    conn.execute("UPDATE pages SET image_path = 'library/gone.png' WHERE page_index = 0")
    conn.commit()
    assert absolute_paths(conn) == 1
    assert conn.execute("SELECT library_path FROM scans").fetchone()[0] == lib
