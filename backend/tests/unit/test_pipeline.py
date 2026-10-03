import pytest
from PIL import Image, ImageDraw

from lindley.assembler import assemble
from lindley.config import Settings
from lindley.db.database import connect, init_db
from lindley.providers.fake import FakeProvider
from lindley.worker.intake import import_file
from lindley.worker.ocr.base import PageResult
from lindley.worker.pipeline import Pipeline


class StubOcr:
    """Hands out prepared readings in order, like Tesseract reading one page after another."""

    name = "tesseract"
    version = "tesseract v5.test eng"

    def __init__(
        self, *readings: tuple[str, float | None] | Exception, rotation: int | None = None
    ) -> None:
        self.readings = list(readings)
        self.rotation = rotation
        self.seen: list[tuple] = []  # (image path, its size) for each page read

    def orientation(self, image_path):
        return self.rotation

    def recognize(self, image_path):
        with Image.open(image_path) as img:
            self.seen.append((image_path, img.size))
        r = self.readings.pop(0)
        if isinstance(r, Exception):
            raise r
        text, conf = r
        words = [{"text": w, "conf": conf, "bbox": [10, 10, 40, 20]} for w in text.split()]
        return [PageResult(1, text, conf, self.name, words)]


class BrokenVision:
    model = "broken-vision"

    def transcribe(self, image, hints=None):
        raise ConnectionError("vision model not running")


@pytest.fixture
def conn(settings: Settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


def written_page(size=(400, 560), lines=8) -> Image.Image:
    img = Image.new("RGB", size, "white")
    draw = ImageDraw.Draw(img)
    for i in range(lines):
        draw.rectangle([40, 60 + 40 * i, size[0] - 60, 72 + 40 * i], fill="black")
    return img


@pytest.fixture
def scan(conn, settings, tmp_path):
    """Import a one-page scan with some writing; each differs in size so none are duplicates."""
    made = []

    def make(name="scan_0001.png", img=None):
        path = tmp_path / name
        (img or written_page((400 + len(made), 560))).save(path)
        made.append(path)
        return import_file(conn, settings, path).scan_id

    return make


def page_row(conn, scan_id):
    return conn.execute("SELECT * FROM pages WHERE scan_id = ?", (scan_id,)).fetchone()


def readings(conn, scan_id):
    return conn.execute(
        "SELECT t.source, t.confidence, t.is_current, t.words FROM transcriptions t"
        " JOIN pages p ON p.id = t.page_id WHERE p.scan_id = ? ORDER BY t.id",
        (scan_id,),
    ).fetchall()


def steps(conn, scan_id, step):
    return [
        (r["status"], r["error"])
        for r in conn.execute(
            "SELECT status, error FROM intake_steps WHERE scan_id = ? AND step = ?",
            (scan_id, step),
        )
    ]


def status(conn, scan_id):
    return conn.execute("SELECT status FROM scans WHERE id = ?", (scan_id,)).fetchone()[0]


def test_a_clear_page_is_read_by_tesseract_alone(conn, settings, scan):
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dear Sister, we are well.", 92.0)), FakeProvider())
    assert pipe.process_scan(conn, sid) == "read" and status(conn, sid) == "read"
    [r] = readings(conn, sid)
    assert (r["source"], r["confidence"], r["is_current"]) == ("tesseract", 92.0, 1)
    assert r["words"] and steps(conn, sid, "ocr") == [("done", None)]
    assert steps(conn, sid, "vision") == []
    assert conn.execute("SELECT language FROM pages").fetchone()[0] == "eng"
    engine = conn.execute("SELECT engine_model FROM transcriptions").fetchone()[0]
    assert engine == "tesseract v5.test eng"


def test_a_hard_page_goes_to_the_vision_model_and_both_readings_are_kept(conn, settings, scan):
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), FakeProvider())
    assert pipe.process_scan(conn, sid) == "read"
    rs = readings(conn, sid)
    assert [(r["source"], r["is_current"]) for r in rs] == [("tesseract", 0), ("vision", 1)]
    assert steps(conn, sid, "vision") == [("done", None)]


def test_when_the_vision_model_fails_tesseract_still_counts(conn, settings, scan):
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), BrokenVision())
    assert pipe.process_scan(conn, sid) == "read"
    assert [(r["source"], r["is_current"]) for r in readings(conn, sid)] == [("tesseract", 1)]
    assert steps(conn, sid, "vision") == [("failed", "vision model not running")]


def test_without_a_vision_model_the_step_is_skipped(conn, settings, scan):
    sid = scan()
    assert Pipeline(settings, StubOcr(("", None))).process_scan(conn, sid) == "read"
    assert steps(conn, sid, "vision") == [("skipped", "No vision model is set up")]
    assert readings(conn, sid)[0]["is_current"] == 1


def test_vision_only_reading(conn, settings, scan):
    settings.ocr.engine = "vision"
    sid = scan()
    assert Pipeline(settings, StubOcr(), FakeProvider()).process_scan(conn, sid) == "read"
    assert [r["source"] for r in readings(conn, sid)] == ["vision"]
    assert steps(conn, sid, "ocr") == []
    assert Pipeline(settings, StubOcr()).process_scan(conn, scan("b.png")) == "failed"


def test_a_failed_scan_can_be_read_again_without_repeating_pages(conn, settings, tmp_path):
    imgs = [Image.new("RGB", (100, 140), c) for c in ("white", "ivory")]
    imgs[0].save(tmp_path / "two.tif", save_all=True, append_images=imgs[1:])
    sid = import_file(conn, settings, tmp_path / "two.tif").scan_id
    pipe = Pipeline(settings, StubOcr(("page one", 90.0), RuntimeError("Tesseract crashed")))
    assert pipe.process_scan(conn, sid) == "failed"
    err = conn.execute("SELECT error FROM scans WHERE id = ?", (sid,)).fetchone()[0]
    assert err == "Tesseract crashed"
    assert steps(conn, sid, "ocr") == [("done", None), ("failed", "Tesseract crashed")]
    assert Pipeline(settings, StubOcr(("page two", 90.0))).process_scan(conn, sid) == "read"
    assert [r["source"] for r in readings(conn, sid)] == ["tesseract", "tesseract"]


def test_read_scans_become_a_lindley_document(conn, settings, scan):
    first, second = scan("scan_0001.png"), scan("scan_0002.png")
    pipe = Pipeline(
        settings,
        StubOcr(
            ("Xenia, O., March 4 1892\nDear Sister,\nWe are all well and the river came up", 90.0),
            ("over the low road again.\nYour loving brother\nWill", 90.0),
        ),
    )
    assert pipe.process_scan(conn, first) == pipe.process_scan(conn, second) == "read"
    report = assemble(conn, settings.assembler)
    assert report.documents_created == 1 and report.inbox_left == 0
    doc = conn.execute("SELECT name, origin FROM documents").fetchone()
    assert doc["origin"] == "lindley" and "Will" in doc["name"]


def test_the_image_is_checked_before_it_is_read(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dear Sister, we are well.", 92.0))).process_scan(conn, sid)
    p = page_row(conn, sid)
    assert p["blank_score"] < 0.97 and p["paper_color"] == "#ffffff" and len(p["phash"]) == 16
    assert (p["detected_rotation"], p["script"]) == (0, "printed")
    assert steps(conn, sid, "image") == [("done", None)]


def test_a_sideways_page_is_read_from_an_upright_copy(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("Dear Sister, we are well.", 92.0), rotation=90)
    assert Pipeline(settings, ocr).process_scan(conn, sid) == "read"
    assert page_row(conn, sid)["detected_rotation"] == 90
    [(path, size)] = ocr.seen
    assert size == (560, 400) and path.parent == settings.processing_dir
    assert not path.exists()  # the turned copy is removed; the original is untouched
    original = conn.execute("SELECT image_path FROM pages").fetchone()[0]
    with Image.open(original) as img:
        assert img.size == (400, 560)


def test_a_turn_set_by_a_person_is_added_to_the_detected_one(conn, settings, scan):
    sid = scan()
    conn.execute("UPDATE pages SET user_rotation = 180")
    conn.commit()
    ocr = StubOcr(("text", 92.0), rotation=90)
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert ocr.seen[0][1] == (560, 400)  # 90 + 180 = 270: still sideways


def test_a_blank_page_is_not_sent_to_the_vision_model(conn, settings, scan):
    sid = scan(img=Image.new("RGB", (400, 560), "white"))
    pipe = Pipeline(settings, StubOcr(("", None), rotation=90), FakeProvider())
    assert pipe.process_scan(conn, sid) == "read"
    assert steps(conn, sid, "vision") == [("skipped", "The page looks blank")]
    p = page_row(conn, sid)
    assert (p["blank_score"], p["detected_rotation"], p["script"]) == (1.0, 0, "none")
    assert [r["source"] for r in readings(conn, sid)] == ["tesseract"]


def test_handwriting_is_noticed_from_tesseracts_confidence(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr wc arc", 30.0))).process_scan(conn, sid)
    assert page_row(conn, sid)["script"] == "handwritten"


def test_a_retry_does_not_check_the_image_again(conn, settings, tmp_path):
    imgs = [written_page(), written_page(lines=4)]
    imgs[0].save(tmp_path / "two.tif", save_all=True, append_images=imgs[1:])
    sid = import_file(conn, settings, tmp_path / "two.tif").scan_id
    Pipeline(settings, StubOcr(("one", 90.0), RuntimeError("crashed"))).process_scan(conn, sid)
    Pipeline(settings, StubOcr(("two", 90.0))).process_scan(conn, sid)
    assert steps(conn, sid, "image") == [("done", None), ("done", None)]
