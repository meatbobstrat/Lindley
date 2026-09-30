import pytest
from PIL import Image

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

    def __init__(self, *readings: tuple[str, float | None] | Exception) -> None:
        self.readings = list(readings)

    def recognize(self, image_path):
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


@pytest.fixture
def scan(conn, settings, tmp_path):
    """Import a one-page scan; each call makes a different image so none are duplicates."""
    made = []

    def make(name="scan_0001.png"):
        path = tmp_path / name
        Image.new("RGB", (100 + len(made), 140), "white").save(path)
        made.append(path)
        return import_file(conn, settings, path).scan_id

    return make


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
