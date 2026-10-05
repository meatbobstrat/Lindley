import json
import threading

import pytest
from PIL import Image, ImageDraw

from lindley import browse
from lindley.assembler import assemble
from lindley.config import Settings
from lindley.db.database import connect, init_db
from lindley.providers import allowance
from lindley.providers.base import Transcription
from lindley.providers.connectors.fake import FakeProvider
from lindley.providers.throttle import Guarded, Throttle
from lindley.worker.intake import import_file
from lindley.worker.ocr.base import PageResult
from lindley.worker.pipeline import (
    Pipeline,
    rate_vision_readings,
    recover_interrupted,
    unfinished_scans,
    waiting_for_vision,
)


class StubOcr:
    """Hands out prepared readings in order, like Tesseract reading one page after another.

    `turn` is the way Tesseract turns a page it's sure of as it reads it (oriented_reading);
    `osd` is what its separate orientation check says, (rotation, confidence)."""

    name = "tesseract"
    version = "tesseract v5.test eng"

    def __init__(
        self,
        *readings: tuple[str, float | None] | Exception,
        turn: int = 0,
        osd: tuple[int, float] | None = None,
    ) -> None:
        self.readings = list(readings)
        self.turn, self.osd = turn, osd
        self.seen: list[tuple] = []  # (image path, its size) for each page read
        self.checked = 0  # orientation checks asked for

    def orientation(self, image_path):
        self.checked += 1
        return self.osd

    def oriented_reading(self, image_path):
        turn, self.turn = self.turn, 0  # only the first page read is turned
        if turn in (90, 270):
            with Image.open(image_path) as img:
                self.seen.append((image_path, img.size))
            return turn, None
        return turn, self.recognize(image_path)[0]

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

    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, image, hints=None):
        self.calls += 1
        raise ConnectionError("vision model not running")


class MustNotCall:
    """A vision model that fails the test if Lindley calls it without a person's OK."""

    model = "paid-vision"

    def transcribe(self, image, hints=None):
        raise AssertionError("the vision model was called without an OK")


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
    settings.ai.providers["local"].allow = "auto"
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), FakeProvider())
    assert pipe.process_scan(conn, sid) == "read"
    rs = readings(conn, sid)
    assert [(r["source"], r["is_current"]) for r in rs] == [("tesseract", 0), ("vision", 1)]
    assert steps(conn, sid, "vision") == [("done", None)]


def test_automatic_vision_calls_are_recorded_and_stop_at_the_daily_limit(conn, settings, scan):
    cfg = settings.ai.providers["local"]
    cfg.allow, cfg.daily_limit = "auto", 1
    first, second = scan("a.png"), scan("b.png")
    vision = Guarded(FakeProvider(), "local", Throttle())  # as get_provider makes it
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0), ("Dcar Sistcr", 41.0)), vision)
    assert pipe.process_scan(conn, first) == "read"
    assert pipe.process_scan(conn, second) == "read"  # Tesseract's reading, for now
    assert steps(conn, first, "vision") == [("done", None)]
    [(status, why)] = steps(conn, second, "vision")
    assert status == "queued" and "1 automatic calls are used up" in why
    calls = conn.execute("SELECT provider, purpose, automatic, ok FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("local", "vision", 1, 1)]
    pipe.read_waiting(conn)  # a person's OK: sent, recorded, not counted against the limit
    calls = conn.execute(
        "SELECT automatic, p.scan_id, model, input_tokens FROM ai_calls c"
        " JOIN pages p ON p.id = c.page_id ORDER BY c.id"
    ).fetchall()
    assert [tuple(c) for c in calls] == [(1, first, "fake", 1000), (0, second, "fake", 1000)]


def test_when_the_vision_model_fails_tesseract_still_counts(conn, settings, scan):
    settings.ai.providers["local"].allow = "auto"
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
    settings.ai.providers["local"].allow = "auto"
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


def test_a_sideways_page_is_read_again_from_an_upright_copy(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("Dear Sister, we are well.", 92.0), turn=90)
    assert Pipeline(settings, ocr).process_scan(conn, sid) == "read"
    assert page_row(conn, sid)["detected_rotation"] == 90
    [(_, first), (path, size)] = ocr.seen
    assert first == (400, 560) and size == (560, 400) and path.parent == settings.processing_dir
    assert not path.exists()  # the turned copy is removed; the original is untouched
    original = conn.execute("SELECT image_path FROM pages").fetchone()[0]
    with Image.open(original) as img:
        assert img.size == (400, 560)
    assert steps(conn, sid, "ocr") == [("done", None), ("done", None)]
    assert ocr.checked == 0


def test_an_upside_down_page_is_read_once(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("Dear Sister, we are well.", 92.0), turn=180)
    assert Pipeline(settings, ocr).process_scan(conn, sid) == "read"
    assert page_row(conn, sid)["detected_rotation"] == 180
    assert len(ocr.seen) == 1 and steps(conn, sid, "ocr") == [("done", None)]
    assert current_text(conn, sid) == "Dear Sister, we are well."


def test_a_page_that_reads_well_has_no_separate_orientation_check(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("Dear Sister, we are well.", 92.0), osd=(180, 9.0))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert ocr.checked == 0 and page_row(conn, sid)["detected_rotation"] == 0


def test_a_turn_set_by_a_person_is_trusted(conn, settings, scan):
    sid = scan()
    conn.execute("UPDATE pages SET user_rotation = 270")
    conn.commit()
    ocr = StubOcr(("Dcar Sistcr", 30.0), turn=180, osd=(90, 9.0))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert ocr.seen[0][1] == (560, 400)  # read the way the person turned it
    assert len(ocr.seen) == 1 and ocr.checked == 0
    assert page_row(conn, sid)["detected_rotation"] == 0


def test_a_blank_page_is_not_sent_to_the_vision_model(conn, settings, scan):
    sid = scan(img=Image.new("RGB", (400, 560), "white"))
    pipe = Pipeline(settings, StubOcr(("", None), osd=(90, 1.0)), FakeProvider())
    assert pipe.process_scan(conn, sid) == "read"
    assert steps(conn, sid, "vision") == [("skipped", "The page looks blank")]
    p = page_row(conn, sid)
    assert (p["blank_score"], p["detected_rotation"], p["script"]) == (1.0, 0, "none")
    assert [r["source"] for r in readings(conn, sid)] == ["tesseract"]
    assert pipe.tesseract.checked == 0


def test_a_page_only_the_vision_model_reads_is_turned_when_tesseract_is_sure(conn, settings, scan):
    settings.ocr.engine = "vision"
    settings.ai.providers["local"].allow = "auto"
    sure, unsure = scan("a.png"), scan("b.png")
    Pipeline(settings, StubOcr(osd=(90, 9.0)), FakeProvider()).process_scan(conn, sure)
    Pipeline(settings, StubOcr(osd=(90, 1.0)), FakeProvider()).process_scan(conn, unsure)
    assert page_row(conn, sure)["detected_rotation"] == 90
    assert page_row(conn, unsure)["detected_rotation"] == 0


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


# ---------------------------------------------------------------- Vision only with a person's OK


def test_by_default_a_hard_page_waits_for_an_ok(conn, settings, scan):
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), MustNotCall())
    assert pipe.process_scan(conn, sid) == "read"
    assert [(r["source"], r["is_current"]) for r in readings(conn, sid)] == [("tesseract", 1)]
    assert steps(conn, sid, "vision") == [("queued", "Waiting for you to OK the vision model")]
    assert waiting_for_vision(conn) == 1


def test_a_waiting_page_is_queued_only_once(conn, settings, scan):
    settings.ocr.engine = "vision"
    sid = scan()
    pipe = Pipeline(settings, StubOcr(), MustNotCall())
    assert pipe.process_scan(conn, sid) == "queued" and status(conn, sid) == "queued"
    assert pipe.process_scan(conn, sid) == "queued"  # e.g. the watcher's start-up sweep
    assert len(steps(conn, sid, "vision")) == 1 and readings(conn, sid) == []


def test_read_waiting_sends_the_pages_a_person_oked(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), MustNotCall()).process_scan(conn, sid)
    run = Pipeline(settings, StubOcr(), FakeProvider()).read_waiting(conn)
    assert (run.read, run.failed, run.waiting) == (1, 0, 0)
    rs = readings(conn, sid)
    assert [(r["source"], r["is_current"]) for r in rs] == [("tesseract", 0), ("vision", 1)]
    assert steps(conn, sid, "vision") == [("done", None)]  # the queued step, now run


def test_read_waiting_finishes_a_vision_only_scan(conn, settings, scan):
    settings.ocr.engine = "vision"
    sid = scan()
    Pipeline(settings, StubOcr(), MustNotCall()).process_scan(conn, sid)
    Pipeline(settings, StubOcr(), FakeProvider()).read_waiting(conn)
    assert status(conn, sid) == "read" and [r["source"] for r in readings(conn, sid)] == ["vision"]


def test_a_failed_vision_call_is_only_tried_again_when_asked(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), MustNotCall()).process_scan(conn, sid)
    broken = BrokenVision()
    run = Pipeline(settings, StubOcr(), broken).read_waiting(conn)
    assert (run.read, run.failed, run.waiting) == (0, 1, 0)
    assert Pipeline(settings, StubOcr(), broken).read_waiting(conn).failed == 0
    assert broken.calls == 1
    run = Pipeline(settings, StubOcr(), FakeProvider()).read_waiting(conn, retry_failed=True)
    assert run.read == 1
    assert [s for s, _ in steps(conn, sid, "vision")] == ["failed", "done"]


def test_a_failed_vision_only_page_is_not_sent_again_on_its_own(conn, settings, scan):
    settings.ocr.engine = "vision"
    settings.ai.providers["local"].allow = "auto"
    sid = scan()
    broken = BrokenVision()
    assert Pipeline(settings, StubOcr(), broken).process_scan(conn, sid) == "queued"
    assert Pipeline(settings, StubOcr(), broken).process_scan(conn, sid) == "queued"
    assert broken.calls == 1


def test_read_waiting_stops_when_the_provider_keeps_failing(conn, settings, scan):
    sids = [scan(f"scan_000{i}.png") for i in range(1, 6)]
    for sid in sids:
        Pipeline(settings, StubOcr(("Dcar", 30.0)), MustNotCall()).process_scan(conn, sid)
    broken = BrokenVision()
    run = Pipeline(settings, StubOcr(), broken).read_waiting(conn)
    assert broken.calls == 3 and run.failed == 3 and run.waiting == 2
    assert run.stopped and "vision model not running" in run.stopped


def test_a_page_a_person_checked_isnt_sent_to_the_vision_model(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), MustNotCall()).process_scan(conn, sid)
    conn.execute("UPDATE transcriptions SET confirmed_at = datetime('now')")
    conn.commit()
    run = Pipeline(settings, StubOcr(), MustNotCall()).read_waiting(conn, retry_failed=True)
    assert run.read == run.failed == run.waiting == 0
    assert [(r["source"], r["is_current"]) for r in readings(conn, sid)] == [("tesseract", 1)]


def test_words_tesseract_was_unsure_of_are_recorded(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), MustNotCall()).process_scan(conn, sid)
    spans = conn.execute(
        "SELECT t.unsure_spans FROM transcriptions t JOIN pages p ON p.id = t.page_id"
        " WHERE p.scan_id = ?",
        (sid,),
    ).fetchone()[0]
    assert json.loads(spans) == [[0, 4], [5, 11]]


def test_a_blank_page_never_waits_for_vision(conn, settings, scan):
    sid = scan(img=Image.new("RGB", (400, 560), "white"))
    Pipeline(settings, StubOcr(("", None)), MustNotCall()).process_scan(conn, sid)
    assert waiting_for_vision(conn) == 0


# ---------------------------------------------------------------- Unsure which way up


def current_text(conn, scan_id):
    return conn.execute(
        "SELECT t.text FROM transcriptions t JOIN pages p ON p.id = t.page_id"
        " WHERE p.scan_id = ? AND t.is_current = 1",
        (scan_id,),
    ).fetchone()[0]


def test_an_unsure_turn_is_kept_when_the_page_reads_clearly_better(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("6&o qno gut", 28.0), ("Dear Sister, we are well.", 60.0), osd=(90, 1.1))
    assert Pipeline(settings, ocr).process_scan(conn, sid) == "read"
    assert [size for _, size in ocr.seen] == [(400, 560), (560, 400)]
    assert page_row(conn, sid)["detected_rotation"] == 90
    assert current_text(conn, sid) == "Dear Sister, we are well."
    assert [r["source"] for r in readings(conn, sid)] == ["tesseract"]  # the garble isn't kept
    assert steps(conn, sid, "ocr") == [("done", None), ("done", None)]


def test_an_unsure_turn_is_not_tried_on_a_page_that_reads_well(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("Dear Sister, we are well.", 88.0), osd=(180, 0.8))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert len(ocr.seen) == 1 and page_row(conn, sid)["detected_rotation"] == 0


def test_an_unsure_turn_that_reads_no_better_is_dropped(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("He had been a mule driver", 61.0), ("fiostoa0 Sf", 26.0), osd=(180, 0.8))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert len(ocr.seen) == 2 and page_row(conn, sid)["detected_rotation"] == 0
    assert current_text(conn, sid) == "He had been a mule driver"


def test_a_sure_turn_tesseract_did_not_make_while_reading_is_only_a_guess(conn, settings, scan):
    sid = scan()
    ocr = StubOcr(("He had been a mule driver", 61.0), ("fiostoa0 Sf", 26.0), osd=(180, 9.0))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert ocr.checked == 1 and page_row(conn, sid)["detected_rotation"] == 0


def test_a_page_the_check_calls_upright_is_tried_upside_down_when_it_reads_poorly(
    conn, settings, scan
):
    # Typed pages lying upside down: the check said 0 degrees, even when sure
    sid = scan()
    ocr = StubOcr(("Suyuig yodouoy", 27.0), ("Tonopah Mining Company", 79.0), osd=(0, 4.3))
    Pipeline(settings, ocr).process_scan(conn, sid)
    assert page_row(conn, sid)["detected_rotation"] == 180
    assert current_text(conn, sid) == "Tonopah Mining Company"
    upright = StubOcr(("He had been a mule driver", 61.0), ("fiostoa0 Sf", 26.0), osd=(0, 4.3))
    Pipeline(settings, upright).process_scan(conn, sid := scan("b.png"))
    assert len(upright.seen) == 2 and page_row(conn, sid)["detected_rotation"] == 0


def read_before_upside_down_was_tried(conn, settings, scan, reading):
    """A page read the way it was scanned, then queued by the upgrade to schema 10."""
    from lindley.db.database import MIGRATIONS

    sid = scan()
    Pipeline(settings, StubOcr(reading)).process_scan(conn, sid)  # the check couldn't tell
    conn.executescript(MIGRATIONS[10])
    return sid


def test_a_page_read_upside_down_before_is_turned_over(conn, settings, scan):
    sid = read_before_upside_down_was_tried(conn, settings, scan, ("Suyuig yodouoy", 27.0))
    pipe = Pipeline(settings, StubOcr(("Tonopah Mining Company", 79.0), osd=(0, 4.3)))
    assert pipe.check_upside_down(conn) == (1, 1)
    page = page_row(conn, sid)
    assert page["detected_rotation"] == 180 and page["script"] == "printed"
    assert current_text(conn, sid) == "Tonopah Mining Company"
    assert [(r["source"], r["is_current"]) for r in readings(conn, sid)] == [
        ("tesseract", 0),
        ("tesseract", 1),
    ]
    assert json.loads(readings(conn, sid)[1]["words"])  # boxes, for the searchable PDF
    assert steps(conn, sid, "ocr")[-1] == ("done", None)
    assert pipe.check_upside_down(conn) == (0, 0)  # checked once


def test_a_page_read_poorly_the_right_way_up_stays_as_it_was(conn, settings, scan):
    sid = read_before_upside_down_was_tried(conn, settings, scan, ("He had been a mule", 61.0))
    pipe = Pipeline(settings, StubOcr(("fiostoa0 Sf", 26.0), osd=(0, 4.3)))
    assert pipe.check_upside_down(conn) == (1, 0)
    assert page_row(conn, sid)["detected_rotation"] == 0
    assert current_text(conn, sid) == "He had been a mule"


def test_a_page_that_reads_well_or_was_checked_isnt_read_again(conn, settings, scan):
    well = read_before_upside_down_was_tried(conn, settings, scan, ("Dear Sister", 88.0))
    sid = scan("b.png")
    Pipeline(settings, StubOcr(("Suyuig", 27.0))).process_scan(conn, sid)
    conn.execute(  # a person checked it after the upgrade queued it
        "UPDATE transcriptions SET confirmed_at = datetime('now') WHERE page_id = ?",
        (page_row(conn, sid)["id"],),
    )
    conn.execute(
        "INSERT INTO intake_steps (scan_id, page_id, step, status) VALUES (?, ?, 'ocr', 'queued')",
        (sid, page_row(conn, sid)["id"]),
    )
    conn.commit()
    ocr = StubOcr()  # nothing to read: it mustn't be asked
    assert Pipeline(settings, ocr).check_upside_down(conn) == (0, 0)
    assert ocr.checked == 0
    assert steps(conn, well, "ocr")[-1] == ("skipped", "It reads well enough")
    assert steps(conn, sid, "ocr")[-1] == (
        "skipped",
        "A person checked the text, or it isn't Tesseract's",
    )


def test_a_page_another_thread_is_reading_isnt_sent_twice(conn, settings, scan):
    a, b = scan("a.png"), scan("b.png")
    for sid in (a, b):
        Pipeline(settings, StubOcr(("Dcar", 30.0)), MustNotCall()).process_scan(conn, sid)
    pipe = Pipeline(settings, StubOcr(), FakeProvider())
    real = pipe._vision_reader

    def meanwhile():  # while a's page is read, a person's request takes b's
        conn.execute(
            "UPDATE intake_steps SET status = 'running' WHERE step = 'vision' AND scan_id = ?",
            (b,),
        )
        conn.commit()
        return real()

    pipe._vision_reader = meanwhile
    run = pipe.read_waiting(conn)
    assert (run.read, run.failed) == (1, 0)
    assert [s for s, _ in steps(conn, b, "vision")] == ["running"]


def test_a_failed_page_retried_elsewhere_isnt_retried_twice(conn, settings, scan):
    a, b = scan("a.png"), scan("b.png")
    for sid in (a, b):
        Pipeline(settings, StubOcr(("Dcar", 30.0)), MustNotCall()).process_scan(conn, sid)
    Pipeline(settings, StubOcr(), BrokenVision()).read_waiting(conn)  # both failed
    pipe = Pipeline(settings, StubOcr(), FakeProvider())
    real = pipe._vision_reader

    def meanwhile():  # while a's page is retried, another request retries b's
        conn.execute(
            "INSERT INTO intake_steps (scan_id, page_id, step, status)"
            " SELECT scan_id, page_id, 'vision', 'running' FROM intake_steps"
            " WHERE step = 'vision' AND scan_id = ?",
            (b,),
        )
        conn.commit()
        return real()

    pipe._vision_reader = meanwhile
    run = pipe.read_waiting(conn, retry_failed=True)
    assert (run.read, run.failed) == (1, 0)
    assert [s for s, _ in steps(conn, b, "vision")] == ["failed", "running"]


def test_a_page_being_read_isnt_sent_again_by_a_new_reading(conn, settings, scan):
    settings.ocr.engine = "vision"
    settings.ai.providers["local"].allow = "auto"
    sid = scan()
    conn.execute(
        "INSERT INTO intake_steps (scan_id, page_id, step, status) VALUES (?, ?, 'vision',"
        " 'running')",
        (sid, page_row(conn, sid)["id"]),
    )
    conn.commit()
    assert Pipeline(settings, StubOcr(), MustNotCall()).process_scan(conn, sid) == "queued"


def test_an_ai_that_keeps_failing_is_left_alone_and_pages_wait(conn, settings, scan):
    settings.ai.providers["local"].allow = "auto"
    sids = [scan(f"scan_000{i}.png") for i in range(1, 6)]
    broken = BrokenVision()
    pipe = Pipeline(settings, StubOcr(*[("Dcar", 30.0)] * 5), broken)
    for sid in sids:
        pipe.process_scan(conn, sid)
    assert broken.calls == allowance.FAILING_AFTER  # then it stopped calling
    [(status, why)] = steps(conn, sids[-1], "vision")
    assert status == "queued" and "calls failed" in why


def test_steps_cut_off_when_lindley_closed_are_picked_up(conn, settings, scan):
    sid = scan()
    page = page_row(conn, sid)["id"]
    conn.executemany(
        "INSERT INTO intake_steps (scan_id, page_id, step, status) VALUES (?, ?, ?, 'running')",
        [(sid, page, "ocr"), (sid, page, "vision")],
    )
    conn.execute("UPDATE scans SET status = 'reading'")
    conn.commit()
    assert recover_interrupted(conn) == (1, 1)
    assert steps(conn, sid, "ocr")[0][0] == "failed"
    assert steps(conn, sid, "vision")[0][0] == "queued" and waiting_for_vision(conn) == 1
    assert unfinished_scans(conn) == [sid]  # its reading was cut off too


def test_unfinished_scans_leave_out_pages_waiting_for_vision(conn, settings, scan):
    settings.ocr.engine = "vision"
    waiting, done, cut_off = scan("a.png"), scan("b.png"), scan("c.png")
    Pipeline(settings, StubOcr(), MustNotCall()).process_scan(conn, waiting)
    Pipeline(settings, StubOcr(), FakeProvider()).read_waiting(conn)
    Pipeline(settings, StubOcr(), MustNotCall()).process_scan(conn, done)
    settings.ai.providers["local"].allow = "auto"
    Pipeline(settings, StubOcr(), FakeProvider()).process_scan(conn, done)
    # cut_off: imported, then Lindley closed before reading it
    assert unfinished_scans(conn) == [cut_off]


# ---------------------------------------------------------------- A few scans at once


class SideBySide:
    """Reads a page only once `n` pages are being read together, so the test fails unless
    they really are read side by side."""

    name = "tesseract"
    version = "tesseract v5.test eng"

    def __init__(self, n: int, conf: float = 90.0) -> None:
        self.together = threading.Barrier(n, timeout=10)
        self.conf = conf

    def recognize(self, image_path):
        self.together.wait()
        return [PageResult(1, "Dear Sister, we are well.", self.conf, self.name, [])]


def test_scans_are_read_a_few_at_once(conn, settings, scan):
    settings.ocr.workers = 3
    sids = [scan(f"scan_000{i}.png") for i in range(1, 4)]
    done = []
    pipe = Pipeline(settings, SideBySide(3))
    statuses = pipe.process_scans(conn, sids, on_done=lambda s, st: done.append((s, st)))
    assert statuses == dict.fromkeys(sids, "read")
    assert sorted(done) == sorted(statuses.items())  # each reported as it finished
    assert all(status(conn, sid) == "read" for sid in sids)


def test_scans_not_started_when_asked_to_stop_are_left(conn, settings, scan):
    settings.ocr.workers = 2
    sids = [scan("a.png"), scan("b.png")]
    stop = threading.Event()
    stop.set()
    assert Pipeline(settings, StubOcr()).process_scans(conn, sids, stop=stop) == {}
    assert unfinished_scans(conn) == sids


def test_one_worker_reads_in_order_on_the_callers_connection(conn, settings, scan):
    settings.ocr.workers = 1
    sids = [scan("a.png"), scan("b.png")]
    ocr = StubOcr(("first", 90.0), ("second", 90.0))
    Pipeline(settings, ocr).process_scans(conn, sids)
    assert [current_text(conn, sid) for sid in sids] == ["first", "second"]


def test_scans_read_together_keep_to_the_vision_models_limit(conn, settings, scan):
    settings.ocr.workers = 3
    cfg = settings.ai.providers["local"]
    cfg.allow, cfg.daily_limit = "auto", 1
    sids = [scan(f"scan_000{i}.png") for i in range(1, 4)]
    Pipeline(settings, SideBySide(3, conf=41.0), FakeProvider()).process_scans(conn, sids)
    assert conn.execute("SELECT COUNT(*) FROM ai_calls WHERE automatic = 1").fetchone()[0] == 1
    assert waiting_for_vision(conn) == 2


def test_a_scan_whose_reading_raises_is_logged_and_left(conn, settings, scan, monkeypatch):
    settings.ocr.workers = 1
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("text", 90.0)))
    monkeypatch.setattr(pipe, "_settle", lambda *a: 1 / 0)
    assert pipe.process_scans(conn, [sid]) == {sid: None}


class Says:
    """A vision model that reads every page as `text`, and says nothing of how sure it is."""

    model = "says"

    def __init__(self, text: str) -> None:
        self.text = text

    def transcribe(self, image, hints=None):
        return Transcription(text=self.text)


def test_a_vision_reading_that_is_mostly_illegible_doesnt_replace_tesseracts(conn, settings, scan):
    sid = scan()
    pipe = Pipeline(settings, StubOcr(("Dcar Sistcr, wc arc wcll", 41.0)), Says("x"))
    pipe.process_scan(conn, sid)
    pipe.vision = Says("Dear [illegible] [illegible] [illegible] [illegible]")
    assert pipe.read_waiting(conn).read == 1
    rs = readings(conn, sid)
    assert [(r["source"], r["confidence"], r["is_current"]) for r in rs] == [
        ("tesseract", 41.0, 1),
        ("vision", 20.0, 0),
    ]


def test_a_clear_vision_reading_is_used_and_one_with_doubts_waits_for_review(conn, settings, scan):
    clear, doubtful = scan("a.png"), scan("b.png")
    reading = StubOcr(("Dcar Sistcr", 41.0), ("Dcar Sistcr", 41.0))
    pipe = Pipeline(settings, reading, Says("Dear Sister, we are all well here."))
    pipe.process_scan(conn, clear)
    pipe.process_scan(conn, doubtful)
    pipe.read_waiting(conn, [page_row(conn, clear)["id"]])
    pipe.vision = Says("Dear Sister [?], we are well [?], and Uncle Zebulon [?] sends love.")
    pipe.read_waiting(conn, [page_row(conn, doubtful)["id"]])
    review = settings.ocr.review_below
    states = {
        p["file"]: (p["source"], p["confidence"], p["state"]) for p in browse.inbox(conn, review)
    }
    assert states == {
        "a.png": ("vision", 100, "ok"),
        "b.png": ("vision", 75, "review"),  # 3 unsure of 11 words
    }


def test_vision_readings_made_before_they_had_a_confidence_get_one(conn, settings, scan):
    sid = scan()
    Pipeline(settings, StubOcr(("Dcar Sistcr", 41.0)), Says("x")).process_scan(conn, sid)
    page = page_row(conn, sid)["id"]
    for text in ("Dear [illegible] Sister [?]", ""):
        conn.execute(
            "INSERT INTO transcriptions (page_id, source, engine_model, text, is_current)"
            " VALUES (?, 'vision', 'old', ?, 0)",
            (page, text),
        )
    assert rate_vision_readings(conn) == 1  # a blank one has nothing to go on
    rated = conn.execute(
        "SELECT text, confidence FROM transcriptions WHERE source = 'vision' ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in rated] == [("Dear [illegible] Sister [?]", 33.3), ("", None)]
    assert rate_vision_readings(conn) == 0
