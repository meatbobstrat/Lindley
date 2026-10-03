import io
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient
from PIL import Image, ImageDraw

from lindley.app import create_app
from lindley.config import Settings
from lindley.db.database import connect, init_db
from lindley.watcher import watcher as watcher_mod
from lindley.watcher.watcher import FolderWatcher
from lindley.worker.ocr.base import PageResult
from lindley.worker.pipeline import Pipeline


class StubOcr:
    name = "tesseract"
    version = "tesseract v5.test eng"

    def recognize(self, image_path):
        return [PageResult(1, "Dear Sister, we are well.", 90.0, self.name, [])]


@pytest.fixture
def inbox(settings: Settings):
    folder = settings.watch_folders[0]
    folder.mkdir(parents=True)
    init_db(settings.db_path)
    return folder


def png_bytes(color="white"):
    buf = io.BytesIO()
    Image.new("RGB", (120, 160), color).save(buf, "PNG")
    return buf.getvalue()


def make_watcher(settings, **kw):
    kw.setdefault("settle_s", 0)
    kw.setdefault("stable_s", 0)
    kw.setdefault("pipeline", Pipeline(settings, StubOcr()))
    return FolderWatcher(settings, chat=None, **kw)


def scans(settings):
    conn = connect(settings.db_path)
    try:
        return [tuple(r) for r in conn.execute("SELECT original_name, origin, status FROM scans")]
    finally:
        conn.close()


def test_a_file_is_taken_only_once_it_stops_growing(settings, inbox):
    w = make_watcher(settings)
    data = png_bytes()
    path = inbox / "scan_0001.png"
    path.write_bytes(data[:100])
    w.notice(path)
    assert w.tick() == []  # first look: size noted
    with path.open("ab") as f:
        f.write(data[100:])
    assert w.tick() == []  # it grew
    assert w.tick() == [path]  # unchanged since the last look
    assert scans(settings) == [("scan_0001.png", "watched", "read")]
    assert w.tick() == []  # taken once


def test_the_assembler_runs_once_things_settle(settings, inbox, monkeypatch):
    calls = []
    monkeypatch.setattr(watcher_mod, "sort_on_its_own", lambda *a: calls.append(a) or _Report())
    w = make_watcher(settings, settle_s=3600)
    (inbox / "a.png").write_bytes(png_bytes())
    w.notice(inbox / "a.png")
    w.tick()
    w.tick()
    assert calls == []  # still inside the settle window
    w.settle_s = 0
    w.tick()
    assert len(calls) == 1
    w.tick()
    assert len(calls) == 1  # nothing new since


class _Report:
    considered = documents_created = ai_calls = 0


def test_files_already_read_from_the_same_place_are_not_hashed_again(settings, inbox, monkeypatch):
    (inbox / "a.png").write_bytes(png_bytes())
    first = make_watcher(settings)
    first.sweep([inbox])
    first.tick()
    assert first.tick() == [inbox / "a.png"]
    later = make_watcher(settings)
    monkeypatch.setattr(watcher_mod, "ingest", lambda *a, **k: pytest.fail("re-imported"))
    later.sweep([inbox])
    assert later.tick() == [] and later.tick() == []


def test_unsupported_files_and_lindleys_own_folders_are_ignored(settings, inbox):
    settings.library_dir = inbox / "library"
    settings.library_dir.mkdir()
    (inbox / "notes.txt").write_text("hello")
    (settings.library_dir / "page.png").write_bytes(png_bytes())
    w = make_watcher(settings)
    w.sweep([inbox])
    assert w.tick() == [] and w.tick() == []
    assert scans(settings) == []


def test_a_missing_watch_folder_is_skipped(settings, tmp_path):
    settings.watch_folders = [tmp_path / "nowhere"]
    assert make_watcher(settings).folders() == []


def test_the_background_watcher_picks_up_a_dropped_file(settings, inbox):
    w = make_watcher(settings, poll_s=0.05)
    read = [("dropped.png", "watched", "read")]
    w.start()
    try:
        (inbox / "dropped.png").write_bytes(png_bytes("ivory"))
        deadline = time.monotonic() + 10
        # wait for it to be read, not only found: a busy computer can see it mid-read
        while scans(settings) != read and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        w.stop()
    assert scans(settings) == read


def test_the_app_starts_and_stops_the_watcher(settings, inbox, tmp_path):
    app = create_app(settings, settings_path=tmp_path / "settings.json")
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        w = app.state.watcher
        assert w._thread and w._thread.is_alive()
    assert not w._thread.is_alive()


class CountingVision:
    model = "paid-vision"

    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, image, hints=None):
        self.calls += 1
        raise ConnectionError("should not have been called")


def test_dropped_scans_never_call_the_vision_model_without_an_ok(settings, inbox):
    settings.ocr.engine = "vision"  # every page needs the vision model
    vision = CountingVision()
    (inbox / "a.png").write_bytes(png_bytes())
    for _ in range(2):  # a first run, then a restart whose start-up sweep finds it again
        w = make_watcher(settings, pipeline=Pipeline(settings, StubOcr(), vision))
        w.sweep([inbox])
        w.tick()
        w.tick()
    assert scans(settings) == [("a.png", "watched", "queued")]
    assert vision.calls == 0


class SamePage:
    """Reads every scan as the same long page, as two scans of one sheet would read."""

    name = "tesseract"
    version = "tesseract v5.test eng"
    TEXT = " ".join(
        f"Line {i} of the memoir tells how we crossed the river at Eldorado in the spring."
        for i in range(8)
    )

    def recognize(self, image_path):
        return [PageResult(1, self.TEXT, 90.0, self.name, [])]


def test_the_watcher_finds_duplicates_before_assembling(settings, inbox):
    w = make_watcher(settings, pipeline=Pipeline(settings, SamePage()))
    for name, color in (("scan_0001.png", "white"), ("scan_0002.png", "ivory")):
        img = Image.new("RGB", (400, 560), color)
        ImageDraw.Draw(img).rectangle([40, 60, 340, 400], fill="black")  # some writing
        img.save(inbox / name)
        w.notice(inbox / name)
    for _ in range(3):
        w.tick()
    conn = connect(settings.db_path)
    try:
        assert [r[0] for r in conn.execute("SELECT kind FROM duplicates")] == ["same_page"]
        docs = [r[0] for r in conn.execute("SELECT document_id FROM pages")]
        assert docs[0] is None or docs[0] != docs[1]
    finally:
        conn.close()


def test_after_starting_the_inbox_is_sorted_once_with_nothing_new(settings, inbox, monkeypatch):
    """Pages may be waiting for an AI that may run on its own now."""
    calls = []
    monkeypatch.setattr(watcher_mod, "sort_on_its_own", lambda *a: calls.append(a) or _Report())
    w = make_watcher(settings, settle_s=0)
    w.tick()
    w.tick()
    assert len(calls) == 1


def test_saving_settings_starts_the_watcher_again_with_them(client, settings, monkeypatch):
    started = []

    class Watcher:
        def __init__(self, s):
            self.settings = s

        def start(self):
            started.append(self.settings)

        def stop(self, wait=True):
            started.append("stopped" if not wait else "waited")  # the request doesn't wait

    monkeypatch.setattr("lindley.api.settings.FolderWatcher", Watcher)
    client.app.state.watcher = Watcher(settings)
    new = settings.model_copy(deep=True)
    new.ai.providers["local"].allow = "auto"
    assert client.put("/api/settings", json=new.model_dump(mode="json")).status_code == 200
    assert started[0] == "stopped" and started[1].ai.providers["local"].allow == "auto"


def test_an_empty_file_doesnt_hold_up_sorting(settings, inbox, monkeypatch):
    calls = []
    monkeypatch.setattr(watcher_mod, "sort_on_its_own", lambda *a: calls.append(a) or _Report())
    w = make_watcher(settings, stuck_s=3600)
    (inbox / "empty.png").write_bytes(b"")
    (inbox / "a.png").write_bytes(png_bytes())
    w.sweep([inbox])
    for _ in range(3):
        w.tick()
    assert calls == []  # it may still be on its way
    w.stuck_s = 0
    assert w.tick() == []  # never taken while it's empty...
    assert len(calls) == 1  # ...but no longer waited for
    (inbox / "empty.png").write_bytes(png_bytes("ivory"))  # the scanner finished after all
    w.tick()
    assert w.tick() == [inbox / "empty.png"]


def test_one_file_that_fails_doesnt_lose_the_others(settings, inbox, monkeypatch):
    real = watcher_mod.ingest
    fail = {"a.png"}

    def ingest(conn, settings, pipeline, path, origin):
        if path.name in fail:
            raise sqlite3.OperationalError("database is locked")
        return real(conn, settings, pipeline, path, origin)

    monkeypatch.setattr(watcher_mod, "ingest", ingest)
    w = make_watcher(settings, settle_s=3600)
    for name, color in (("a.png", "white"), ("b.png", "ivory"), ("c.png", "linen")):
        (inbox / name).write_bytes(png_bytes(color))
    w.sweep([inbox])
    w.tick()
    w.tick()
    assert sorted(s[0] for s in scans(settings)) == ["b.png", "c.png"]
    fail.clear()  # the database is free again: it's tried again
    w.tick()
    w.tick()
    assert sorted(s[0] for s in scans(settings)) == ["a.png", "b.png", "c.png"]


def test_a_file_that_keeps_failing_is_given_up_on(settings, inbox, monkeypatch):
    tries = []
    monkeypatch.setattr(watcher_mod, "ingest", lambda *a, **k: tries.append(1) / 0)
    w = make_watcher(settings, settle_s=3600)
    (inbox / "a.png").write_bytes(png_bytes())
    w.notice(inbox / "a.png")
    for _ in range(10):
        w.tick()
    assert len(tries) == watcher_mod.MAX_TRIES and not w._pending


def test_a_pdf_still_being_written_waits_for_its_ending(settings, inbox):
    w = make_watcher(settings, settle_s=3600)
    buf = io.BytesIO()
    Image.new("RGB", (120, 160), "white").save(buf, "PDF")
    path = inbox / "scan.pdf"
    path.write_bytes(buf.getvalue()[:-200])  # the scanner paused half way
    w.notice(path)
    for _ in range(3):
        assert w.tick() == []
    path.write_bytes(buf.getvalue())
    w.tick()
    assert w.tick() == [path]


def test_a_file_must_stay_unchanged_for_a_while(settings, inbox):
    w = make_watcher(settings, settle_s=3600, stable_s=3600)
    (inbox / "a.png").write_bytes(png_bytes())
    w.notice(inbox / "a.png")
    for _ in range(3):
        assert w.tick() == []
    w.stable_s = 0
    assert w.tick() == [inbox / "a.png"]


def test_assembling_that_fails_waits_before_trying_again(settings, inbox, monkeypatch):
    calls = []

    def broken(*a):
        calls.append(a)
        raise RuntimeError("boom")

    monkeypatch.setattr(watcher_mod, "sort_on_its_own", broken)
    w = make_watcher(settings)
    for _ in range(5):
        w.tick()
    assert len(calls) == 1 and w._unassembled  # waits, rather than failing every second
    w._assemble_after = 0
    w.tick()
    assert len(calls) == 2


def test_lindleys_working_folder_is_ignored(settings, inbox):
    settings.processing_dir = inbox / "processing"
    settings.processing_dir.mkdir()
    (settings.processing_dir / "page-1.png").write_bytes(png_bytes())
    w = make_watcher(settings)
    w.sweep([inbox])
    assert w.tick() == [] and w.tick() == []


class NoTesseract:
    name = "tesseract"
    version = "tesseract (missing)"

    def recognize(self, image_path):
        raise RuntimeError("Tesseract wasn't found")


def test_a_scan_whose_reading_failed_is_read_when_lindley_starts_again(settings, inbox):
    settings.move_files = True  # the original goes once imported: only Lindley's copy is left
    (inbox / "a.png").write_bytes(png_bytes())
    first = make_watcher(settings, pipeline=Pipeline(settings, NoTesseract()))
    first.sweep([inbox])
    first.tick()
    first.tick()
    assert scans(settings) == [("a.png", "watched", "failed")]
    assert not (inbox / "a.png").exists()
    later = make_watcher(settings)  # Tesseract installed since
    assert len(later.resume()) == 1
    assert scans(settings) == [("a.png", "watched", "read")]
    assert later.resume() == []


def test_a_new_watcher_waits_for_the_old_one_to_finish(settings, inbox):
    (inbox / "a.png").write_bytes(png_bytes())
    w = make_watcher(settings, poll_s=0.02)
    with watcher_mod._WORK:  # the old watcher is part way through a long read
        w.start()
        time.sleep(0.3)
        assert scans(settings) == []
    deadline = time.monotonic() + 10
    while scans(settings) != [("a.png", "watched", "read")] and time.monotonic() < deadline:
        time.sleep(0.05)
    w.stop()
    assert scans(settings) == [("a.png", "watched", "read")]


def test_the_app_picks_up_work_cut_off_when_it_closed(settings, inbox, tmp_path):
    conn = connect(settings.db_path)
    conn.execute(
        "INSERT INTO scans (id, sha256, original_name, source_path, origin, import_mode)"
        " VALUES (1, 'h', 'a.png', 'a.png', 'watched', 'copy')"
    )
    conn.execute("INSERT INTO intake_steps (scan_id, step, status) VALUES (1, 'split', 'running')")
    conn.commit()
    app = create_app(settings, settings_path=tmp_path / "settings.json", watch=False)
    with TestClient(app):
        status = conn.execute("SELECT status FROM intake_steps").fetchone()[0]
    conn.close()
    assert status == "failed"


def test_a_duplicate_file_isnt_hashed_again_on_each_start(settings, inbox, monkeypatch):
    (inbox / "a.png").write_bytes(png_bytes())
    (inbox / "copy of a.png").write_bytes(png_bytes())
    first = make_watcher(settings)
    first.sweep([inbox])
    first.tick()
    first.tick()
    later = make_watcher(settings)
    monkeypatch.setattr(watcher_mod, "ingest", lambda *a, **k: pytest.fail("hashed again"))
    later.sweep([inbox])
    assert later.tick() == [] and later.tick() == []


def test_a_file_changed_in_place_at_the_same_size_is_read_again(settings, inbox):
    path = inbox / "a.png"
    path.write_bytes(png_bytes("white"))
    first = make_watcher(settings)
    first.sweep([inbox])
    first.tick()
    first.tick()
    path.write_bytes(png_bytes("black"))  # rescanned over the old file: same size, new time
    later = make_watcher(settings)
    later.sweep([inbox])
    later.tick()
    assert later.tick() == [path]
    assert len(scans(settings)) == 2


def test_move_mode_takes_a_file_it_couldnt_remove_again(settings, inbox, monkeypatch):
    settings.move_files = True
    path = inbox / "a.png"
    path.write_bytes(png_bytes())
    unlink = watcher_mod.Path.unlink
    monkeypatch.setattr(
        watcher_mod.Path,
        "unlink",
        lambda self, *a, **k: (
            (_ for _ in ()).throw(PermissionError("in use"))
            if self.name == "a.png"
            else unlink(self, *a, **k)
        ),
    )
    first = make_watcher(settings)
    first.sweep([inbox])
    first.tick()
    first.tick()
    assert path.exists()
    monkeypatch.undo()
    later = make_watcher(settings)
    later.sweep([inbox])
    later.tick()
    later.tick()
    assert not path.exists()  # seen before, but still there: removed now


def test_the_vision_queue_is_found_by_index(settings, inbox):
    from lindley.worker.pipeline import _LAST_VISION

    conn = connect(settings.db_path)
    plan = " ".join(r[3] for r in conn.execute(f"EXPLAIN QUERY PLAN {_LAST_VISION}"))
    conn.close()
    assert "idx_intake_steps_page" in plan
