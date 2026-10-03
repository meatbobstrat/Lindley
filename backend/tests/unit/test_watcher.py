import io
import time

import pytest
from fastapi.testclient import TestClient
from PIL import Image

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
    return FolderWatcher(settings, Pipeline(settings, StubOcr()), chat=None, **kw)


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
    monkeypatch.setattr(watcher_mod, "assemble", lambda *a: calls.append(a) or _Report())
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
    considered = documents_created = 0


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
    w.start()
    try:
        (inbox / "dropped.png").write_bytes(png_bytes("ivory"))
        deadline = time.monotonic() + 10
        while not scans(settings) and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        w.stop()
    assert scans(settings) == [("dropped.png", "watched", "read")]


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
        w = FolderWatcher(settings, Pipeline(settings, StubOcr(), vision), chat=None, settle_s=0)
        w.sweep([inbox])
        w.tick()
        w.tick()
    assert scans(settings) == [("a.png", "watched", "queued")]
    assert vision.calls == 0
