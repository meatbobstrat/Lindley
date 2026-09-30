"""Folder watcher: new scans dropped into a watched folder are imported, read and assembled.

watchdog reports new or changed files, and a sweep at start-up catches anything dropped while
Lindley was closed. A file is taken only once its size has stopped changing (a scanner or a copy
may still be writing it). One background thread does the work, one file at a time. Once no new
file has arrived for `settle_s` seconds, the assembler runs once over the whole Inbox.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from lindley.assembler import assemble
from lindley.config import Settings
from lindley.db.database import connect
from lindley.providers.base import ChatProvider, ProviderError
from lindley.providers.registry import get_provider
from lindley.worker.intake import ingest, is_supported
from lindley.worker.pipeline import Pipeline

log = logging.getLogger(__name__)

POLL_S = 1.0
SETTLE_S = 20.0


class _Handler(FileSystemEventHandler):
    def __init__(self, watcher: FolderWatcher) -> None:
        self.watcher = watcher

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory or event.event_type not in ("created", "modified", "moved"):
            return
        path = event.dest_path if event.event_type == "moved" else event.src_path
        self.watcher.notice(Path(str(path)))


class FolderWatcher:
    def __init__(
        self,
        settings: Settings,
        pipeline: Pipeline | None = None,
        chat: ChatProvider | None = None,
        *,
        poll_s: float = POLL_S,
        settle_s: float = SETTLE_S,
    ) -> None:
        self.settings = settings
        self.pipeline = pipeline or Pipeline.from_settings(settings)
        self.chat = chat if chat is not None else _chat_from(settings)
        self.poll_s = poll_s
        self.settle_s = settle_s
        self._pending: dict[Path, int] = {}  # path -> size at the last check (-1: not yet seen)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._observer = None
        self._conn: sqlite3.Connection | None = None
        self._last_new = 0.0
        self._unassembled = False

    # --------------------------------------------------------------- lifecycle

    def folders(self) -> list[Path]:
        found = [f for f in self.settings.watch_folders if f.is_dir()]
        for f in set(self.settings.watch_folders) - set(found):
            log.warning("Watched folder %s doesn't exist; skipping it", f)
        return found

    def start(self) -> None:
        folders = self.folders()
        tesseract = getattr(self.pipeline.tesseract, "is_available", lambda: True)
        if self.settings.ocr.engine != "vision" and not tesseract():
            log.warning("Tesseract isn't installed: new scans will be imported but not read")
        self._observer = Observer()
        for folder in folders:
            self._observer.schedule(_Handler(self), str(folder), recursive=True)
        self._observer.start()
        self._thread = threading.Thread(
            target=self._run, args=(folders,), name="lindley-watcher", daemon=True
        )
        self._thread.start()

    def sweep(self, folders: list[Path]) -> None:
        """Notice anything dropped in while Lindley was closed."""
        for folder in folders:
            for f in folder.rglob("*"):
                self.notice(f)

    def stop(self) -> None:
        self._stop.set()
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
        if self._thread:
            self._thread.join(timeout=30)

    def _run(self, folders: list[Path]) -> None:
        try:
            self.sweep(folders)
            while not self._stop.wait(self.poll_s):
                try:
                    self.tick()
                except Exception:
                    log.exception("Folder watcher step failed")
        finally:
            if self._conn:
                self._conn.close()

    # --------------------------------------------------------------- work

    def notice(self, path: Path) -> None:
        """A file appeared or changed. It's taken once its size stops changing."""
        own = (self.settings.library_dir.resolve(), self.settings.quarantine_dir.resolve())
        if not is_supported(path) or any(path.resolve().is_relative_to(d) for d in own):
            return
        with self._lock:
            self._pending.setdefault(path, -1)

    def tick(self) -> list[Path]:
        """Take the files that have finished arriving, then assemble once things settle.

        Returns the files taken. Called by the background thread; tests call it directly.
        """
        ready = []
        conn = self._db()
        with self._lock:
            for path, last in list(self._pending.items()):
                try:
                    size = path.stat().st_size
                except OSError:
                    del self._pending[path]  # gone, or a folder
                    continue
                if last == -1 and _already_read(conn, path, size):
                    del self._pending[path]
                    continue
                if size == last and size > 0 and _readable(path):
                    ready.append(path)
                    del self._pending[path]
                else:
                    self._pending[path] = size
        for path in ready:
            r = ingest(conn, self.settings, self.pipeline, path, origin="watched")
            log.info("%s: %s%s", path.name, r.status, f", {r.reading}" if r.reading else "")
            if r.status == "new" or r.reading:
                self._unassembled = True
            self._last_new = time.monotonic()
        with self._lock:
            waiting = bool(self._pending)
        if self._unassembled and not waiting and time.monotonic() - self._last_new >= self.settle_s:
            report = assemble(conn, self.settings.assembler, self.chat)
            log.info(
                "Assembled %d Inbox pages: %d new documents",
                report.considered,
                report.documents_created,
            )
            self._unassembled = False
        return ready

    def _db(self) -> sqlite3.Connection:
        if self._conn is None:  # made on the thread that uses it
            self._conn = connect(self.settings.db_path)
        return self._conn


def _already_read(conn: sqlite3.Connection, path: Path, size: int) -> bool:
    """Read before from this very path at this size, so there's no need to hash it again.

    Copy mode leaves originals in the watched folder; this keeps start-up sweeps cheap. Scans
    that failed aren't matched, so they're tried again.
    """
    row = conn.execute(
        "SELECT 1 FROM scans WHERE source_path = ? AND file_size = ? AND status = 'read'",
        (str(path.resolve()), size),
    ).fetchone()
    return row is not None


def _readable(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except OSError:
        return False


def _chat_from(settings: Settings) -> ChatProvider | None:
    if not settings.assembler.use_ai:
        return None
    try:
        return get_provider(settings.ai, settings.ai.chat_provider)
    except ProviderError:
        return None
