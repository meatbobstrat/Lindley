"""Folder watcher: new scans dropped into a watched folder are imported, read and assembled.

watchdog reports new or changed files, and a sweep at start-up catches anything dropped while
Lindley was closed. A file is taken only once it has finished arriving: its size and time
unchanged for `stable_s` seconds, and its ending there (worker.intake.looks_complete), since a
scanner or a copy may still be writing it. A file that never finishes (empty, or kept locked)
stops holding things up after `stuck_s` seconds. One background thread does the work: the files
that have arrived are imported one at a time, then read a few at once. Once no new file has
arrived for `settle_s` seconds, the assembler runs once over the whole Inbox.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from lindley.assembler.auto import chat_on_its_own, read_on_its_own, sort_on_its_own
from lindley.assembler.relearn import relearn
from lindley.config import Settings
from lindley.db.database import connect
from lindley.duplicates import find_duplicates
from lindley.providers.base import ChatProvider
from lindley.worker.intake import (
    ImportResult,
    already_seen,
    import_file,
    is_supported,
    looks_complete,
    needs_reading,
)
from lindley.worker.pipeline import Pipeline, unfinished_scans, waiting_for_vision

log = logging.getLogger(__name__)

POLL_S = 1.0
SETTLE_S = 20.0
STABLE_S = 3.0  # a file unchanged this long has finished arriving, if its ending is there
STUCK_S = 60.0  # a file unchanged this long but still not ready no longer holds things up
MAX_TRIES = 3  # a file whose import raised is tried this many times, then left until it changes
BACKOFF_S = (30.0, 3600.0)  # waits after the assembler fails: the first, doubling to the most

# Held while a watcher works. A watcher replaced by new settings may still be finishing a long
# read; its successor waits, so the two never read the same file or page at once.
_WORK = threading.Lock()


@dataclass
class _Arrival:
    """A file noticed but not taken yet."""

    size: int = -1  # -1: not looked at yet
    mtime: int = -1
    changed: float = field(default_factory=time.monotonic)  # when it last changed
    warned: bool = False  # logged as stuck


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
        stable_s: float = STABLE_S,
        stuck_s: float = STUCK_S,
    ) -> None:
        self.settings = settings
        self.pipeline = pipeline or Pipeline.from_settings(settings)
        self.chat = chat if chat is not None else chat_on_its_own(settings)
        self.poll_s = poll_s
        self.settle_s = settle_s
        self.stable_s = stable_s
        self.stuck_s = stuck_s
        self._pending: dict[Path, _Arrival] = {}
        self._tries: dict[Path, int] = {}  # files whose import raised: how many times
        self._added: list[int] = []  # scans a person added with Add scans…, to read
        self._assemble_after = 0.0  # after a failure, the assembler waits until then
        self._backoff = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._observer = None
        self._conn: sqlite3.Connection | None = None
        # Sort the Inbox once things settle after starting, too: pages may be waiting for an AI
        # that may run on its own now, after a restart or a change of settings.
        self._last_new = time.monotonic()
        self._unassembled = True

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

    def stop(self, wait: bool = True) -> None:
        """Stop after the file or page in hand. `wait=False` returns at once (new settings: the
        next watcher waits for this one's work to finish before starting its own)."""
        self._stop.set()
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
        if self._thread and wait:
            self._thread.join(timeout=30)

    def _run(self, folders: list[Path]) -> None:
        try:
            # Whatever goes wrong picking up old work, new files are still watched for
            with _WORK:
                if not self._stop.is_set():
                    try:
                        self.resume()
                    except Exception:
                        log.exception("Picking up work cut off when Lindley closed failed")
            try:
                self.sweep(folders)
            except Exception:
                log.exception("Looking for files dropped in while Lindley was closed failed")
            while not self._stop.wait(self.poll_s):
                with _WORK:
                    if self._stop.is_set():
                        break
                    try:
                        self.tick()
                    except Exception:
                        log.exception("Folder watcher step failed")
        finally:
            if self._conn:
                self._conn.close()

    def resume(self) -> list[int]:
        """Read scans whose reading never finished: cut off when Lindley closed, or failed
        (Tesseract wasn't installed, say). Lindley's own copy is read, so this works in move
        mode too, where the original has gone. Then pages read before Lindley tried poorly read
        pages upside down are checked (Pipeline.check_upside_down), and those read before it
        tried mirror images (Pipeline.check_mirrored). Returns the scans picked up."""
        conn = self._db()
        scans = unfinished_scans(conn)
        read = self.pipeline.process_scans(conn, scans, stop=self._stop)
        for scan_id, status in read.items():
            if status:
                log.info("Picked up reading scan %d: %s", scan_id, status)
        if read:
            self._unassembled = True
        if self.pipeline.check_upside_down(conn, stop=self._stop)[1]:
            self._unassembled = True
        if self.pipeline.check_mirrored(conn, stop=self._stop)[1]:
            self._unassembled = True
        return scans

    # --------------------------------------------------------------- work

    def notice(self, path: Path) -> None:
        """A file appeared or changed. It's taken once it has finished arriving."""
        s = self.settings
        own = (s.library_dir.resolve(), s.quarantine_dir.resolve(), s.processing_dir.resolve())
        if not is_supported(path) or any(path.resolve().is_relative_to(d) for d in own):
            return
        with self._lock:
            self._pending.setdefault(path, _Arrival())

    def read_later(self, scan_ids: list[int]) -> None:
        """Scans a person added (Add scans… in the Inbox), imported already: they're read with
        the next files, and the Inbox is sorted once things settle."""
        with self._lock:
            self._added.extend(s for s in scan_ids if s not in self._added)

    def tick(self) -> list[Path]:
        """Take the files that have finished arriving, then assemble once things settle.

        The files are all imported first, then read a few at once (Pipeline.process_scans).
        Returns the files taken. Called by the background thread; tests call it directly.
        """
        conn = self._db()
        ready = self._arrived(conn)
        taken: list[tuple[Path, ImportResult]] = []
        for i, path in enumerate(ready):
            if self._stop.is_set():  # the next watcher's start-up sweep finds the rest
                del ready[i:]
                break
            try:
                taken.append((path, import_file(conn, self.settings, path, origin="watched")))
            except Exception:
                log.exception("Couldn't take %s", path)
                self._try_again(path)
        to_read = [r.scan_id for _, r in taken if needs_reading(conn, r)]
        with self._lock:
            added, self._added = [s for s in self._added if s not in to_read], []
        read = self.pipeline.process_scans(conn, to_read + added, stop=self._stop)
        if any(read.get(s) for s in added):
            self._unassembled = True
            self._last_new = time.monotonic()
        for path, r in taken:
            self._tries.pop(path, None)
            if r.scan_id in to_read:
                r.reading = read.get(r.scan_id)
                if r.reading is None:
                    # Stopped before it was read, or its reading raised (logged): the next
                    # start picks it up from Lindley's own copy (unfinished_scans).
                    continue
            log.info("%s: %s%s", path.name, r.status, f", {r.reading}" if r.reading else "")
            if r.status == "new" or r.reading:
                self._unassembled = True
            self._last_new = time.monotonic()
        # Hard pages that arrived while the vision model had to ask go now, if it may run on
        # its own (see lindley.assembler.auto); the rest wait in Needs AI for a person.
        try:
            if not self._stop.is_set() and (
                (run := read_on_its_own(conn, self.settings, self.pipeline)) and run.read
            ):
                log.info("Read %d hard page(s) with the vision model", run.read)
                self._unassembled = True
                self._last_new = time.monotonic()
            if ready and (waiting := waiting_for_vision(conn)):
                log.info("%d page(s) are waiting in Needs AI for the vision model", waiting)
        except Exception:
            log.exception("Reading hard pages with the vision model failed")
        now = time.monotonic()
        if (
            self._unassembled
            and not self._stop.is_set()
            and not self._arriving(now)
            and now - self._last_new >= self.settle_s
            and now >= self._assemble_after
        ):
            try:
                self._assemble(conn)
            except Exception:
                self._backoff = min(max(self._backoff * 2, BACKOFF_S[0]), BACKOFF_S[1])
                self._assemble_after = time.monotonic() + self._backoff
                log.exception("Assembling the Inbox failed; trying again in %.0fs", self._backoff)
            else:
                self._unassembled = False
                self._backoff = 0.0
        return ready

    def _arrived(self, conn: sqlite3.Connection) -> list[Path]:
        """The files that have finished arriving, taken off the list."""
        ready, now = [], time.monotonic()
        with self._lock:
            for path, seen in list(self._pending.items()):
                try:
                    st = path.stat()
                except OSError:
                    del self._pending[path]  # gone, or a folder
                    continue
                # Move mode never skips one: a file still there wasn't removed yet.
                if (
                    seen.size == -1
                    and not self.settings.move_files
                    and already_seen(conn, path, st)
                ):
                    del self._pending[path]
                    continue
                if (st.st_size, st.st_mtime_ns) != (seen.size, seen.mtime):
                    seen.size, seen.mtime, seen.changed = st.st_size, st.st_mtime_ns, now
                    continue
                if now - seen.changed < self.stable_s:
                    continue
                stuck = now - seen.changed >= self.stuck_s
                # A file whose ending never arrives is taken in the end, to fail and be
                # quarantined where a person will see it.
                if st.st_size > 0 and _readable(path) and (looks_complete(path) or stuck):
                    ready.append(path)
                    del self._pending[path]
                elif stuck and not seen.warned:
                    seen.warned = True
                    why = "it's empty" if st.st_size == 0 else "it can't be opened"
                    log.warning("Waiting for %s to change: %s", path, why)
        return ready

    def _arriving(self, now: float) -> bool:
        """Files still arriving: the assembler waits for them. Stuck ones don't count."""
        with self._lock:
            return any(now - seen.changed < self.stuck_s for seen in self._pending.values())

    def _try_again(self, path: Path) -> None:
        """A file whose import raised (the database was busy, say) is tried again, a few times."""
        with self._lock:
            tries = self._tries.get(path, 0) + 1
            if tries < MAX_TRIES:
                self._tries[path] = tries
                self._pending[path] = _Arrival()
            else:
                del self._tries[path]
                log.error("Gave up on %s after %d tries, until it changes", path, tries)

    def _assemble(self, conn: sqlite3.Connection) -> None:
        found = find_duplicates(conn).found  # first: copies of a page never share a document
        if found:
            log.info("%d possible duplicate(s) to look at under Duplicates", len(found))
        report = sort_on_its_own(conn, self.settings, self.chat)
        log.info(
            "Assembled %d Inbox pages: %d new documents",
            report.considered,
            report.documents_created,
        )
        try:  # people's answers so far may teach it to do better
            if (learnt := relearn(conn)) is not None:
                log.info(
                    "Learned from %d documents people vouched for: %s",
                    learnt.documents,
                    "now in use" if learnt.adopted else "no better, so not used",
                )
        except Exception:  # noqa: BLE001 - learning is a bonus; sorting goes on without it
            log.exception("Learning from people's answers failed")

    def _db(self) -> sqlite3.Connection:
        if self._conn is None:  # made on the thread that uses it
            self._conn = connect(self.settings.db_path)
        return self._conn


def _readable(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except OSError:
        return False
