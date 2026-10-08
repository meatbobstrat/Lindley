"""Downloading Lindley's own AI: the engine and models a person chose (catalog.py).

Only ever because a person asked: Lindley never fetches anything on its own. One download runs
at a time, on a thread of its own, and the status bar shows it (lindley.activity). A file comes
down into `<name>.part`, so a download cut off (Cancel, a lost connection, Lindley closed) goes
on from where it stopped next time. It's checked against its SHA-256 before it's put in place:
one that doesn't match is deleted.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import shutil
import tarfile
import threading
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx2

from lindley import activity
from lindley.localai.catalog import MODELS, Engine, File, engine

log = logging.getLogger(__name__)

CHUNK = 1 << 20


class DownloadError(RuntimeError):
    pass


class Cancelled(DownloadError):
    pass


def _client(transport=None) -> httpx2.Client:
    # Hugging Face and GitHub send each file on from another address
    return httpx2.Client(
        follow_redirects=True,
        timeout=httpx2.Timeout(30, read=120),
        transport=transport,
        headers={"User-Agent": "Lindley"},
    )


def fetch(
    client: httpx2.Client,
    file: File,
    dest: Path,
    progress: Callable[[int], None] = lambda done: None,
    cancelled: threading.Event | None = None,
) -> None:
    """Download `file` to `dest`, going on from a `.part` left before. `progress` is told the
    bytes in hand. Raises Cancelled when `cancelled` is set, DownloadError when it fails."""
    if dest.is_file():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    have = part.stat().st_size if part.is_file() else 0
    if have > file.size:
        part.unlink()
        have = 0
    digest = hashlib.sha256()
    if have:
        with part.open("rb") as f:
            while chunk := f.read(CHUNK):
                digest.update(chunk)
    progress(have)
    headers = {"Range": f"bytes={have}-"} if have and have < file.size else {}
    try:
        if have < file.size:
            with client.stream("GET", file.url, headers=headers) as r:
                if r.status_code == 200 and have:  # the server sent it all again
                    have, digest = 0, hashlib.sha256()
                elif r.status_code not in (200, 206):
                    raise DownloadError(f"{file.name}: the server answered {r.status_code}")
                with part.open("ab" if have else "wb") as f:
                    for chunk in r.iter_bytes(CHUNK):
                        if cancelled is not None and cancelled.is_set():
                            raise Cancelled(f"{file.name}: cancelled")
                        f.write(chunk)
                        digest.update(chunk)
                        have += len(chunk)
                        progress(have)
    except httpx2.HTTPError as e:
        raise DownloadError(f"{file.name} stopped downloading: {e}") from e
    if have != file.size or digest.hexdigest() != file.sha256:
        part.unlink(missing_ok=True)
        raise DownloadError(
            f"{file.name} didn't come down as it should (its size or checksum is wrong), so it "
            "was deleted. Try again."
        )
    os.replace(part, dest)


def unpack(engine: Engine, root: Path, archive: Path) -> None:
    """Put the engine's zip (Windows) or .tar.gz (Ubuntu, Mac) in its folder, then remove it.
    A .tar.gz keeps its links between libraries and which files are programs; the data filter
    refuses anything that would land outside the folder."""
    into = engine.folder(root)
    tmp = into.with_name(into.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    if engine.file.name.endswith(".tar.gz"):
        with tarfile.open(archive) as t:
            t.extractall(tmp, filter="data")
    else:
        with zipfile.ZipFile(archive) as z:
            z.extractall(tmp)
    found = next(tmp.rglob(engine.program), None)
    if found is None:
        shutil.rmtree(tmp, ignore_errors=True)
        raise DownloadError(f"{engine.file.name} has no {engine.program} in it")
    shutil.rmtree(into, ignore_errors=True)
    os.replace(found.parent, into)
    shutil.rmtree(tmp, ignore_errors=True)
    archive.unlink(missing_ok=True)


def wanted(root: Path, model_ids: list[str]) -> list[tuple[str, File, Path]]:
    """What's still to download for these models, with the engine first if it's missing:
    (label, file, where it goes)."""
    out = []
    if (e := engine()) is not None and not e.installed(root):
        out.append(("Lindley's AI engine", e.file, root / "engine" / e.file.name))
    for mid in dict.fromkeys(model_ids):
        m = MODELS[mid]
        for f in m.files:
            if not (dest := m.folder(root) / f.name).is_file():
                out.append((m.label, f, dest))
    return out


def get(
    root: Path,
    model_ids: list[str],
    progress: Callable[[str, int, int], None] = lambda label, done, of: None,
    cancelled: threading.Event | None = None,
    transport=None,
) -> None:
    """Download what these models still need, and the engine if it's missing. `progress` is
    told (what, bytes done, bytes in all)."""
    todo = wanted(root, model_ids)
    total = sum(f.size for _, f, _ in todo)
    before = 0
    with _client(transport) as client:
        for label, f, dest in todo:
            fetch(
                client, f, dest, lambda d, b=before, x=label: progress(x, b + d, total), cancelled
            )
            before += f.size
            e = engine()
            if e is not None and f is e.file:
                unpack(e, root, dest)


class Downloads:
    """Downloads people asked for, one at a time, in order. `root` gives the folder in use now
    (ai.local). Each request has a cancel of its own: one cancelled stops, and asking again
    straight after starts afresh rather than being caught by that cancel."""

    def __init__(self, root: Callable[[], Path], transport=None) -> None:
        self._root = root
        self._transport = transport  # tests
        self._queue: queue.Queue[_Batch | None] = queue.Queue()
        self._lock = threading.Lock()
        self._batches: list[_Batch] = []  # queued or downloading, in order
        self._running: _Batch | None = None
        self._thread: threading.Thread | None = None
        self.idle = threading.Event()
        self.idle.set()

    def start(self, model_ids: list[str]) -> list[str]:
        """Queue these models for download. Returns those queued (not already)."""
        unknown = [m for m in model_ids if m not in MODELS]
        if unknown:
            raise KeyError(", ".join(unknown))
        with self._lock:
            # One cancelled but still stopping doesn't count: it's asked for again
            live = {m for b in self._batches if not b.cancelled.is_set() for m in b.models}
            new = [m for m in dict.fromkeys(model_ids) if m not in live]
            if new:
                batch = _Batch(new)
                self._batches.append(batch)
                self.idle.clear()
                self._queue.put(batch)
                if self._thread is None or not self._thread.is_alive():
                    self._thread = threading.Thread(
                        target=self._run, name="lindley-download", daemon=True
                    )
                    self._thread.start()
        return new

    def asked(self) -> list[str]:
        """The models queued or downloading: one cancelled counts until it has stopped, as its
        files may still be open."""
        with self._lock:
            return list(
                dict.fromkeys(
                    m
                    for b in self._batches
                    if b is self._running or not b.cancelled.is_set()
                    for m in b.models
                )
            )

    def cancel(self) -> None:
        """Stop what's downloading and drop the rest. What came down so far is kept for next
        time."""
        with self._lock:
            for b in self._batches:
                b.cancelled.set()

    def stop(self) -> None:
        self.cancel()
        self._queue.put(None)

    def _run(self) -> None:
        while (batch := self._queue.get()) is not None:
            with self._lock:
                self._running = batch
            try:
                if not batch.cancelled.is_set():
                    self._do(batch.models, batch.cancelled)
            finally:
                with self._lock:
                    self._running = None
                    self._batches.remove(batch)
                    if not self._batches:
                        self.idle.set()

    def _do(self, models: list[str], cancelled: threading.Event) -> None:
        names = " and ".join(MODELS[m].label for m in models)
        root = self._root()
        of = sum(f.size for _, f, _ in wanted(root, models))
        try:
            with activity.doing("download", of, asked=True, pages=0, label=names) as step:
                get(
                    root,
                    models,
                    lambda label, done, total: step(done, total),
                    cancelled,
                    self._transport,
                )
        except Cancelled:
            activity.finished("download", f"Stopped downloading {names}.", ok=False)
        except (DownloadError, OSError) as e:
            log.warning("Downloading %s failed: %s", names, e)
            activity.finished("download", f"Couldn't download {names}: {e}", ok=False)
        else:
            activity.finished("download", f"{names} downloaded, ready to use.")


@dataclass
class _Batch:
    """Models asked for together, and their cancel."""

    models: list[str]
    cancelled: threading.Event = field(default_factory=threading.Event)
