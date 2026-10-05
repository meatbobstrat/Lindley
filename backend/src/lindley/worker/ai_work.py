"""AI work a person asked for, done in the background: reading hard pages, and sorting pages.

A call to an AI on a laptop can take minutes, so a person's request only queues the work and
answers at once; they can keep working meanwhile. One job runs at a time, in order, on a thread
of its own with its own database connection. The status bar shows it as it runs, and what came
of it when it's done (lindley.activity).

Pages already queued or being read aren't queued again, so a second press sends nothing more.
Work cut off when Lindley closes isn't lost: its pages still wait in Needs AI (a vision call cut
off part way goes back in the queue, recover_interrupted).
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from lindley import activity
from lindley.assembler import assemble
from lindley.assembler.auto import sort_on_its_own
from lindley.config import Settings
from lindley.db.database import connect
from lindley.providers import allowance
from lindley.providers.base import ProviderError
from lindley.providers.registry import connectors, get_provider
from lindley.providers.throttle import metered
from lindley.worker.pipeline import Pipeline

log = logging.getLogger(__name__)


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def connection_label(settings: Settings, job: str) -> str | None:
    """The name people see for the connection that does a job."""
    name = settings.ai.connection_for(job)
    cfg = allowance.provider_config(settings, name)
    if cfg is None:
        return None
    connector = connectors().get(cfg.type)
    return cfg.label or (connector.info.label if connector else cfg.type)


def sort_as_asked(conn: sqlite3.Connection, settings: Settings, page_ids: set[int]) -> dict:
    """Send these pages to the sorting AI at once, as a person asked: asking is the OK,
    whatever the connection's `allow`, and the calls are recorded as ones a person asked for.
    Raises ProviderError when there's no AI to sort with."""
    name = settings.ai.connection_for("assemble")
    if not name:
        raise ProviderError("No AI is set up to sort pages. Choose one in Settings.")
    chat = get_provider(settings.ai, "assemble")
    with metered(chat) as used:
        report = assemble(conn, settings.assembler, chat, asked=page_ids)
    if report.ai_calls or used:
        with conn:
            allowance.record(conn, name, "assemble", False, count=report.ai_calls, used=used)
    return {
        "ai_calls": report.ai_calls,
        "reused": report.ai_reused,
        "rejected": report.ai_rejected,
        "documents_created": report.documents_created,
        "pages_added": report.pages_added,
        "hints": report.hints,
    }


@dataclass
class Job:
    kind: activity.Kind
    pages: list[int]
    done: threading.Event = field(default_factory=threading.Event)


class AiWork:
    """The queue of AI work people asked for. `get_settings` gives the settings in use now, so
    a job started after they change uses the new ones."""

    def __init__(self, get_settings: Callable[[], Settings]) -> None:
        self._settings = get_settings
        self._queue: queue.Queue[Job | None] = queue.Queue()
        self._lock = threading.Lock()
        self._held: dict[int, Job] = {}  # each page queued or in hand, and its job
        self._jobs: list[Job] = []  # queued or running, in order
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------ asking

    def read(self, page_ids: list[int]) -> tuple[list[int], list[int]]:
        """Queue these pages for the vision model. Returns (queued, already in hand)."""
        return self._add("read", page_ids)

    def sort(self, page_ids: list[int]) -> tuple[list[int], list[int]]:
        """Queue these pages for the sorting AI, as one question. Returns (queued, already)."""
        return self._add("sort", page_ids)

    def _add(self, kind: activity.Kind, page_ids: list[int]) -> tuple[list[int], list[int]]:
        with self._lock:
            already = [p for p in page_ids if p in self._held]
            new = [p for p in dict.fromkeys(page_ids) if p not in self._held]
            if new:
                job = Job(kind, new)
                self._held.update(dict.fromkeys(new, job))
                self._jobs.append(job)
                self._queue.put(job)
                self._start()
        return new, already

    def pages(self) -> set[int]:
        """Pages queued or in hand."""
        with self._lock:
            return set(self._held)

    def waiting(self) -> list[dict]:
        """Jobs queued behind the one running, for the status bar."""
        with self._lock:
            return [{"kind": j.kind, "of": len(j.pages)} for j in self._jobs[1:]]

    def wait_idle(self, timeout: float = 10) -> bool:
        """Wait for every job queued so far to finish (tests)."""
        with self._lock:
            jobs = list(self._jobs)
        return all(j.done.wait(timeout) for j in jobs)

    def stop(self) -> None:
        """Stop after the job in hand; the rest of the queue is dropped (its pages still wait
        in Needs AI)."""
        self._queue.put(None)

    # ------------------------------------------------------------ the work

    def _start(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._run, name="lindley-ai", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while (job := self._queue.get()) is not None:
            try:
                self._do(job)
            finally:
                with self._lock:
                    for p in job.pages:
                        self._held.pop(p, None)
                    self._jobs.remove(job)
                job.done.set()

    def _do(self, job: Job) -> None:
        settings = self._settings()
        conn = connect(settings.db_path)
        try:
            message, ok = (self._read if job.kind == "read" else self._sort)(conn, settings, job)
            activity.finished(job.kind, message, ok)
        except Exception as e:  # noqa: BLE001 - said to the person who asked, and logged
            log.exception("AI work a person asked for failed (%s)", job.kind)
            activity.finished(job.kind, f"The AI couldn't {job.kind} them: {e}", ok=False)
        finally:
            conn.close()

    def _read(self, conn: sqlite3.Connection, settings: Settings, job: Job) -> tuple[str, bool]:
        pipeline = Pipeline.from_settings(settings)
        if pipeline.vision is None:
            return "No AI is set up to read hard pages any more, so nothing was sent.", False
        label = connection_label(settings, "vision")
        log.info("Reading %s with %s, as a person asked", _plural(len(job.pages), "page"), label)
        with activity.doing("read", len(job.pages), asked=True, connection=label) as step:
            run = pipeline.read_waiting(conn, job.pages, retry_failed=True, progress=step)
        if run.read:
            sort_on_its_own(conn, settings)  # new text: the rules may sort the pages now
        if not run.read and run.failed:
            return f"The AI didn’t manage {_plural(run.failed, 'page')}: {run.error}", False
        out = f"The AI read {_plural(run.read, 'page')}"
        if run.failed:
            out += f"; {run.failed} failed ({run.error})"
        if run.stopped:
            out += f". It stopped: {run.stopped}"
        return out + ". Check its work: those pages may need your review.", True

    def _sort(self, conn: sqlite3.Connection, settings: Settings, job: Job) -> tuple[str, bool]:
        label = connection_label(settings, "assemble")
        log.info("Sorting %s with %s, as a person asked", _plural(len(job.pages), "page"), label)
        with activity.doing("sort", len(job.pages), asked=True, connection=label):
            r = sort_as_asked(conn, settings, set(job.pages))
        return (
            f"The AI sorted the pages: {_plural(r['documents_created'], 'new document')} and"
            f" {_plural(r['pages_added'], 'page')} added to documents. Check its work under"
            " In progress."
        ), True
