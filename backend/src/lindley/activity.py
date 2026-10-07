"""What Lindley is doing with an AI right now, for the status bar: reading hard pages, sorting
pages or downloading a model for its own AI, how far it has got, and whether a person asked or
it runs on its own. And what came of the work a person asked for, so the app can say when it's
done.

One registry for the whole process, like the connections' throttles: the watcher and the work
a person asks for (lindley.worker.ai_work) both report here. Nothing is stored: after a restart
there's nothing in hand.
"""

from __future__ import annotations

import itertools
import threading
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Literal

FINISHED_KEPT = 20  # results kept for the app to pick up

Kind = Literal["read", "sort", "download"]

_lock = threading.Lock()
_ids = itertools.count(1)
_working: dict[int, dict] = {}
_finished: deque[dict] = deque(maxlen=FINISHED_KEPT)


@contextmanager
def doing(
    kind: Kind,
    of: int,
    *,
    asked: bool,
    connection: str | None = None,
    pages: int | None = None,
    quiet: bool = False,
    label: str | None = None,
) -> Iterator[Callable[[int, int | None], None]]:
    """While the block runs, the status bar shows this work; `step(done, of)` says how many
    are done, and of how many if that's changed. Reading counts pages; sorting counts the
    questions asked of the AI, and `pages` is how many pages it's sorting (0: not known).
    Downloading counts bytes, and `label` says what's downloading. `quiet`: not shown until a
    step says there's something to do (of > 0), for work that mostly turns out to need no AI."""
    key = next(_ids)
    entry = {
        "kind": kind,
        "done": 0,
        "of": of,
        "pages": of if pages is None else pages,
        "asked": asked,
        "connection": connection,
        "quiet": quiet,
        "label": label,
    }
    with _lock:
        _working[key] = entry

    def step(done: int, of: int | None = None) -> None:
        with _lock:
            entry["done"] = done
            if of is not None:
                entry["of"] = of

    try:
        yield step
    finally:
        with _lock:
            _working.pop(key, None)


def finished(kind: Kind, message: str, ok: bool = True) -> int:
    """What came of work a person asked for, in words. Returns its id: newer ones are higher."""
    with _lock:
        key = next(_ids)
        _finished.append({"id": key, "kind": kind, "message": message, "ok": ok})
        return key


def current() -> list[dict]:
    with _lock:
        return [
            {k: v for k, v in e.items() if k != "quiet"}
            for e in _working.values()
            if not (e["quiet"] and e["of"] == 0)
        ]


def recent() -> list[dict]:
    with _lock:
        return list(_finished)


def clear() -> None:
    """Forget everything (tests)."""
    with _lock:
        _working.clear()
        _finished.clear()
