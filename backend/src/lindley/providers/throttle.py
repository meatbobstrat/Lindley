"""Throttling each AI connection: calls a minute, calls at once, and waiting when it's busy.

Every call to an AI goes through its connection's Throttle, whether Lindley made it on its own
or a person asked for it. There is one Throttle per connection for the whole process, so the
watcher, the API and the scripts share it. A connection whose limits change gets a new one.

When the AI says it's busy (HTTP 429, 503 or 529), Lindley waits as long as it asks (at most a
minute) and tries again, at most twice. That's the only time a call is repeated: one that
failed for any other reason is not.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from lindley.config import ProviderConfig
from lindley.providers.base import ChatMessage, ProviderBusy, Transcription

log = logging.getLogger(__name__)

BUSY_TRIES = 2  # tries again after "busy", at most
BUSY_WAIT = 5.0  # seconds, when the AI doesn't say how long
BUSY_WAIT_MAX = 60.0
WINDOW = 60.0  # per_minute counts calls started in the last minute


class Throttle:
    def __init__(
        self,
        per_minute: int | None = None,
        at_once: int = 2,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.per_minute = per_minute
        self.at_once = at_once
        self.clock, self.sleep = clock, sleep
        self._slots = threading.BoundedSemaphore(at_once)
        self._started: deque[float] = deque()
        self._lock = threading.Lock()

    def _wait_for_turn(self) -> None:
        """Wait until starting a call keeps to per_minute."""
        while self.per_minute:
            with self._lock:
                now = self.clock()
                while self._started and now - self._started[0] >= WINDOW:
                    self._started.popleft()
                if len(self._started) < self.per_minute:
                    self._started.append(now)
                    return
                wait = WINDOW - (now - self._started[0])
            log.info("Waiting %.0f s to keep to %d AI calls a minute", wait, self.per_minute)
            self.sleep(wait)

    @contextmanager
    def slot(self) -> Iterator[None]:
        """Hold one of the at_once places, having waited for a turn this minute."""
        with self._slots:
            self._wait_for_turn()
            yield

    def _busy_wait(self, e: ProviderBusy, tries: int) -> None:
        wait = min(e.retry_after or BUSY_WAIT, BUSY_WAIT_MAX)
        log.info(
            "The AI is busy (%s); trying again in %.0f s (%d of %d)", e, wait, tries, BUSY_TRIES
        )
        self.sleep(wait)

    def call(self, fn: Callable, *args):
        for tries in range(BUSY_TRIES + 1):
            try:
                with self.slot():
                    return fn(*args)
            except ProviderBusy as e:
                if tries == BUSY_TRIES:
                    raise
                self._busy_wait(e, tries + 1)
        raise AssertionError("unreachable")

    def stream(self, fn: Callable, *args) -> Iterator:
        """Like call, for an answer that comes in pieces. Once a piece has arrived, a busy reply
        isn't tried again: the person has already seen part of the answer."""
        for tries in range(BUSY_TRIES + 1):
            started = False
            try:
                with self.slot():
                    for piece in fn(*args):
                        started = True
                        yield piece
                return
            except ProviderBusy as e:
                if started or tries == BUSY_TRIES:
                    raise
                self._busy_wait(e, tries + 1)


_throttles: dict[str, tuple[tuple, Throttle]] = {}
_throttles_lock = threading.Lock()


def throttle_for(name: str, config: ProviderConfig) -> Throttle:
    """The connection's Throttle, shared by every caller; a new one if its limits changed."""
    limits = (config.per_minute, config.at_once)
    with _throttles_lock:
        held = _throttles.get(name)
        if held is None or held[0] != limits:
            held = (limits, Throttle(config.per_minute, config.at_once))
            _throttles[name] = held
        return held[1]


class Guarded:
    """A provider whose every call keeps to its connection's throttle."""

    def __init__(self, inner, name: str, throttle: Throttle) -> None:
        self.inner = inner
        self.name = name
        self.throttle = throttle

    @property
    def model(self) -> str | None:
        return getattr(self.inner, "model", None)

    def chat(self, messages: list[ChatMessage]) -> str:
        return self.throttle.call(self.inner.chat, messages)

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        return self.throttle.stream(self.inner.chat_stream, messages)

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        return self.throttle.call(self.inner.transcribe, image, hints)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self.throttle.call(self.inner.embed, texts)

    def check(self) -> str:
        return self.throttle.call(self.inner.check)
