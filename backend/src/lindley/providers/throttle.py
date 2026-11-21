"""Throttling each AI connection: calls a minute, and calls at once.

Every call to an AI goes through its connection's Throttle, whether Lindley made it on its own
or a person asked for it. There is one Throttle per connection for the whole process, so the
watcher, the API and the scripts share it. A connection whose limits change gets a new one.

When the AI says it's busy, the call is tried again, twice, after as long as the AI asks (or a
few seconds), without holding a place meanwhile. Nothing else is tried again: a call that took
too long may still be running at the AI, and is charged for each time it's sent.

Each connection's provider is wrapped in a Guarded, which keeps to its Throttle and notes what
each call used, when the connector says (see `metered`).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from lindley.config import ProviderConfig
from lindley.providers.base import ChatMessage, ProviderError, Transcription, Usage

log = logging.getLogger(__name__)

WINDOW = 60.0  # per_minute counts calls started in the last minute
TRIES = 3  # a call the AI said it's busy for is sent at most this many times
BUSY_WAIT = (2.0, 4.0)  # seconds before each try again, when the AI doesn't say how long
MAX_WAIT = 60.0  # the longest wait the AI may ask for


class Throttle:
    def __init__(
        self,
        per_minute: int | None = None,
        at_once: int = 2,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        name: str = "",
    ) -> None:
        self.name = name or "an AI connection"  # for the log
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
            log.info(
                "Waiting %.0f s to keep %s to %d calls a minute", wait, self.name, self.per_minute
            )
            self.sleep(wait)

    @contextmanager
    def slot(self) -> Iterator[None]:
        """Hold one of the at_once places, having waited for a turn this minute."""
        with self._slots:
            self._wait_for_turn()
            yield

    def _wait_if_busy(self, e: ProviderError, tries: int) -> None:
        """Raise `e` unless the AI said it's busy and there are tries left; else wait."""
        if not e.busy or tries >= TRIES:
            if e.busy:
                log.warning("%s is still busy after %d tries: %s", self.name, tries, e)
            raise e
        wait = min(e.retry_after if e.retry_after is not None else BUSY_WAIT[tries - 1], MAX_WAIT)
        log.info("%s is busy: trying again in %.0f s (try %d of %d)", self.name, wait, tries, TRIES)
        self.sleep(wait)

    def call(self, fn: Callable, *args):
        for tries in range(1, TRIES + 1):
            try:
                with self.slot():
                    return fn(*args)
            except ProviderError as e:
                self._wait_if_busy(e, tries)
        raise AssertionError("unreachable")

    def stream(self, fn: Callable, *args) -> Iterator:
        """Like call, for an answer that comes in pieces: the place is held until the last. Once
        a piece has arrived, a busy AI isn't tried again: the answer would start over."""
        for tries in range(1, TRIES + 1):
            started = False
            try:
                with self.slot():
                    for piece in fn(*args):
                        started = True
                        yield piece
                    return
            except ProviderError as e:
                if started:
                    raise
                self._wait_if_busy(e, tries)


_throttles: dict[str, tuple[tuple, Throttle]] = {}
_throttles_lock = threading.Lock()


def throttle_for(name: str, config: ProviderConfig) -> Throttle:
    """The connection's Throttle, shared by every caller; a new one if its limits changed."""
    limits = (config.per_minute, config.at_once)
    with _throttles_lock:
        held = _throttles.get(name)
        if held is None or held[0] != limits:
            held = (limits, Throttle(config.per_minute, config.at_once, name=name))
            _throttles[name] = held
        return held[1]


class Meter:
    """What a provider's calls used, kept apart for each thread: scans read side by side each
    see only their own calls."""

    def __init__(self) -> None:
        self._here = threading.local()

    def add(self, usage: Usage) -> None:
        if (noted := getattr(self._here, "noted", None)) is not None:
            noted.append(usage)

    @contextmanager
    def watch(self) -> Iterator[list[Usage]]:
        """The calls made on this thread inside the `with`, each as it was reported."""
        outer = getattr(self._here, "noted", None)
        self._here.noted = noted = []
        try:
            yield noted
        finally:
            self._here.noted = outer
            if outer is not None:
                outer.extend(noted)


@contextmanager
def metered(provider: Any) -> Iterator[list[Usage]]:
    """`with metered(provider) as used:` makes `used` the list of what each call to the
    provider inside the `with` used, on this thread. Empty when the connector doesn't say, or
    the provider isn't a Guarded one."""
    meter = getattr(provider, "meter", None)
    if meter is None:
        yield []
        return
    with meter.watch() as used:
        yield used


class Guarded:
    """A provider whose every call keeps to its connection's throttle."""

    def __init__(self, inner, name: str, throttle: Throttle) -> None:
        self.inner = inner
        self.name = name
        self.throttle = throttle
        self.meter = Meter()
        if hasattr(inner, "on_usage"):
            inner.on_usage = self.meter.add

    @property
    def model(self) -> str | None:
        return getattr(self.inner, "model", None)

    def _call(self, what: str, fn: Callable, *args):
        """A call through the throttle, with one line in the debug log: which connection and
        model, how long it took, what it used, and how it ended. Never what was sent or said."""
        started = time.monotonic()
        with self.meter.watch() as used:
            try:
                result = self.throttle.call(fn, *args)
            except Exception as e:
                self._noted(what, started, used, f"failed ({type(e).__name__})")
                raise
        self._noted(what, started, used, "ok")
        return result

    def _noted(self, what: str, started: float, used: list[Usage], how: str) -> None:
        if not log.isEnabledFor(logging.DEBUG):
            return
        tokens = (
            "; tokens in/out/cached: "
            f"{sum(u.input_tokens for u in used)}/{sum(u.output_tokens for u in used)}"
            f"/{sum(u.cache_read_tokens for u in used)}"
            if used
            else ""
        )
        log.debug(
            "AI call %s on %s (%s): %s in %.2f s%s",
            what,
            self.name,
            self.model or "no model named",
            how,
            time.monotonic() - started,
            tokens,
        )

    def _streamed(self, stream: Iterator[str]) -> Iterator[str]:
        started, pieces, how = time.monotonic(), 0, "ended"
        try:
            for piece in stream:
                pieces += 1
                yield piece
        except GeneratorExit:
            how = "stopped by the reader"
            raise
        except Exception as e:
            how = f"failed ({type(e).__name__})"
            raise
        finally:
            log.debug(
                "AI call chat_stream on %s (%s): %s after %d piece(s), %.2f s",
                self.name,
                self.model or "no model named",
                how,
                pieces,
                time.monotonic() - started,
            )

    def chat(self, messages: list[ChatMessage]) -> str:
        return self._call("chat", self.inner.chat, messages)

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        return self._streamed(self.throttle.stream(self.inner.chat_stream, messages))

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        return self._call("transcribe", self.inner.transcribe, image, hints)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self._call("embed", self.inner.embed, texts)

    def p_yes(self, system: str, question: str) -> float:
        return self._call("p_yes", self.inner.p_yes, system, question)

    def check(self) -> str:
        return self._call("check", self.inner.check)
