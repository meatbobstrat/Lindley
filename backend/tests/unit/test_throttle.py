"""Throttling an AI connection: calls a minute, and calls at once."""

import threading
import time

import pytest

from lindley.config import AiSettings, JobConfig, ProviderConfig
from lindley.providers.base import ChatMessage, ProviderError
from lindley.providers.registry import get_provider
from lindley.providers.throttle import Guarded, Throttle, throttle_for


class Clock:
    """Time that only moves when someone sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, s: float) -> None:
        self.slept.append(s)
        self.now += s


def test_calls_a_minute():
    clock = Clock()
    t = Throttle(per_minute=2, clock=clock, sleep=clock.sleep)
    for _ in range(2):
        t.call(lambda: None)
    assert clock.slept == []
    clock.now += 20
    t.call(lambda: None)  # the third this minute waits for the first two to be a minute old
    assert clock.slept == [40]
    t.call(lambda: None)
    assert clock.slept == [40]
    t.call(lambda: None)  # the third since the wait
    assert clock.slept == [40, 60]


def test_no_limit_a_minute():
    clock = Clock()
    t = Throttle(per_minute=None, clock=clock, sleep=clock.sleep)
    for _ in range(100):
        t.call(lambda: None)
    assert clock.slept == []


def test_calls_at_once():
    t = Throttle(at_once=2)
    lock, running, most = threading.Lock(), [0], [0]

    def work():
        with lock:
            running[0] += 1
            most[0] = max(most[0], running[0])
        time.sleep(0.02)
        with lock:
            running[0] -= 1

    threads = [threading.Thread(target=t.call, args=(work,)) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert most[0] == 2


def test_a_failed_call_is_not_repeated_and_frees_its_place():
    clock = Clock()
    t = Throttle(at_once=1, clock=clock, sleep=clock.sleep)
    calls = []

    def refused():
        calls.append(1)
        raise ProviderError("refused the key")

    with pytest.raises(ProviderError):
        t.call(refused)
    assert calls == [1] and clock.slept == []
    assert t.call(lambda: "ok") == "ok"


def busy(after=None):
    return ProviderError("busy (429)", busy=True, retry_after=after)


def test_a_busy_ai_is_tried_again_twice_after_as_long_as_it_asks():
    clock = Clock()
    t = Throttle(at_once=1, clock=clock, sleep=clock.sleep)
    replies: list = [busy(7), busy(), "ok"]

    def call():
        r = replies.pop(0)
        if isinstance(r, Exception):
            assert not t._slots.acquire(blocking=False)  # its place is held while it's sent
            raise r
        return r

    assert t.call(call) == "ok"
    assert clock.slept == [7, 4.0]
    replies[:] = [busy(), busy(), busy(), "ok"]
    with pytest.raises(ProviderError, match="busy"):
        t.call(call)
    assert replies == ["ok"]  # three tries, then it gives up
    replies[:] = [busy(3600), "ok"]
    clock.slept.clear()
    assert t.call(call) == "ok" and clock.slept == [60.0]  # never more than a minute


def test_a_call_that_took_too_long_is_not_tried_again():
    clock = Clock()
    t = Throttle(clock=clock, sleep=clock.sleep)
    calls = []

    def slow():
        calls.append(1)
        raise ProviderError("The AI took too long to answer")

    with pytest.raises(ProviderError):
        t.call(slow)
    assert calls == [1] and clock.slept == []


def test_a_stream_is_tried_again_only_before_its_first_piece():
    clock = Clock()
    t = Throttle(clock=clock, sleep=clock.sleep)
    tries = []

    def answer():
        tries.append(1)
        if len(tries) == 1:
            raise busy()
        yield "Dear"
        raise busy()

    stream = t.stream(answer)
    assert next(stream) == "Dear"
    with pytest.raises(ProviderError):
        next(stream)
    assert len(tries) == 2 and clock.slept == [2.0]


def test_a_stream_holds_its_place_until_the_last_piece():
    t = Throttle(at_once=1)
    stream = t.stream(lambda: iter(["Dear", "Sister"]))
    assert next(stream) == "Dear"
    assert not t._slots.acquire(blocking=False)  # still held
    assert list(stream) == ["Sister"]
    assert t._slots.acquire(blocking=False)


def test_one_throttle_per_connection_until_its_limits_change():
    cfg = ProviderConfig(type="fake", per_minute=10)
    a = throttle_for("t-shared", cfg)
    assert throttle_for("t-shared", cfg) is a
    assert throttle_for("t-other", cfg) is not a
    cfg.at_once = 4
    b = throttle_for("t-shared", cfg)
    assert b is not a and b.at_once == 4


def test_providers_for_a_job_are_throttled():
    ai = AiSettings(
        providers={"f": ProviderConfig(type="fake")}, jobs={"chat": JobConfig(connection="f")}
    )
    p = get_provider(ai, "chat")
    assert isinstance(p, Guarded) and p.name == "f" and p.model == "fake"
    assert p.chat([ChatMessage("user", "hi")]) == "[fake] hi"
    assert "".join(p.chat_stream([ChatMessage("user", "a b")])) == "[fake]ab"
    assert p.transcribe(b"1234").text == "<4 bytes transcribed>"
    assert len(p.embed(["x"])[0]) == 8
