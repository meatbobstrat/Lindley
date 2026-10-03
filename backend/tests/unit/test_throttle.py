"""Throttling an AI connection: calls a minute, calls at once, and waiting when it's busy."""

import threading
import time

import pytest

from lindley.config import AiSettings, JobConfig, ProviderConfig
from lindley.providers.base import ChatMessage, ProviderBusy, ProviderError
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


def flaky(*errors):
    """A call that raises these errors in turn, then answers "ok"."""
    left = list(errors)

    def fn():
        if left:
            raise left.pop(0)
        return "ok"

    return fn


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


def test_busy_waits_as_asked_and_tries_again_twice():
    clock = Clock()
    t = Throttle(clock=clock, sleep=clock.sleep)
    assert t.call(flaky(ProviderBusy("busy", 3), ProviderBusy("busy", None))) == "ok"
    assert clock.slept == [3, 5]  # as asked; 5 s when it doesn't say
    with pytest.raises(ProviderBusy):
        t.call(flaky(*[ProviderBusy("busy", 500)] * 3))
    assert clock.slept[2:] == [60, 60]  # never more than a minute, and only twice


def test_a_failed_call_is_not_repeated():
    clock = Clock()
    t = Throttle(clock=clock, sleep=clock.sleep)
    with pytest.raises(ProviderError):
        t.call(flaky(ProviderError("refused the key")))
    assert clock.slept == []


def test_a_stream_is_tried_again_only_before_it_starts():
    clock = Clock()
    t = Throttle(clock=clock, sleep=clock.sleep)
    tries = []

    def stream(fail_after):
        tries.append(fail_after)
        if fail_after == 0:
            raise ProviderBusy("busy", 1)
        yield "Dear"
        raise ProviderBusy("busy", 1)

    attempts = iter([0, None])

    def first_busy_then_fine():
        if next(attempts) == 0:
            raise ProviderBusy("busy", 1)
        yield from ["Dear", "Sister"]

    assert list(t.stream(first_busy_then_fine)) == ["Dear", "Sister"]
    with pytest.raises(ProviderBusy):
        list(t.stream(stream, 1))
    assert tries == [1]  # part of the answer had arrived, so not tried again


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
