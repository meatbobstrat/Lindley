"""A look at this computer: processor, memory and graphics cards, for suggesting a tier."""

import sys

import pytest

from lindley.localai import computer
from lindley.localai.computer import GB, Computer, Graphics


def test_linux_parts_are_read_from_proc():
    cpuinfo = "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Core(TM) i7\n"
    assert computer._cpuinfo(cpuinfo) == "Intel(R) Core(TM) i7"
    assert computer._cpuinfo("processor : 0\n") is None
    meminfo = "MemTotal:       16303404 kB\nMemFree:         1234 kB\n"
    assert computer._meminfo(meminfo) == 16303404 * 1024
    assert computer._meminfo("") is None


def test_nvidia_cards_are_read_from_its_tool():
    cards = computer._nvidia_smi("NVIDIA GeForce RTX 3060, 12288\nbroken line\n")
    assert cards == [Graphics("NVIDIA GeForce RTX 3060", 12 * GB)]


def test_only_cards_with_memory_of_their_own_count(monkeypatch):
    found = [Graphics("Virtual display", 0), Graphics("Small", 2 * GB), Graphics("Big", 8 * GB)]
    monkeypatch.setattr(computer, "_win_graphics", lambda: found)
    monkeypatch.setattr(computer, "_linux_graphics", lambda: found)
    monkeypatch.setattr(computer, "_mac_processor", lambda: None)
    computer.look.cache_clear()
    try:
        c = computer.look()
    finally:
        computer.look.cache_clear()
    if sys.platform == "darwin":
        assert c.graphics == ()
    else:
        assert [g.name for g in c.graphics] == ["Big", "Small"]
        assert c.card == Graphics("Big", 8 * GB)


def test_a_part_that_cant_be_found_is_left_out(monkeypatch):
    def fails():
        raise OSError("no such key")

    for name in ("_win_processor", "_win_memory", "_win_graphics", "_linux_processor"):
        monkeypatch.setattr(computer, name, fails)
    monkeypatch.setattr(computer, "_linux_memory", fails)
    monkeypatch.setattr(computer, "_linux_graphics", fails)
    monkeypatch.setattr(computer, "_mac_processor", fails)
    monkeypatch.setattr(computer, "_mac_memory", fails)
    computer.look.cache_clear()
    try:
        c = computer.look()
    finally:
        computer.look.cache_clear()
    assert c == Computer(processor=None, threads=c.threads, memory=None, graphics=())
    assert c.threads >= 1 and c.card is None


def test_this_computer_is_looked_at():
    c = computer.look()
    assert c.threads >= 1
    assert c.memory is None or c.memory > GB


@pytest.mark.skipif(sys.platform != "win32", reason="Windows' registry and kernel32")
def test_windows_says_its_processor_and_memory():
    assert computer._win_processor()
    assert computer._win_memory() > GB
