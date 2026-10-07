"""A look at this computer: its processor, memory and graphics cards, for suggesting a tier
(providers/tiers.py, suggest) in Setup and Settings.

Each part is looked up on its own, from what the system already keeps (Windows' registry and
kernel32; Linux's /proc and /sys; macOS's sysctl), and nothing is installed or downloaded for it.
A part that can't be found is None or left out, never an error. Built-in graphics share the
computer's memory, so only cards with memory of their own (1 GB or more) are counted.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from functools import cache
from pathlib import Path

log = logging.getLogger(__name__)

GB = 1024**3
CARD_MIN = 1 * GB  # less memory of its own than this: built-in graphics, or a virtual display
TIMEOUT_S = 5


@dataclass(frozen=True)
class Graphics:
    name: str
    memory: int  # bytes of its own


@dataclass(frozen=True)
class Computer:
    processor: str | None  # its name, e.g. "Intel(R) Core(TM) i5-9400 CPU @ 2.90GHz"
    threads: int  # what it can run at once (os.cpu_count)
    memory: int | None  # bytes
    graphics: tuple[Graphics, ...] = ()  # cards with memory of their own, the largest first

    @property
    def card(self) -> Graphics | None:
        return self.graphics[0] if self.graphics else None


def _try(what: str, look, default):
    try:
        return look()
    except Exception as e:  # a look at the computer never stops Lindley
        log.info("Couldn't find this computer's %s: %s", what, e)
        return default


@cache
def look() -> Computer:
    """This computer, looked at once: it doesn't change while Lindley runs."""
    if sys.platform == "win32":
        processor, memory, graphics = _win_processor, _win_memory, _win_graphics
    elif sys.platform == "darwin":
        processor, memory, graphics = _mac_processor, _mac_memory, lambda: []
    else:
        processor, memory, graphics = _linux_processor, _linux_memory, _linux_graphics
    cards = _try("graphics", graphics, [])
    return Computer(
        processor=_try("processor", processor, None),
        threads=os.cpu_count() or 1,
        memory=_try("memory", memory, None),
        graphics=tuple(sorted((g for g in cards if g.memory >= CARD_MIN), key=lambda g: -g.memory)),
    )


# ------------------------------------------------------------------ Windows

_CPU_KEY = r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
# The display adapters' class: one subkey (0000, 0001...) for each adapter's driver
_DISPLAY_KEY = r"SYSTEM\CurrentControlSet\Control\Class\{4d36e968-e325-11ce-bfc1-08002be10318}"


def _win_processor() -> str | None:
    import winreg

    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _CPU_KEY) as k:
        return str(winreg.QueryValueEx(k, "ProcessorNameString")[0]).strip() or None


def _win_memory() -> int:
    import ctypes

    class Status(ctypes.Structure):
        _fields_ = [
            ("dwLength", ctypes.c_ulong),
            ("dwMemoryLoad", ctypes.c_ulong),
            ("ullTotalPhys", ctypes.c_ulonglong),
            ("ullAvailPhys", ctypes.c_ulonglong),
            ("ullTotalPageFile", ctypes.c_ulonglong),
            ("ullAvailPageFile", ctypes.c_ulonglong),
            ("ullTotalVirtual", ctypes.c_ulonglong),
            ("ullAvailVirtual", ctypes.c_ulonglong),
            ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
        ]

    s = Status()
    s.dwLength = ctypes.sizeof(s)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s)):
        raise OSError("GlobalMemoryStatusEx failed")
    return int(s.ullTotalPhys)


def _win_graphics() -> list[Graphics]:
    """Each adapter with a driver, and its memory: `qwMemorySize`, which is right past 4 GB,
    where WMI's AdapterRAM stops. Built-in graphics and virtual displays have none."""
    import winreg

    out = []
    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _DISPLAY_KEY) as cls:
        i = 0
        while True:
            try:
                sub = winreg.EnumKey(cls, i)
            except OSError:
                break
            i += 1
            if not sub.isdigit():
                continue
            try:
                with winreg.OpenKey(cls, sub) as k:
                    name = str(winreg.QueryValueEx(k, "DriverDesc")[0]).strip()
                    memory = int(winreg.QueryValueEx(k, "HardwareInformation.qwMemorySize")[0])
            except (OSError, ValueError, TypeError):
                continue
            if name:
                out.append(Graphics(name, memory))
    return out


# ------------------------------------------------------------------ Linux


def _cpuinfo(text: str) -> str | None:
    """The processor's name from /proc/cpuinfo."""
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key.strip() in ("model name", "Model") and value.strip():
            return value.strip()
    return None


def _meminfo(text: str) -> int | None:
    """Bytes of memory, from /proc/meminfo's MemTotal (in kB)."""
    for line in text.splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return None


def _nvidia_smi(text: str) -> list[Graphics]:
    """Cards from `nvidia-smi --query-gpu=name,memory.total --format=csv,noheader,nounits`:
    a name and MiB a line."""
    out = []
    for line in text.splitlines():
        name, _, mib = line.rpartition(",")
        try:
            out.append(Graphics(name.strip(), int(float(mib)) * 1024**2))
        except ValueError:
            continue
    return out


def _linux_processor() -> str | None:
    return _cpuinfo(Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace"))


def _linux_memory() -> int | None:
    return _meminfo(Path("/proc/meminfo").read_text(encoding="utf-8", errors="replace"))


def _linux_graphics() -> list[Graphics]:
    out = []
    # AMD's cards say their memory in /sys (amdgpu)
    for vram in sorted(Path("/sys/class/drm").glob("card[0-9]*/device/mem_info_vram_total")):
        try:
            out.append(Graphics("AMD graphics card", int(vram.read_text().strip())))
        except (OSError, ValueError):
            continue
    # NVIDIA's, through its driver's own tool, when it's installed
    if smi := shutil.which("nvidia-smi"):
        r = subprocess.run(
            [smi, "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            check=False,
        )
        if r.returncode == 0:
            out += _nvidia_smi(r.stdout)
    return out


# ------------------------------------------------------------------ macOS


def _sysctl(name: str) -> str:
    r = subprocess.run(
        ["sysctl", "-n", name], capture_output=True, text=True, timeout=TIMEOUT_S, check=True
    )
    return r.stdout.strip()


def _mac_processor() -> str | None:
    return _sysctl("machdep.cpu.brand_string") or None


def _mac_memory() -> int:
    return int(_sysctl("hw.memsize"))
