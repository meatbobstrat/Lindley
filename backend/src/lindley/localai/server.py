"""Running Lindley's own AI: one llama-server, in router mode, on this computer only.

The server loads each model when it's first asked for, in a process of its own, and keeps up to
two in memory. It's started when a job first needs it, not with Lindley, and stopped when
Lindley stops. It runs offline (`--offline`) on 127.0.0.1, at a port free when it starts, and
only knows the models that are downloaded: its --models-preset file is written each time it
starts, so a model downloaded since starts it again.

On Windows it's put in a job that Windows ends when Lindley ends, however Lindley ends (a crash,
or uvicorn's --reload), so it's never left running on its own.
"""

from __future__ import annotations

import logging
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx2

from lindley.config import LocalAiSettings
from lindley.localai.catalog import MODELS, Model, engine
from lindley.providers.base import ProviderError

log = logging.getLogger(__name__)

HOST = "127.0.0.1"
MODELS_MAX = 2  # models kept loaded at once
START_S = 60  # seconds to wait for it to start (models load later, when asked for)

NOT_DOWNLOADED = "Lindley's own AI isn't downloaded yet. Download it in Settings › AI."


def preset(local: LocalAiSettings, models: list[Model]) -> str:
    """The server's --models-preset file: one section per model, its files and settings."""
    root = local.folder()
    out = []
    for m in models:
        lines = [f"[{m.id}]", f"model = {(m.folder(root) / m.files[0].name).resolve()}"]
        if len(m.files) > 1:
            lines.append(f"mmproj = {(m.folder(root) / m.files[1].name).resolve()}")
        lines.append(f"ctx-size = {m.context}")
        lines.append("parallel = 1")  # all its context for one question at a time
        if local.device:
            lines.append(f"device = {local.device}")
        if local.device == "none":
            lines.append("n-gpu-layers = 0")
        lines += [f"{k} = {v}" for k, v in m.preset.items()]
        out.append("\n".join(lines))
    return "\n\n".join(out) + "\n"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


class LocalServer:
    """One llama-server for these settings. `command`: what to run in its place (tests)."""

    def __init__(self, local: LocalAiSettings, command: list[str] | None = None) -> None:
        self.local = local
        self._command = command
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self._job = None  # Windows: the job that ends the server with Lindley
        self._models: set[str] = set()  # the models the running server knows
        self.port: int | None = None

    def program(self) -> Path | None:
        if self.local.server_path:
            return self.local.server_path
        e = engine()
        return e.path(self.local.folder()) if e else None

    def installed(self) -> list[Model]:
        root = self.local.folder()
        return [m for m in MODELS.values() if m.installed(root)]

    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def url(self, model: str) -> str:
        """The server's OpenAI-compatible address, started (or started again, to know a model
        downloaded since) if need be. Raises ProviderError when it can't run this model."""
        with self._lock:
            if self.running() and model in self._models:
                return f"http://{HOST}:{self.port}/v1"
            models = self.installed()
            if model not in {m.id for m in models}:
                label = MODELS[model].label if model in MODELS else model
                raise ProviderError(
                    f"{label} isn't downloaded yet. Download it in Settings › AI."
                    if model in MODELS
                    else f"Lindley's own AI has no model called {label!r}"
                )
            self._stop()
            self._start(models)
            return f"http://{HOST}:{self.port}/v1"

    def _start(self, models: list[Model]) -> None:
        root = self.local.folder()
        if self._command is None and not ((p := self.program()) and p.is_file()):
            raise ProviderError(NOT_DOWNLOADED)
        root.mkdir(parents=True, exist_ok=True)
        ini = root / "server.ini"
        ini.write_text(preset(self.local, models), encoding="utf-8")
        self.port = _free_port()
        args = [
            "--host",
            HOST,
            "--port",
            str(self.port),
            "--offline",
            "--no-webui",
            "--models-preset",
            str(ini),
            "--models-max",
            str(MODELS_MAX),
        ]
        command = [*(self._command or [str(self.program())]), *args]
        log.info("Starting Lindley's own AI: %s", " ".join(command))
        with (root / "server.log").open("wb") as out:
            self._proc = subprocess.Popen(
                command,
                stdout=out,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        self._job = _end_with_us(self._proc)
        self._models = {m.id for m in models}
        deadline = time.monotonic() + START_S
        with httpx2.Client(timeout=2) as client:
            while time.monotonic() < deadline:
                if self._proc.poll() is not None:
                    raise ProviderError(f"Lindley's own AI stopped as it started: {self._said()}")
                try:
                    if client.get(f"http://{HOST}:{self.port}/health").status_code == 200:
                        return
                except httpx2.HTTPError:
                    pass
                time.sleep(0.2)
        self._stop()
        raise ProviderError(f"Lindley's own AI didn't start in {START_S} seconds")

    def _said(self) -> str:
        """The last thing the server wrote, for an error."""
        try:
            lines = (self.local.folder() / "server.log").read_text("utf-8", "replace").split("\n")
        except OSError:
            return "it said nothing"
        return next((x.strip() for x in reversed(lines) if x.strip()), "it said nothing")

    def stop(self) -> None:
        with self._lock:
            self._stop()

    def _stop(self) -> None:
        if self._proc is not None:
            if self._proc.poll() is None:
                self._proc.terminate()
                try:
                    self._proc.wait(10)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
            log.info("Stopped Lindley's own AI")
        _close(self._job)  # and any model process left
        self._proc, self._job, self._models = None, None, set()

    def peak_memory(self) -> int | None:
        """The most memory the server and its models have held, in bytes (Windows;
        None elsewhere, or when it's not running)."""
        return _peak(self._job)


_current: LocalServer | None = None
_current_lock = threading.Lock()


def use(local: LocalAiSettings) -> LocalServer:
    """The server for these settings, one for the whole process. Settings changed since stop
    the one running; the new one starts when it's needed."""
    global _current
    with _current_lock:
        if _current is None or _current.local != local:
            if _current is not None:
                _current.stop()
            _current = LocalServer(local)
        return _current


def stop() -> None:
    with _current_lock:
        if _current is not None:
            _current.stop()


# ------------------------------------------------------------------ Windows jobs

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.CreateJobObjectW.restype = wintypes.HANDLE
    _k32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
    _k32.SetInformationJobObject.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    _k32.QueryInformationJobObject.argtypes = (
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
    )
    _k32.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
    _k32.CloseHandle.argtypes = (wintypes.HANDLE,)

    class _Basic(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _Extended(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _Basic),
            ("IoInfo", ctypes.c_ulonglong * 6),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    _EXTENDED_INFO = 9  # JobObjectExtendedLimitInformation
    _KILL_ON_JOB_CLOSE = 0x2000

    def _end_with_us(proc: subprocess.Popen):
        """A job holding the server (and the model processes it starts), ended by Windows when
        its last handle closes: when Lindley stops, or ends any other way."""
        job = _k32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = _Extended()
        info.BasicLimitInformation.LimitFlags = _KILL_ON_JOB_CLOSE
        _k32.SetInformationJobObject(job, _EXTENDED_INFO, ctypes.byref(info), ctypes.sizeof(info))
        if not _k32.AssignProcessToJobObject(job, int(proc._handle)):  # type: ignore[attr-defined]
            _k32.CloseHandle(job)
            return None
        return job

    def _close(job) -> None:
        if job:
            _k32.CloseHandle(job)

    class _Pids(ctypes.Structure):
        _fields_ = [
            ("NumberOfAssignedProcesses", wintypes.DWORD),
            ("NumberOfProcessIdsInList", wintypes.DWORD),
            ("ProcessIdList", ctypes.c_size_t * 64),
        ]

    class _Counters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    _k32.OpenProcess.restype = wintypes.HANDLE
    _k32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    _k32.K32GetProcessMemoryInfo.argtypes = (wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD)
    _PIDS_INFO = 3  # JobObjectBasicProcessIdList
    _QUERY = 0x1000  # PROCESS_QUERY_LIMITED_INFORMATION

    def _peak(job) -> int | None:
        """Each of the job's processes' most memory in use (its peak working set, the model's
        weights read from disk included), added up."""
        if not job:
            return None
        pids = _Pids()
        if not _k32.QueryInformationJobObject(
            job, _PIDS_INFO, ctypes.byref(pids), ctypes.sizeof(pids), None
        ):
            return None
        total = 0
        for pid in pids.ProcessIdList[: pids.NumberOfProcessIdsInList]:
            h = _k32.OpenProcess(_QUERY, False, pid)
            if not h:
                continue
            c = _Counters()
            c.cb = ctypes.sizeof(c)
            if _k32.K32GetProcessMemoryInfo(h, ctypes.byref(c), c.cb):
                total += c.PeakWorkingSetSize
            _k32.CloseHandle(h)
        return total

else:  # the server is stopped with Lindley; a Mac build comes with the installer

    def _end_with_us(proc: subprocess.Popen):
        return None

    def _close(job) -> None:
        pass

    def _peak(job) -> int | None:
        return None
