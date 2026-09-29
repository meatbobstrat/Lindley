"""Folder watcher built on watchdog.

Stub: implemented in the watcher phase. Responsibilities:
- monitor every folder in Settings.watch_folders for new image/PDF files
- wait until a file has finished copying (size stable) before acting
- move or copy it (Settings.move_files) into Settings.processing_dir
- enqueue a job row (status 'queued') for the worker
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from lindley.config import Settings

SUPPORTED_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp", ".pdf"})


def is_supported(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_SUFFIXES


class FolderWatcher:
    def __init__(self, settings: Settings, on_file: Callable[[Path], None]) -> None:
        self.settings = settings
        self.on_file = on_file

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError
