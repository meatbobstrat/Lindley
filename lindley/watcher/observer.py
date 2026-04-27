import hashlib
import os
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


SUPPORTED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".tiff", ".bmp", ".gif"}


class FileHandler(FileSystemEventHandler):
    """Handle file system events for a watched folder."""

    def __init__(
        self,
        folder_id: str,
        folder_path: str,
        move_files: bool,
        db_path: str,
        inbox_dir: str,
        quarantine_dir: str,
    ):
        self.folder_id = folder_id
        self.folder_path = folder_path
        self.move_files = move_files
        self.db_path = db_path
        self.inbox_dir = inbox_dir
        self.quarantine_dir = quarantine_dir

    def _get_file_hash(self, path: str) -> str:
        """Compute SHA256 hash of file."""
        h = hashlib.sha256()
        try:
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    h.update(chunk)
            return h.hexdigest()
        except Exception as e:
            print(f"[Observer] Hash error for {path}: {e}")
            return ""

    def _is_supported(self, path: str) -> bool:
        """Check if file has a supported extension."""
        ext = Path(path).suffix.lower()
        return ext in SUPPORTED_EXTENSIONS

    def _is_duplicate(self, file_hash: str) -> bool:
        """Check if file hash already exists in DB."""
        if not file_hash:
            return False
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT id FROM files WHERE sha256 = ?", (file_hash,))
            exists = cur.fetchone() is not None
            conn.close()
            return exists
        except Exception as e:
            print(f"[Observer] DB error checking duplicate: {e}")
            return False

    def _ingest_file(self, src_path: str) -> None:
        """Ingest a file into the inbox."""
        if not os.path.isfile(src_path):
            return

        if not self._is_supported(src_path):
            print(f"[Observer] Unsupported file type: {src_path}")
            return

        file_hash = self._get_file_hash(src_path)
        if not file_hash:
            print(f"[Observer] Failed to hash file: {src_path}")
            return

        # Check for duplicates
        if self._is_duplicate(file_hash):
            print(f"[Observer] Duplicate detected: {src_path} (hash {file_hash})")
            self._quarantine_file(src_path, "duplicate")
            return

        # Move or copy file to inbox
        filename = os.path.basename(src_path)
        dst_path = os.path.join(self.inbox_dir, filename)

        # Ensure unique filename
        base, ext = os.path.splitext(filename)
        counter = 1
        while os.path.exists(dst_path):
            dst_path = os.path.join(self.inbox_dir, f"{base}_{counter}{ext}")
            counter += 1

        try:
            if self.move_files:
                shutil.move(src_path, dst_path)
                print(f"[Observer] Moved {src_path} -> {dst_path}")
            else:
                shutil.copy2(src_path, dst_path)
                print(f"[Observer] Copied {src_path} -> {dst_path}")
        except Exception as e:
            print(f"[Observer] Failed to move/copy {src_path}: {e}")
            self._quarantine_file(src_path, "move_error")
            return

        # Insert into DB
        self._insert_db_record(dst_path, filename, file_hash)

    def _quarantine_file(self, src_path: str, reason: str) -> None:
        """Move file to quarantine."""
        os.makedirs(self.quarantine_dir, exist_ok=True)
        filename = os.path.basename(src_path)
        dst_path = os.path.join(self.quarantine_dir, filename)

        base, ext = os.path.splitext(filename)
        counter = 1
        while os.path.exists(dst_path):
            dst_path = os.path.join(self.quarantine_dir, f"{base}_{counter}{ext}")
            counter += 1

        try:
            shutil.move(src_path, dst_path)
            print(f"[Observer] Quarantined {src_path} -> {dst_path} (reason: {reason})")
        except Exception as e:
            print(f"[Observer] Failed to quarantine {src_path}: {e}")

    def _insert_db_record(self, file_path: str, filename: str, file_hash: str) -> None:
        """Insert file record into DB."""
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            file_size = os.path.getsize(file_path)
            created_at = datetime.now().isoformat()

            cur.execute(
                """
                INSERT INTO files (name, size, sha256, path, location, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (filename, file_size, file_hash, file_path, "inbox", "queued", created_at),
            )
            conn.commit()
            conn.close()
            print(f"[Observer] DB record created for {filename}")
        except Exception as e:
            print(f"[Observer] DB insert error for {filename}: {e}")
            self._quarantine_file(file_path, "db_insert_error")

    def on_created(self, event):
        """Handle file creation events."""
        if event.is_directory:
            return
        # Small delay to ensure file write is complete
        time.sleep(0.5)
        self._ingest_file(event.src_path)


class WatcherObserver:
    """Manage watchdog observers for multiple folders."""

    def __init__(self, db_path: str, inbox_dir: str, quarantine_dir: str):
        self.db_path = db_path
        self.inbox_dir = inbox_dir
        self.quarantine_dir = quarantine_dir
        self.observers: Dict[str, Observer] = {}

    def add_watch(self, folder_config: Dict) -> None:
        """Add a folder to watch."""
        folder_id = folder_config.get("id")
        folder_path = folder_config.get("path")
        move_files = folder_config.get("move_files", True)

        if not folder_path or not os.path.isdir(folder_path):
            print(f"[Observer] Invalid folder path: {folder_path}")
            return

        handler = FileHandler(
            folder_id=folder_id,
            folder_path=folder_path,
            move_files=move_files,
            db_path=self.db_path,
            inbox_dir=self.inbox_dir,
            quarantine_dir=self.quarantine_dir,
        )

        observer = Observer()
        observer.schedule(handler, folder_path, recursive=False)
        observer.start()

        self.observers[folder_id] = observer
        print(f"[Observer] Watching {folder_path} (move={move_files})")

    def scan_existing_files(self, folder_config: Dict) -> None:
        """Scan folder for existing files on startup."""
        folder_path = folder_config.get("path")
        move_files = folder_config.get("move_files", True)

        if not os.path.isdir(folder_path):
            return

        handler = FileHandler(
            folder_id=folder_config.get("id"),
            folder_path=folder_path,
            move_files=move_files,
            db_path=self.db_path,
            inbox_dir=self.inbox_dir,
            quarantine_dir=self.quarantine_dir,
        )

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                handler._ingest_file(file_path)

    def start(self) -> None:
        """Start all observers."""
        for observer in self.observers.values():
            if not observer.is_alive():
                observer.start()
        print(f"[Observer] Started {len(self.observers)} observers")

    def stop(self) -> None:
        """Stop all observers."""
        for observer in self.observers.values():
            observer.stop()
        for observer in self.observers.values():
            observer.join(timeout=5)
        print("[Observer] Stopped all observers")
