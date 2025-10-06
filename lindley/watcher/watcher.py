import os
import time
import shutil
import hashlib
import redis
import sqlite3
import json
import threading
from watchdog.observers.polling import PollingObserver as Observer  # safer cross-platform
from watchdog.events import FileSystemEventHandler

# Import shared DB initializer
from init_db import init_db  # <-- reuse canonical schema

# ---------------- Settings ----------------
SETTINGS_PATH = os.path.abspath("./settings.json")

def load_settings():
    if not os.path.exists(SETTINGS_PATH):
        raise FileNotFoundError(
            f"[Watcher] ERROR: settings.json not found at {SETTINGS_PATH}"
        )
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

settings = load_settings()

REDIS_URL = settings.get("redis_url", "redis://localhost:6379/0")
QUEUE_NAME = settings.get("queue_name", "ocr_jobs")

watch_config = settings.get("watch_folders", ["./data/input"])
if isinstance(watch_config, str):
    WATCH_FOLDERS = [os.path.abspath(watch_config)]
elif isinstance(watch_config, list):
    WATCH_FOLDERS = [os.path.abspath(p) for p in watch_config]
else:
    raise ValueError("[Watcher] ERROR: watch_folders must be a string or list of strings")

# Always include inbox
inbox_path = os.path.abspath("./data/inbox")
if inbox_path not in WATCH_FOLDERS:
    WATCH_FOLDERS.append(inbox_path)

QUARANTINE_DIR = os.path.abspath(settings.get("quarantine_dir", "./data/quarantine"))
DB_PATH = os.path.abspath(settings.get("db_path", "./data/watcher.db"))
RESCAN_INTERVAL = int(settings.get("rescan_interval", 60))  # seconds

print("[Watcher] Starting up...")
print(f"[Watcher] Using Redis URL: {REDIS_URL}")
print(f"[Watcher] Watching folders: {WATCH_FOLDERS}")

# Redis connection
try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    redis_ok = True
    print("[Watcher] Connected to Redis successfully.")
except Exception as e:
    r = None
    redis_ok = False
    print(f"[Watcher] WARNING: Redis unavailable, running without queueing. ({e})")

# Ensure dirs exist
os.makedirs(QUARANTINE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
for folder in WATCH_FOLDERS:
    os.makedirs(folder, exist_ok=True)

# ---------------- Helpers ----------------
def hash_file(path, algo="sha256", chunk_size=8192):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def is_duplicate(path):
    name = os.path.basename(path)
    size = os.path.getsize(path)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT sha256 FROM files WHERE name=? AND size=?", (name, size))
    row = cur.fetchone()
    conn.close()

    if not row:
        return False

    new_hash = hash_file(path)
    return new_hash == row[0]

def record_file(path, h, status="queued"):
    name = os.path.basename(path)
    size = os.path.getsize(path)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO files (name, size, sha256, path, status)
        VALUES (?, ?, ?, ?, ?)
    """, (name, size, h, path, status))
    conn.commit()
    conn.close()

def is_file_stable(path, wait=0.5):
    """Return True if file size hasn't changed across two checks."""
    try:
        s1 = os.path.getsize(path)
        time.sleep(wait)
        s2 = os.path.getsize(path)
        return s1 == s2
    except FileNotFoundError:
        return False

# ---------------- Event Handler ----------------
class Handler(FileSystemEventHandler):
    def process_file(self, path):
        if not os.path.isfile(path):
            return

        while not is_file_stable(path):
            time.sleep(0.5)

        try:
            print(f"[Watcher] Processing {path}")

            if is_duplicate(path):
                print(f"[Watcher] Duplicate detected: {path}")
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
                return

            print(f"[Watcher] Hashing {path}")
            h = hash_file(path)

            dest = os.path.abspath(path)  # stay in place (inbox or input folder)

            print(f"[Watcher] Recording {dest} in DB")
            record_file(dest, h)

            if redis_ok and r:
                print(f"[Watcher] Enqueuing {dest}")
                r.lpush(QUEUE_NAME, dest)

            print(f"[Watcher] SUCCESS for {dest}")

        except Exception as e:
            print(f"[Watcher] ERROR on {path}: {type(e).__name__}: {e}")
            try:
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
                print(f"[Watcher] Quarantined {path}")
            except Exception as qe:
                print(f"[Watcher] FAILED to quarantine {path}: {type(qe).__name__}: {qe}")

    def on_created(self, event):
        if not event.is_directory:
            print(f"[Watcher] CREATED event: {event.src_path}")
            self.process_file(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            dest = getattr(event, "dest_path", event.src_path)
            print(f"[Watcher] MOVED event: {dest}")
            self.process_file(dest)

    def on_closed(self, event):
        if not event.is_directory:
            print(f"[Watcher] CLOSED event: {event.src_path}")
            self.process_file(event.src_path)

# ---------------- Rescan Worker ----------------
def rescan_loop(handler, stop_evt):
    while not stop_evt.is_set():
        for folder in WATCH_FOLDERS:
            for fname in os.listdir(folder):
                fpath = os.path.join(folder, fname)
                if os.path.isfile(fpath):
                    handler.process_file(fpath)
        stop_evt.wait(RESCAN_INTERVAL)

# ---------------- Main ----------------
def main():
    # Use the central DB initializer from init_db.py
    init_db(DB_PATH)

    observer = Observer()
    handler = Handler()

    for folder in WATCH_FOLDERS:
        observer.schedule(handler, folder, recursive=False)
        print(f"[Watcher] Watching {folder} ...")

        # Startup scan
        for fname in os.listdir(folder):
            fpath = os.path.join(folder, fname)
            if os.path.isfile(fpath):
                handler.process_file(fpath)

    observer.start()

    stop_evt = threading.Event()
    rescan_thread = threading.Thread(target=rescan_loop, args=(handler, stop_evt), daemon=True)
    rescan_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_evt.set()
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
