import os
import time
import shutil
import hashlib
import redis
import sqlite3
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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

# Support single string or list for watch_folders
watch_config = settings.get("watch_folders", ["./data/input"])
if isinstance(watch_config, str):
    WATCH_FOLDERS = [os.path.abspath(watch_config)]
elif isinstance(watch_config, list):
    WATCH_FOLDERS = [os.path.abspath(p) for p in watch_config]
else:
    raise ValueError("[Watcher] ERROR: watch_folders must be a string or list of strings")

PROCESSING_DIR = os.path.abspath(settings.get("processing_dir", "./data/tmp"))
QUARANTINE_DIR = os.path.abspath(settings.get("quarantine_dir", "./data/quarantine"))
DB_PATH = os.path.abspath(settings.get("db_path", "./data/watcher.db"))
MOVE_FILES = bool(settings.get("move_files", True))

print("[Watcher] Starting up...")
print(f"[Watcher] Using Redis URL: {REDIS_URL}")
print(f"[Watcher] Watching folders: {WATCH_FOLDERS}")
print(f"[Watcher] Mode: {'Move' if MOVE_FILES else 'Copy'}")

# Redis (optional)
try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    redis_ok = True
    print("[Watcher] Connected to Redis successfully.")
except Exception as e:
    r = None
    redis_ok = False
    print(f"[Watcher] WARNING: Redis unavailable, running without queueing. ({e})")

os.makedirs(PROCESSING_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
for folder in WATCH_FOLDERS:
    os.makedirs(folder, exist_ok=True)

# ---------------- DB bootstrap ----------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        path TEXT NOT NULL,
        status TEXT DEFAULT 'queued',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    print(f"[Watcher] Database initialized at {DB_PATH}")

# ---------------- Hash helper ----------------
def hash_file(path, algo="sha256", chunk_size=8192):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

# ---------------- Dedup logic ----------------
def is_duplicate(path):
    name = os.path.basename(path)
    size = os.path.getsize(path)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT sha256 FROM files WHERE name=? AND size=?", (name, size))
    row = cur.fetchone()
    if not row:
        conn.close()
        return False

    new_hash = hash_file(path)
    is_dup = (new_hash == row[0])
    conn.close()
    return is_dup

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

# ---------------- Event Handler ----------------
class Handler(FileSystemEventHandler):
    def process_file(self, path):
        if not os.path.isfile(path):
            return

        try:
            if is_duplicate(path):
                print(f"[Watcher] Duplicate detected: {path}")
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
                return

            h = hash_file(path)
            dest = os.path.join(PROCESSING_DIR, os.path.basename(path))

            if MOVE_FILES:
                shutil.move(path, dest)
                print(f"[Watcher] Moved {path} → {dest}")
            else:
                shutil.copy2(path, dest)
                print(f"[Watcher] Copied {path} → {dest}")

            record_file(dest, h)

            if redis_ok and r:
                r.lpush(QUEUE_NAME, dest)
                print(f"[Watcher] Enqueued {dest}")
            else:
                print(f"[Watcher] Redis offline, skipped enqueue for {dest}")

        except Exception as e:
            print(f"[Watcher] ERROR on {path}: {e}")
            try:
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
            except:
                pass

    def on_closed(self, event):
        if not event.is_directory:
            print(f"[Watcher] CLOSED event: {event.src_path}")
            self.process_file(event.src_path)

# ---------------- Main ----------------
def main():
    init_db()
    observer = Observer()
    for folder in WATCH_FOLDERS:
        observer.schedule(Handler(), folder, recursive=False)
        print(f"[Watcher] Watching {folder} ...")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
