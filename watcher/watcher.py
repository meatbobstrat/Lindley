import os
import time
import shutil
import hashlib
import redis
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"

INPUT_DIR = "/input"
PROCESSING_DIR = "/processing"
QUARANTINE_DIR = "/quarantine"
DB_PATH = "/db/watcher.db"

print("[Watcher] Starting up...")
print(f"[Watcher] Using Redis URL: {REDIS_URL}")

try:
    r = redis.from_url(REDIS_URL)
    print("[Watcher] Connected to Redis successfully.")
except Exception as e:
    print(f"[Watcher] ERROR: Could not connect to Redis: {e}")
    raise

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(PROCESSING_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# --- DB bootstrap ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL,
        sha256 TEXT NOT NULL UNIQUE,
        status TEXT DEFAULT 'queued',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()
    print(f"[Watcher] Database initialized at {DB_PATH}")

# --- Dedup helper ---
def hash_file(path):
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def is_duplicate(h):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM files WHERE sha256 = ?", (h,))
    row = cur.fetchone()
    conn.close()
    return row is not None

def record_file(path, h, status="queued"):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO files (path, sha256, status) VALUES (?, ?, ?)", (path, h, status))
    conn.commit()
    conn.close()

# --- Event Handler ---
class Handler(FileSystemEventHandler):
    def process_file(self, path):
        if not os.path.isfile(path):
            return

        try:
            h = hash_file(path)
            if is_duplicate(h):
                print(f"[Watcher] Duplicate detected: {path}")
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
                return

            dest = os.path.join(PROCESSING_DIR, os.path.basename(path))
            shutil.move(path, dest)
            record_file(dest, h)

            r.lpush(QUEUE_NAME, dest)
            print(f"[Watcher] Enqueued {dest}")
        except Exception as e:
            print(f"[Watcher] ERROR on {path}: {e}")
            try:
                shutil.move(path, os.path.join(QUARANTINE_DIR, os.path.basename(path)))
            except:
                pass

    def on_closed(self, event):
        if not event.is_directory:
            print(f"[Watcher] CLOSED event: {event.src_path}")
