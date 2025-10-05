import os
import sys
import time
import sqlite3
import redis
import pytesseract
from PIL import Image

from init_db import init_db  # canonical schema

# ---------------- Settings ----------------
SETTINGS_PATH = os.path.abspath("./settings.json")

import json
if not os.path.exists(SETTINGS_PATH):
    raise FileNotFoundError(f"[Worker] settings.json not found at {SETTINGS_PATH}")
with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
    settings = json.load(f)

REDIS_URL = settings.get("redis_url", "redis://localhost:6379/0")
QUEUE_NAME = settings.get("queue_name", "ocr_jobs")
DB_PATH = os.path.abspath(settings.get("db_path", "./data/watcher.db"))

# ---------------- Redis Connection ----------------
try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    redis_ok = True
    print("[Worker] Connected to Redis successfully.")
except Exception as e:
    r = None
    redis_ok = False
    print(f"[Worker] WARNING: Redis unavailable, running without queueing. ({e})")

# ---------------- DB Helpers ----------------
def update_file_status(file_path, status, ocr_text=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if ocr_text is not None:
        cur.execute("""
            UPDATE files
            SET status = ?, ocr_text = ?
            WHERE path = ?
        """, (status, ocr_text, file_path))
    else:
        cur.execute("""
            UPDATE files
            SET status = ?
            WHERE path = ?
        """, (status, file_path))
    conn.commit()
    conn.close()

# ---------------- OCR ----------------
def perform_ocr(file_path):
    try:
        img = Image.open(file_path)
        text = pytesseract.image_to_string(img)
        return text
    except Exception as e:
        print(f"[Worker] ERROR OCR {file_path}: {e}")
        return ""

# ---------------- Worker Loop ----------------
def process_job(file_path):
    print(f"[Worker] Starting OCR: {file_path}")
    text = perform_ocr(file_path)

    if text.strip():
        update_file_status(file_path, "ready", ocr_text=text)
        print(f"[Worker] OCR complete for {file_path}")
    else:
        update_file_status(file_path, "error", ocr_text="")
        print(f"[Worker] OCR failed for {file_path}")

def redis_loop():
    print("[Worker] Entering Redis loop...")
    while True:
        try:
            job = r.brpop(QUEUE_NAME, timeout=5)
            if job:
                _, file_path = job
                file_path = file_path.decode("utf-8")
                process_job(file_path)
        except Exception as e:
            print(f"[Worker] Redis loop error: {e}")
            time.sleep(5)

def db_scan_loop():
    print("[Worker] Entering DB scan loop (no Redis)...")
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT path FROM files WHERE status = 'queued'")
            rows = cur.fetchall()
            conn.close()

            for (file_path,) in rows:
                if os.path.exists(file_path):
                    process_job(file_path)
                else:
                    print(f"[Worker] File not found: {file_path}")
        except Exception as e:
            print(f"[Worker] DB scan error: {e}")
        time.sleep(5)

# ---------------- Main ----------------
def main():
    init_db(DB_PATH)

    if redis_ok and r:
        redis_loop()
    else:
        db_scan_loop()

if __name__ == "__main__":
    main()
