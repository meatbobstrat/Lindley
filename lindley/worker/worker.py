import os
import time
import redis
import sqlite3
import pytesseract
from PIL import Image
from pdf2image import convert_from_path

# Import DB initializer
from init_db import init_db

# ---------------- Settings ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BIN_DIR = os.path.join(BASE_DIR, "bin")
TESSERACT_EXE = os.path.join(BIN_DIR, "tesseract.exe")
TESSDATA_DIR = os.path.join(BIN_DIR, "tessdata")

# Configure pytesseract to use bundled binaries
pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE
os.environ["TESSDATA_PREFIX"] = TESSDATA_DIR

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"
DB_PATH = os.path.abspath("./data/watcher.db")
OCR_QUARANTINE = os.path.abspath("./data/ocr_quarantine")
INBOX_DIR = os.path.abspath("./data/inbox")

os.makedirs(OCR_QUARANTINE, exist_ok=True)
os.makedirs(INBOX_DIR, exist_ok=True)

# ---------------- Redis ----------------
r = redis.from_url(REDIS_URL)

# ---------------- DB helpers ----------------
def update_file(path, status, text=None, location=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if text is not None and location is not None:
        cur.execute(
            "UPDATE files SET status=?, ocr_text=?, location=? WHERE path=?",
            (status, text, location, path),
        )
    elif text is not None:
        cur.execute(
            "UPDATE files SET status=?, ocr_text=? WHERE path=?",
            (status, text, path),
        )
    elif location is not None:
        cur.execute(
            "UPDATE files SET status=?, location=? WHERE path=?",
            (status, location, path),
        )
    else:
        cur.execute(
            "UPDATE files SET status=? WHERE path=?",
            (status, path),
        )

    conn.commit()
    conn.close()

# ---------------- OCR logic ----------------
def process_file(path):
    base = os.path.basename(path)
    ext = os.path.splitext(base)[1].lower()

    try:
        print(f"[Worker] Processing {base}")
        update_file(path, "processing")

        text = ""
        if ext in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            img = Image.open(path)
            text = pytesseract.image_to_string(img, lang="eng")

        elif ext == ".pdf":
            pages = convert_from_path(path, dpi=300)
            for i, page in enumerate(pages, start=1):
                page_text = pytesseract.image_to_string(page, lang="eng")
                text += f"\n--- Page {i} ---\n{page_text}"

        else:
            print(f"[Worker] Unsupported type: {ext}")
            update_file(path, "error")
            return

        if text.strip():
            update_file(path, "ready", text, location="inbox")
            print(f"[Worker] OCR complete for {base}")
        else:
            print(f"[Worker] Empty OCR result for {base}")
            os.rename(path, os.path.join(OCR_QUARANTINE, base))
            update_file(path, "error", "", location="ocr_quarantine")

    except Exception as e:
        print(f"[Worker] ERROR on {base}: {e}")
        try:
            os.rename(path, os.path.join(OCR_QUARANTINE, base))
        except Exception:
            pass
        update_file(path, "error", "", location="ocr_quarantine")

# ---------------- Main loop ----------------
if __name__ == "__main__":
    init_db(DB_PATH)

    print("[Worker] Starting loop...")
    while True:
        try:
            job = r.brpop(QUEUE_NAME, timeout=5)
            if job:
                _, path = job
                if isinstance(path, bytes):
                    path = path.decode("utf-8")
                process_file(path)
            else:
                time.sleep(1)
        except Exception as e:
            print(f"[Worker] Loop error: {e}")
            time.sleep(2)
