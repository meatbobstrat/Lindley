import os
import time
import redis
import sqlite3
import pytesseract
from PIL import Image
from pdf2image import convert_from_path

# Import the central DB initializer
from init_db import init_db

# ---------------- Settings ----------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BIN_DIR = os.path.join(BASE_DIR, "bin")
TESSERACT_EXE = os.path.join(BIN_DIR, "tesseract.exe")
TESSDATA_DIR = os.path.join(BIN_DIR, "tessdata")

# Force pytesseract to use the bundled exe + tessdata
pytesseract.pytesseract.tesseract_cmd = TESSERACT_EXE
os.environ["TESSDATA_PREFIX"] = TESSDATA_DIR

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"
DB_PATH = os.path.abspath("./data/watcher.db")
OCR_QUARANTINE = os.path.abspath("./data/ocr_quarantine")

os.makedirs(OCR_QUARANTINE, exist_ok=True)

# ---------------- Redis ----------------
r = redis.from_url(REDIS_URL)

# ---------------- DB helpers ----------------
def update_status(path, status, text=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if text is not None:
        cur.execute(
            "UPDATE files SET status=?, ocr_text=? WHERE path=?",
            (status, text, path)
        )
    else:
        cur.execute("UPDATE files SET status=? WHERE path=?", (status, path))
    conn.commit()
    conn.close()

# ---------------- OCR logic ----------------
def process_file(path):
    base = os.path.basename(path)
    ext = os.path.splitext(base)[1].lower()

    try:
        print(f"[Worker] Processing {base}")
        update_status(path, "processing")

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
            update_status(path, "error")
            return

        update_status(path, "processed", text)
        print(f"[Worker] Completed {base}")

    except Exception as e:
        print(f"[Worker] ERROR on {base}: {e}")
        try:
            os.rename(path, os.path.join(OCR_QUARANTINE, base))
        except Exception:
            pass
        update_status(path, "error")

# ---------------- Main loop ----------------
if __name__ == "__main__":
    # Ensure DB schema is initialized
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
