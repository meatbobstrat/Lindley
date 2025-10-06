import os
import time
import redis
import sqlite3
import pytesseract
from PIL import Image, ExifTags
from pdf2image import convert_from_path
from PyPDF2 import PdfReader
from datetime import datetime
from langdetect import detect, DetectorFactory
import pandas as pd  # for OCR confidence parsing
import hashlib
import json

# Import DB initializer
from init_db import init_db

# ---------------- Settings ----------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN_DIR = os.path.join(BASE_DIR, "lindley", "bin")

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
def update_file_record(path, fields: dict):
    """Update arbitrary fields for a file row in the DB."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cols = ", ".join([f"{k}=?" for k in fields.keys()])
    values = list(fields.values()) + [path]
    cur.execute(f"UPDATE files SET {cols} WHERE path=?", values)
    conn.commit()
    conn.close()

# ---------------- Timestamp logic ----------------
def get_fallback_timestamps(path):
    try:
        created = datetime.fromtimestamp(os.path.getctime(path)).isoformat()
        modified = datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
        return created, modified
    except Exception:
        now = datetime.utcnow().isoformat()
        return now, now

# ---------------- Language detection ----------------
DetectorFactory.seed = 0

def safe_detect(text: str) -> str:
    if not text.strip():
        return "unknown"
    if len(text.split()) < 5:
        return "unknown"
    try:
        return detect(text)
    except Exception:
        return "unknown"

# ---------------- OCR with confidence ----------------
def ocr_with_confidence(img):
    try:
        df = pytesseract.image_to_data(
            img, lang="eng", output_type=pytesseract.Output.DATAFRAME
        )
        df = df[df.text.notna()]
        df = df[df.text.str.strip() != ""]

        if df.empty:
            text = pytesseract.image_to_string(img, lang="eng")
            return text, 0.0

        text = " ".join([str(w) for w in df.text if str(w).strip()])
        avg_conf = df[df.conf != -1].conf.mean() if not df.empty else 0.0
        return text, avg_conf
    except Exception as e:
        print(f"[Worker] OCR error: {e}")
        try:
            return pytesseract.image_to_string(img, lang="eng"), 0.0
        except Exception:
            return "", 0.0

# ---------------- Metadata extraction ----------------
def get_file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def extract_image_metadata(img: Image.Image):
    try:
        exif = img._getexif()
        if not exif:
            return {}
        return {ExifTags.TAGS.get(k, k): v for k, v in exif.items()}
    except Exception:
        return {}

def extract_pdf_metadata(path):
    try:
        reader = PdfReader(path)
        info = reader.metadata
        return {k[1:]: str(v) for k, v in info.items()} if info else {}
    except Exception:
        return {}

# ---------------- OCR logic ----------------
def process_file(path):
    base = os.path.basename(path)
    ext = os.path.splitext(base)[1].lower()

    # Only process if in inbox
    if not os.path.abspath(path).startswith(INBOX_DIR):
        print(f"[Worker] Skipping {base} (not in inbox)")
        return

    try:
        print(f"[Worker] Processing {base}")
        update_file_record(path, {"status": "processing"})

        text = ""
        page_count = 0
        file_size = os.path.getsize(path)
        created, modified = get_fallback_timestamps(path)
        confidences = []
        metadata = {}

        file_hash = get_file_hash(path)

        if ext in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            img = Image.open(path)
            metadata.update(extract_image_metadata(img))
            text, conf = ocr_with_confidence(img)
            page_count = 1
            confidences.append(conf)

        elif ext == ".pdf":
            metadata.update(extract_pdf_metadata(path))
            pages = convert_from_path(path, dpi=300)
            all_text = []
            for i, page in enumerate(pages, start=1):
                page_text, conf = ocr_with_confidence(page)
                all_text.append(f"\n--- Page {i} ---\n{page_text}")
                confidences.append(conf)
            text = "\n".join(all_text)
            page_count = len(pages)

        else:
            print(f"[Worker] Unsupported type: {ext}")
            update_file_record(path, {"status": "error"})
            return

        word_count = len(text.split())
        lang = safe_detect(text)
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0

        update_file_record(path, {
            "status": "processed",
            "ocr_text": text,
            "page_count": page_count,
            "file_size": file_size,
            "sha256": file_hash,
            "doc_created": created,
            "doc_modified": modified,
            "word_count": word_count,
            "lang": lang,
            "ocr_confidence": avg_conf,
            "metadata": json.dumps(metadata)
        })
        print(f"[Worker] Completed {base} "
              f"({page_count} pages, {word_count} words, lang={lang}, conf={avg_conf:.1f})")

    except Exception as e:
        print(f"[Worker] ERROR on {base}: {e}")
        try:
            os.rename(path, os.path.join(OCR_QUARANTINE, base))
        except Exception:
            pass
        update_file_record(path, {"status": "error"})

# ---------------- Main loop ----------------
if __name__ == "__main__":
    db_version = init_db(DB_PATH)
    print(f"[Worker] Connected to DB schema version {db_version}")

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
