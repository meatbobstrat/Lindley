import os
import subprocess
import time
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"

r = redis.from_url(REDIS_URL)

def process_file(path):
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    out_path = f"/output/{name}.pdf"
    tmp_path = f"/tmp/{name}_in.pdf"

    try:
        if ext.lower() in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            subprocess.run(["magick", path, "-units", "PixelsPerInch", "-density", "300", tmp_path], check=True)
            inpdf = tmp_path
        elif ext.lower() == ".pdf":
            inpdf = path
        else:
            print(f"[Worker] Skipping unsupported file type: {base}")
            return

        cmd = [
            "ocrmypdf",
            "--optimize", "3",
            "--pdfa",
            "--deskew",
            "--rotate-pages",
            "--clean",
            "--language", "eng",
            "--jobs", str(os.cpu_count() or 2),
            "--force-ocr",
            inpdf, out_path
        ]
        subprocess.run(cmd, check=True)
        print(f"[Worker] Processed {base} -> {out_path}")
    except Exception as e:
        print(f"[Worker] Failed on {base}: {e}")

if __name__ == "__main__":
    print("[Worker] Starting worker loop...")
    os.makedirs("/output", exist_ok=True)
    os.makedirs("/tmp", exist_ok=True)

    while True:
        job = r.brpop(QUEUE_NAME, timeout=5)
        if job:
            _, path = job
            path = path.decode("utf-8")
            process_file(path)
        else:
            time.sleep(1)
