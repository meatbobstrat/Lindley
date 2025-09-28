import os
import subprocess
import time
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"

r = redis.from_url(REDIS_URL)

def run_cmd(cmd):
    print(f"[Worker] Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            print(f"[Worker] stdout:\n{result.stdout}")
        if result.stderr:
            print(f"[Worker] stderr:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"[Worker] Command failed: {' '.join(cmd)}")
        print(f"[Worker] stdout:\n{e.stdout}")
        print(f"[Worker] stderr:\n{e.stderr}")
        raise

def process_file(path):
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    out_path = f"/output/{name}.pdf"
    tmp_path = f"/tmp/{name}_in.pdf"

    try:
        if ext.lower() in [".jpg", ".jpeg", ".png", ".tif", ".tiff"]:
            run_cmd(["magick", path, "-units", "PixelsPerInch", "-density", "300", tmp_path])
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
            "--pdfa-image-compression", "auto",
            inpdf, out_path
        ]
        run_cmd(cmd)
        print(f"[Worker] Processed {base} -> {out_path}")
    except Exception as e:
        print(f"[Worker] Failed on {base}: {e}")

if __name__ == "__main__":
    print("[Worker] Starting worker loop...")
    os.makedirs("/output", exist_ok=True)
    os.makedirs("/tmp", exist_ok=True)

    while True:
        try:
            job = r.brpop(QUEUE_NAME, timeout=5)
            if job:
                print(f"[Worker] Raw job: {job}")
                _, path = job
                if isinstance(path, bytes):
                    path = path.decode("utf-8")
                print(f"[Worker] Dequeued job: {path}")
                process_file(path)
            else:
                print("[Worker] No job found, sleeping...")
                time.sleep(1)
        except Exception as e:
            print(f"[Worker] Loop error: {e}")
            time.sleep(2)
