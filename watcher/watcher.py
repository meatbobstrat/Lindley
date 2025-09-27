import os
import time
import redis
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = "ocr_jobs"

r = redis.from_url(REDIS_URL)

class Handler(FileSystemEventHandler):
    def on_closed(self, event):
        if event.is_directory:
            return
        path = event.src_path
        print(f"[Watcher] New file closed: {path}")
        r.lpush(QUEUE_NAME, path)

if __name__ == "__main__":
    input_dir = "/input"
    os.makedirs(input_dir, exist_ok=True)

    event_handler = Handler()
    observer = Observer()
    observer.schedule(event_handler, input_dir, recursive=False)
    observer.start()
    print(f"[Watcher] Monitoring {input_dir} ...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
