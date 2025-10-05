import os
import subprocess
import sys
import time

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BIN_DIR = os.path.join(BASE_DIR, "lindley", "bin")

# Executables and scripts
REDIS_EXE = os.path.join(BIN_DIR, "redis-server.exe")
WATCHER = os.path.join(BASE_DIR, "lindley", "watcher", "watcher.py")
WORKER = os.path.join(BASE_DIR, "lindley", "worker", "worker.py")

processes = []

def run_process(name, cmd, use_python=False):
    """Run a process (Python script or exe) with PYTHONPATH set to repo root."""
    env = os.environ.copy()
    env["PYTHONPATH"] = BASE_DIR + os.pathsep + env.get("PYTHONPATH", "")
    env["PATH"] = BIN_DIR + os.pathsep + env.get("PATH", "")

    if use_python:
        full_cmd = [sys.executable, cmd]
    else:
        full_cmd = [cmd]

    print(f"[Launcher] Starting {name}: {' '.join(full_cmd)}")
    proc = subprocess.Popen(full_cmd, env=env)
    processes.append(proc)
    return proc

def main():
    try:
        # Start Redis
        if not os.path.exists(REDIS_EXE):
            print("[Launcher] ERROR: redis-server.exe not found in lindley/bin/")
            sys.exit(1)

        run_process("Redis", REDIS_EXE, use_python=False)
        time.sleep(2)  # wait a moment so Redis is ready

        # Start Watcher and Worker
        run_process("Watcher", WATCHER, use_python=True)
        run_process("Worker", WORKER, use_python=True)

        print("[Launcher] Lindley is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[Launcher] Shutting down...")
        for p in processes:
            try:
                p.terminate()
            except Exception:
                pass

if __name__ == "__main__":
    main()
