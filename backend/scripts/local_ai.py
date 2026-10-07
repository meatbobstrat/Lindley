"""Lindley's own AI from the command line: download models, see what's there, run the server.

python scripts/local_ai.py list
python scripts/local_ai.py download gemma-4-e4b qwen3.5-4b
python scripts/local_ai.py serve gemma-4-e4b --device none

Uses the folder in settings (ai.local.models_dir). `serve` starts the server as Lindley would,
loads the model, and waits, printing its address, until Ctrl+C; then it says the most memory the
server and its models took (Windows). --device: "none" for the processor alone, or a device from
`devices` (Vulkan0, Vulkan1...); left out, llama.cpp uses any graphics it can.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

import httpx2

from lindley.config import load_settings
from lindley.localai import download
from lindley.localai.catalog import MODELS, engine
from lindley.localai.server import LocalServer


def gb(n: float) -> str:
    return f"{n / 1e9:.2f} GB"


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("list", help="the models Lindley knows, and which are downloaded")
    d = sub.add_parser("download", help="download models, and the engine if it's missing")
    d.add_argument("models", nargs="+", choices=list(MODELS))
    sub.add_parser("devices", help="the graphics llama.cpp can use")
    s = sub.add_parser("serve", help="run the server with a model loaded, until Ctrl+C")
    s.add_argument("model", choices=list(MODELS))
    s.add_argument("--device", help='"none" (the processor alone), or e.g. Vulkan1')
    a = ap.parse_args()

    local = load_settings().ai.local
    root = local.folder()
    if a.command == "list":
        e = engine()
        print(root)
        print(f"  engine {e.build if e else '-'}: {'yes' if e and e.installed(root) else 'no'}")
        for m in MODELS.values():
            print(f"  {m.id:<18} {gb(m.size):>9}  {'yes' if m.installed(root) else 'no'}")
    elif a.command == "download":
        last = [0.0]

        def progress(label: str, done: int, of: int) -> None:
            if time.monotonic() - last[0] > 2 or done == of:
                last[0] = time.monotonic()
                print(f"\r{label}: {gb(done)} of {gb(of)}   ", end="", flush=True)

        download.get(root, a.models, progress)
        print("\ndone")
    elif a.command == "devices":
        program = LocalServer(local).program()
        subprocess.run([str(program), "--list-devices"], check=False)
    else:
        if a.device is not None:
            local = local.model_copy(update={"device": a.device})
        server = LocalServer(local)
        url = server.url(a.model)
        started = time.monotonic()
        httpx2.post(f"{url.removesuffix('/v1')}/models/load", json={"model": a.model}, timeout=600)
        while True:  # wait for it to load
            r = httpx2.get(f"{url.removesuffix('/v1')}/models", timeout=10).json()
            status = next(m["status"]["value"] for m in r["data"] if m["id"] == a.model)
            if status == "loaded":
                break
            if status not in ("loading", "unloaded"):
                sys.exit(f"{a.model}: {status}")
            time.sleep(0.5)
        print(f"{a.model} loaded in {time.monotonic() - started:.1f} s at {url}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            peak = server.peak_memory()
            server.stop()
            if peak:
                print(f"most memory taken: {gb(peak)}")


if __name__ == "__main__":
    main()
