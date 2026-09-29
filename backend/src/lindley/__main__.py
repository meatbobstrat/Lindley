"""Entry point: `python -m lindley [--settings PATH] [--host HOST] [--port PORT]`."""

from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from lindley.app import create_app

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lindley", description="Run the Lindley server.")
    parser.add_argument("--settings", type=Path, help="path to settings.json")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args(argv)

    uvicorn.run(create_app(settings_path=args.settings), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
