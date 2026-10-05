"""Entry point: `python -m lindley [--settings PATH] [--host HOST] [--port PORT] [--reload]`."""

from __future__ import annotations

import argparse
import copy
import os
from pathlib import Path

import uvicorn
from uvicorn.config import LOGGING_CONFIG

from lindley.app import HOST_ENV_VAR, create_app
from lindley.config import SETTINGS_ENV_VAR

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
SOURCE = Path(__file__).resolve().parent.parent  # backend/src, watched by --reload


def log_config() -> dict:
    """Uvicorn's own logging, with Lindley's messages beside its requests: scans taken and read,
    the Inbox sorted, each page the AI read and how long it took."""
    config = copy.deepcopy(LOGGING_CONFIG)
    config["loggers"]["lindley"] = {"handlers": ["default"], "level": "INFO", "propagate": False}
    return config


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lindley", description="Run the Lindley server.")
    parser.add_argument("--settings", type=Path, help="path to settings.json")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--reload", action="store_true", help="restart when the code changes")
    args = parser.parse_args(argv)

    if args.reload:  # the server is started afresh on each change, so it's named, not made
        if args.settings:
            os.environ[SETTINGS_ENV_VAR] = str(args.settings)
        os.environ[HOST_ENV_VAR] = args.host
        uvicorn.run(
            "lindley.app:create_app",
            factory=True,
            reload=True,
            reload_dirs=[str(SOURCE)],
            host=args.host,
            port=args.port,
            log_config=log_config(),
        )
        return
    app = create_app(settings_path=args.settings, host=args.host)
    uvicorn.run(app, host=args.host, port=args.port, log_config=log_config())


if __name__ == "__main__":
    main()
