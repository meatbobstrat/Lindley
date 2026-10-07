"""Entry point: `python -m lindley [--settings PATH] [--host HOST] [--port PORT] [--reload]
[--no-browser] [--no-tray]`. The installed app starts it with none: see launcher."""

from __future__ import annotations

import argparse
import copy
import os
import sys
from pathlib import Path

import uvicorn
from uvicorn.config import LOGGING_CONFIG

from lindley import launcher
from lindley.app import HOST_ENV_VAR, create_app
from lindley.config import SETTINGS_ENV_VAR, load_settings

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
SOURCE = Path(__file__).resolve().parent.parent  # backend/src, watched by --reload
LOG_BYTES = 5_000_000  # each log file, of three


def log_config(file: Path | None = None) -> dict:
    """Uvicorn's own logging, with Lindley's messages beside its requests: scans taken and read,
    the Inbox sorted, each page the AI read and how long it took. With a file, Lindley's
    messages and uvicorn's are kept there too: an app started from the menu has no console."""
    config = copy.deepcopy(LOGGING_CONFIG)
    config["loggers"]["lindley"] = {"handlers": ["default"], "level": "INFO", "propagate": False}
    if file is not None:
        file.parent.mkdir(parents=True, exist_ok=True)
        config["formatters"]["file"] = {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"}
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "file",
            "filename": str(file),
            "maxBytes": LOG_BYTES,
            "backupCount": 2,
            "encoding": "utf-8",
        }
        for name in ("lindley", "uvicorn", "uvicorn.access"):
            config["loggers"][name]["handlers"].append("file")
    return config


def _no_console() -> None:
    """Started without a console (from the menu), Python has nowhere to print: give it nowhere."""
    for name in ("stdout", "stderr"):
        if getattr(sys, name) is None:
            setattr(sys, name, open(os.devnull, "w", encoding="utf-8"))  # noqa: SIM115


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="lindley", description="Run the Lindley server.")
    parser.add_argument("--settings", type=Path, help="path to settings.json")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--reload", action="store_true", help="restart when the code changes")
    parser.add_argument("--no-browser", action="store_true", help="don't open it in the browser")
    parser.add_argument("--no-tray", action="store_true", help="no icon in the tray")
    _no_console()
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
    url = launcher.address(args.host, args.port)
    if launcher.already_running(url):  # started again: one Lindley, opened
        print(f"Lindley is already running at {url}")
        if not args.no_browser:
            launcher.webbrowser.open(url)
        return
    settings = load_settings(args.settings)
    app = create_app(settings, settings_path=args.settings, host=args.host)
    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        # beside the database: out of the box, Lindley's folder in this user's data
        log_config=log_config(settings.db_path.parent / "logs" / "lindley.log"),
    )
    server = uvicorn.Server(config)
    app.state.quit = lambda: setattr(server, "should_exit", True)  # Quit Lindley, in the app
    launcher.run(server, url, browser=not args.no_browser, tray=not args.no_tray)


if __name__ == "__main__":
    main()
