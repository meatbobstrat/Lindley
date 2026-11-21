"""Entry point: `python -m lindley [--settings PATH] [--host HOST] [--port PORT] [--reload]
[--no-browser] [--no-tray]`. The installed app starts it with none: see launcher."""

from __future__ import annotations

import argparse
import logging
import logging.config
import os
import sys
from pathlib import Path

import uvicorn
from pydantic import ValidationError

from lindley import launcher, logs
from lindley.app import HOST_ENV_VAR, create_app
from lindley.config import SETTINGS_ENV_VAR, load_settings, resolve_settings_path

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
SOURCE = Path(__file__).resolve().parent.parent  # backend/src, watched by --reload


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
    parser.add_argument(
        "--debug",
        action="store_true",
        help=f"log in detail ({logs.LEVEL_ENV_VAR}=DEBUG does the same): for finding a problem",
    )
    _no_console()
    args = parser.parse_args(argv)
    level = logs.level_from(args.debug)

    if args.reload:  # the server is started afresh on each change, so it's named, not made
        if args.settings:
            os.environ[SETTINGS_ENV_VAR] = str(args.settings)
        os.environ[HOST_ENV_VAR] = args.host
        try:  # only to find where the log goes: the app reads them again, and says if it can't
            file = load_settings(args.settings).db_path.parent / "logs" / "lindley.log"
        except (OSError, ValueError):
            file = None
        uvicorn.run(
            "lindley.app:create_app",
            factory=True,
            reload=True,
            reload_dirs=[str(SOURCE)],
            host=args.host,
            port=args.port,
            log_config=logs.log_config(file, level, rotate=False),
        )
        return
    url = launcher.address(args.host, args.port)
    if launcher.already_running(url):  # started again: one Lindley, opened
        print(f"Lindley is already running at {url}")
        if not args.no_browser:
            launcher.webbrowser.open(url)
        return
    settings_path = resolve_settings_path(args.settings)
    try:
        settings = load_settings(args.settings)
    except (OSError, ValueError) as e:  # not JSON, or not settings: ValidationError is a ValueError
        _cant_read(settings_path, e, level)
        sys.exit(1)
    # beside the database: out of the box, Lindley's folder in this user's data
    file = settings.db_path.parent / "logs" / "lindley.log"
    app = create_app(settings, settings_path=args.settings, host=args.host)
    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_config=logs.log_config(file, level),
    )
    logs.install_excepthooks()
    logs.banner(settings_path, settings_path.exists(), settings.db_path, file, level)
    server = uvicorn.Server(config)
    app.state.quit = lambda: setattr(server, "should_exit", True)  # Quit Lindley, in the app
    launcher.run(server, url, browser=not args.no_browser, tray=not args.no_tray)


def _cant_read(path: Path, error: Exception, level: str) -> None:
    """Settings that can't be read: said on the console and in the log, where a person started
    from the menu will look. Nothing is overwritten: they fix the file, or move it aside."""
    message = (
        f"Lindley couldn't read its settings from {path}: {_short(error)}. "
        "Fix that file, or move it aside to start afresh, and start Lindley again."
    )
    try:
        file = logs.default_log_file()
        logging.config.dictConfig(logs.log_config(file, level))
        logging.getLogger("lindley").error("%s", message, exc_info=error)
    except OSError:  # nowhere to write it either
        pass
    print(message, file=sys.stderr)


def _short(error: Exception) -> str:
    """What was wrong, in a line: a validation error runs to a screen."""
    if isinstance(error, ValidationError):
        first = error.errors()[0]
        where = ".".join(str(p) for p in first["loc"]) or "the file"
        more = error.error_count() - 1
        return f"{where}: {first['msg']}" + (f" (and {more} more)" if more else "")
    lines = str(error).strip().splitlines()
    return lines[0] if lines else type(error).__name__


if __name__ == "__main__":
    main()
