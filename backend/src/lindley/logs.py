"""Lindley's logging: where it goes, how much, and what never goes in it.

One log, `logs/lindley.log` beside the database (5 MB each, three kept), and the console when
there is one. Lindley's own messages are INFO; `python -m lindley --debug` (or LINDLEY_LOG_LEVEL=
DEBUG) adds the detail for finding a problem: each step of a reading, each call to an AI, how long
it took. Never anything a person scanned or asked, and never a key: the AI companies' libraries
are held to warnings (at debug they print whole requests), and every message passes `redact`.
"""

from __future__ import annotations

import copy
import logging
import os
import platform
import re
import sys
import threading
from pathlib import Path
from urllib.parse import urlparse

from uvicorn.config import LOGGING_CONFIG

from lindley import __version__
from lindley.config import data_folder

LEVEL_ENV_VAR = "LINDLEY_LOG_LEVEL"
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
LOG_BYTES = 5_000_000  # each log file, of three
# Libraries that print whole requests (a page's text, its image) when asked to be chatty: held
# to warnings, however they're asked
QUIET = ("openai", "anthropic", "httpx", "httpx2", "httpcore", "google", "urllib3", "PIL")
# The screens ask these every few seconds: left out of the access log, or it would be all they say
POLLED = ("/api/overview", "/api/health")

_SECRETS = (
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}"), "sk-***"),
    (re.compile(r"\bAIza[0-9A-Za-z_\-]{20,}"), "AIza***"),
    (re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=\-]{8,}"), r"\1***"),
    (
        re.compile(
            r"(?i)((?:api[_-]?key|x-api-key|x-goog-api-key|authorization|password|secret)"
            r"[\"']?\s*[:=]\s*[\"']?)(?!bearer\b)[^\s\"',&;]+"
        ),
        r"\1***",
    ),
    (re.compile(r"(?i)([?&](?:key|token|api[_-]?key)=)[^&\s]+"), r"\1***"),
)


def redact(text: str) -> str:
    """The text with anything that looks like a key, token or password hidden."""
    for pattern, replacement in _SECRETS:
        text = pattern.sub(replacement, text)
    return text


def web_address(url: str | None) -> str:
    """An address as far as it says where: no user name, password, path or query, which can
    carry a key. For messages and logs; "" when there's no address to show."""
    try:
        parts = urlparse(url or "")
        host, port = parts.hostname, parts.port
    except ValueError:  # a port that isn't one
        return ""
    if not host:
        return ""
    return f"{parts.scheme}://{host}" + (f":{port}" if port else "")


class PrivacyFilter(logging.Filter):
    """Hides keys in what's logged, and drops the chatty libraries' lower levels."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.split(".")[0] in QUIET and record.levelno < logging.WARNING:
            return False
        if record.name == "uvicorn.access":  # its arguments are read by its own format
            return True
        message = record.getMessage()
        hidden = redact(message)
        if hidden != message:
            record.msg, record.args = hidden, None
        if record.exc_info and not record.exc_text:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
        return True


class PollingFilter(logging.Filter):
    """Leaves the screens' regular questions out of the access log (answered fine)."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) == 5:
            _, method, path, _, status = args
            if method == "GET" and str(path).split("?")[0] in POLLED and int(status) < 400:
                return False
        return True


def level_from(debug: bool = False, environ: os._Environ | dict | None = None) -> str:
    """The level to log at: debug if asked on the command line, else LINDLEY_LOG_LEVEL if it
    names one, else INFO."""
    if debug:
        return "DEBUG"
    asked = (environ if environ is not None else os.environ).get(LEVEL_ENV_VAR, "")
    asked = asked.strip().upper()
    if asked == "WARN":
        asked = "WARNING"
    return asked if asked in LEVELS else "INFO"


def default_log_file() -> Path:
    """Where Lindley logs before it knows where its database is (settings it couldn't read)."""
    return data_folder() / "logs" / "lindley.log"


def log_config(file: Path | None = None, level: str = "INFO", rotate: bool = True) -> dict:
    """Uvicorn's own logging, with Lindley's messages beside its requests. With a file, they're
    kept there too: an app started from the menu has no console. `rotate=False` is for the
    reloading dev server, where two processes write the one file."""
    config = copy.deepcopy(LOGGING_CONFIG)
    config["filters"] = {
        "privacy": {"()": "lindley.logs.PrivacyFilter"},
        "polling": {"()": "lindley.logs.PollingFilter"},
    }
    for handler in ("default", "access"):
        config["handlers"][handler]["filters"] = ["privacy"]
    handlers = ["default"]
    config["loggers"]["lindley"] = {"handlers": ["default"], "level": level, "propagate": False}
    if level != "DEBUG":
        config["loggers"]["uvicorn.access"]["filters"] = ["polling"]
    if file is not None:
        file.parent.mkdir(parents=True, exist_ok=True)
        config["formatters"]["file"] = {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"}
        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler" if rotate else "logging.FileHandler",
            "formatter": "file",
            "filename": str(file),
            "encoding": "utf-8",
            "filters": ["privacy"],
        }
        if rotate:
            config["handlers"]["file"].update(maxBytes=LOG_BYTES, backupCount=2)
        handlers.append("file")
        for name in ("lindley", "uvicorn", "uvicorn.access"):
            config["loggers"][name]["handlers"].append("file")
    # Whatever else logs (a library's warning, a thread's crash) is kept too, not lost
    config["root"] = {"handlers": handlers, "level": "WARNING"}
    for name in QUIET:
        config["loggers"][name] = {"level": "WARNING"}
    return config


def install_excepthooks() -> None:
    """What nothing caught, in the log with its traceback: with no console it would be lost."""
    log = logging.getLogger("lindley")

    def main_thread(exc_type, exc, tb) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc, tb)
            return
        log.critical("Lindley stopped on an error nothing caught", exc_info=(exc_type, exc, tb))

    def other_thread(args: threading.ExceptHookArgs) -> None:
        if args.exc_type is SystemExit:
            return
        name = args.thread.name if args.thread else "?"
        log.critical(
            "A background thread (%s) stopped on an error nothing caught",
            name,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = main_thread
    threading.excepthook = other_thread


def banner(settings_path: Path, found: bool, db_path: Path, file: Path | None, level: str) -> None:
    """The first lines of a run: what's running, and where it keeps things."""
    log = logging.getLogger("lindley")
    log.info(
        "Lindley %s on Python %s, %s %s",
        __version__,
        platform.python_version(),
        platform.system(),
        platform.release(),
    )
    log.info("Settings: %s (%s)", settings_path, "read" if found else "none yet, so the defaults")
    log.info("Database: %s", db_path)
    log.info("Log: %s, level %s", file if file else "the console only", level)
