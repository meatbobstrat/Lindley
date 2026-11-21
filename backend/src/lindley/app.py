"""FastAPI application factory."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from lindley import __version__
from lindley.api import assembler as assembler_api
from lindley.api import (
    chat,
    connections,
    documents,
    duplicates,
    folders,
    health,
    library,
    local_ai,
    needs_ai,
    pages,
    scans,
    search,
    suggestions,
)
from lindley.api import history as history_api
from lindley.api import (
    log as log_api,
)
from lindley.api import settings as settings_api
from lindley.config import Settings, load_settings
from lindley.db.database import connect, init_db
from lindley.export.keeping import let_go
from lindley.localai import server as local_server
from lindley.localai.download import Downloads
from lindley.watcher.watcher import FolderWatcher
from lindley.worker.ai_work import AiWork
from lindley.worker.intake import absolute_paths
from lindley.worker.pipeline import (
    follow_settings,
    rate_vision_readings,
    recover_interrupted,
    remove_leftovers,
)

# The built frontend, served so the app is a single process: inside the package once installed
# (the installer copies frontend/dist to lindley/web), else frontend/dist in the repo.
log = logging.getLogger(__name__)

_HERE = Path(__file__).resolve().parent
FRONTEND_DIRS = (_HERE / "web", _HERE.parents[2] / "frontend" / "dist")

DEV_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]

# The address the server listens on (python -m lindley --host): set for --reload, which makes
# the app afresh on each change.
HOST_ENV_VAR = "LINDLEY_HOST"
LOCAL_NAMES = ["127.0.0.1", "localhost"]


def allowed_hosts(host: str) -> list[str]:
    """The names Lindley answers to. Listening only on this computer, only its own names: then a
    web page can't reach Lindley by pointing a name of its own at this computer (DNS rebinding).
    Listening on a network, any name: whoever chose that knows who can reach it."""
    if host in LOCAL_NAMES or host.startswith("127."):
        return sorted({*LOCAL_NAMES, host})
    return ["*"]


class FromLindleyOnly:
    """Changes come only from Lindley's own pages. A browser says which page sent a request
    (Origin); one from another site is refused, so a page a person happens to visit can't add
    scans, undo their work or send pages to an AI. Reading isn't affected, and nor is a request
    with no Origin (scripts, curl)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["method"] not in ("GET", "HEAD", "OPTIONS"):
            headers = Headers(scope=scope)
            origin = headers.get("origin")
            if (
                origin is not None
                and origin not in DEV_ORIGINS
                and urlparse(origin).netloc != headers.get("host")
            ):
                log.warning(
                    "Refused %s %s from another site (%r)", scope["method"], scope["path"], origin
                )
                refused = JSONResponse({"detail": "Only Lindley's own pages can do that"}, 403)
                await refused(scope, receive, send)
                return
        await self.app(scope, receive, send)


def _stopped(what: str, stop: Callable[[], object]) -> None:
    try:
        stop()
    except Exception:
        log.exception("Couldn't stop %s cleanly", what)


def _asked(request: Request) -> str:
    return f"{request.method} {request.url.path}"


def _database_busy(request: Request, e: Exception) -> JSONResponse:
    """Another writer (the watcher reading a long scan, a script) held the database past the
    wait: said so, not a failure. Try again."""
    if "locked" in str(e) or "busy" in str(e):
        log.warning("%s: the database was busy: %s", _asked(request), e)
        return JSONResponse(
            {"detail": "Lindley's library is busy just now. Try that again in a moment."}, 503
        )
    return _database_failed(request, e)


def _database_failed(request: Request, e: Exception) -> JSONResponse:
    log.error("%s: the database failed: %s", _asked(request), e, exc_info=e)
    return JSONResponse(
        {"detail": "Lindley's library couldn't do that. The details are in Lindley's log."}, 500
    )


def _unexpected(request: Request, e: Exception) -> JSONResponse:
    """Whatever nothing else met: a short, plain answer for the screen, not the server's own.
    Uvicorn logs the traceback; this says which request it was."""
    log.error("%s failed: %s: %s", _asked(request), type(e).__name__, e)
    return JSONResponse(
        {"detail": "Something went wrong inside Lindley. The details are in Lindley's log."}, 500
    )


class Frontend(StaticFiles):
    """The built frontend. It finds its pages in the browser (/inbox, /documents/5...), so an
    address that's none of its files gets index.html, to open there: opened directly, or the
    page reloaded. A missing file (a name with a dot) and an unknown /api address are still 404."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        try:
            return await super().get_response(path, scope)
        except HTTPException as e:
            parts = path.replace("\\", "/").split("/")  # a path from the OS: \ on Windows
            if e.status_code != 404 or parts[0] == "api" or "." in parts[-1]:
                raise
            return await super().get_response("index.html", scope)


def create_app(
    settings: Settings | None = None,
    settings_path: Path | None = None,
    watch: bool = True,
    host: str | None = None,
) -> FastAPI:
    """`watch=False` leaves the folder watcher off (tests, or serving without intake). `host`:
    the address the server listens on (default 127.0.0.1)."""
    settings = settings or load_settings(settings_path)
    host = host or os.environ.get(HOST_ENV_VAR) or LOCAL_NAMES[0]

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.db_path)
        conn = connect(settings.db_path)
        try:  # nothing else is running yet: whatever was running was cut off
            log.debug("Recovered interrupted work: %s", recover_interrupted(conn))
            log.debug("Removed %s leftover work file(s)", remove_leftovers(settings))
            log.debug("Rated %s vision reading(s)", rate_vision_readings(conn))
            log.debug("Made %s path(s) absolute", absolute_paths(conn))
            # settings.json may have changed while it was closed
            log.debug("Followed the settings: %s", follow_settings(conn, settings))
            # exported before, or a copy that was open then
            log.debug("Let go of %s", let_go(conn, settings.library_dir))
        finally:
            conn.close()
        watcher = FolderWatcher(settings) if watch else None
        if watcher:
            watcher.start()
        app.state.watcher = watcher
        log.info("Lindley is ready%s", "" if watcher else " (without the folder watcher)")
        try:
            local_server.use(settings.ai.local)  # started when a job first needs it
            yield
        finally:
            log.info("Lindley is stopping")
            # Each in turn, whatever the one before did: a watcher that won't stop must not
            # leave Lindley's own AI running with nobody to end it
            with app.state.watcher_swap:  # the watcher in use now: saving settings starts a new one
                if app.state.watcher:
                    _stopped("the folder watcher", app.state.watcher.stop)
            _stopped("the AI work", app.state.ai_work.stop)
            _stopped("the downloads", app.state.downloads.stop)
            _stopped("Lindley's own AI", local_server.stop)

    app = FastAPI(title="Lindley", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.settings_path = settings_path
    app.state.watcher_swap = threading.Lock()  # one watcher at a time is started or stopped
    # AI work a person asked for, done in the background with the settings in use then
    app.state.ai_work = AiWork(lambda: app.state.settings)
    # Lindley's own AI's files, downloaded when a person asks, into the folder in use then
    app.state.downloads = Downloads(lambda: app.state.settings.ai.local.folder())

    app.add_exception_handler(sqlite3.OperationalError, _database_busy)
    app.add_exception_handler(sqlite3.DatabaseError, _database_failed)
    app.add_exception_handler(Exception, _unexpected)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=DEV_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(FromLindleyOnly)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts(host))  # outermost

    for router in (
        health.router,
        library.router,
        folders.router,
        settings_api.router,
        connections.router,
        documents.router,
        search.router,
        chat.router,
        duplicates.router,
        pages.router,
        scans.router,
        history_api.router,
        suggestions.router,
        assembler_api.router,
        needs_ai.router,
        local_ai.router,
        log_api.router,
    ):
        app.include_router(router, prefix="/api")

    if built := next((d for d in FRONTEND_DIRS if d.is_dir()), None):
        app.mount("/", Frontend(directory=built, html=True), name="frontend")

    return app
