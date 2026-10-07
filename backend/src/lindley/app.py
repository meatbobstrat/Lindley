"""FastAPI application factory."""

from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI
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
from lindley.api import settings as settings_api
from lindley.config import Settings, load_settings
from lindley.db.database import connect, init_db
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

# Built frontend (frontend/dist), served in production so the app is a single process.
FRONTEND_DIST = Path(__file__).resolve().parents[3] / "frontend" / "dist"

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
                refused = JSONResponse({"detail": "Only Lindley's own pages can do that"}, 403)
                await refused(scope, receive, send)
                return
        await self.app(scope, receive, send)


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
            recover_interrupted(conn)
            remove_leftovers(settings)
            rate_vision_readings(conn)
            absolute_paths(conn)
            follow_settings(conn, settings)  # settings.json may have changed while it was closed
        finally:
            conn.close()
        watcher = FolderWatcher(settings) if watch else None
        if watcher:
            watcher.start()
        app.state.watcher = watcher
        local_server.use(settings.ai.local)  # started when a job first needs it
        try:
            yield
        finally:
            # The watcher in use now: saving settings starts a new one (api/settings.py)
            with app.state.watcher_swap:
                if app.state.watcher:
                    app.state.watcher.stop()
            app.state.ai_work.stop()
            app.state.downloads.stop()
            local_server.stop()

    app = FastAPI(title="Lindley", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.settings_path = settings_path
    app.state.watcher_swap = threading.Lock()  # one watcher at a time is started or stopped
    # AI work a person asked for, done in the background with the settings in use then
    app.state.ai_work = AiWork(lambda: app.state.settings)
    # Lindley's own AI's files, downloaded when a person asks, into the folder in use then
    app.state.downloads = Downloads(lambda: app.state.settings.ai.local.folder())

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
    ):
        app.include_router(router, prefix="/api")

    if FRONTEND_DIST.is_dir():
        app.mount("/", Frontend(directory=FRONTEND_DIST, html=True), name="frontend")

    return app
