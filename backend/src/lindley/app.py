"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from lindley import __version__
from lindley.api import assembler as assembler_api
from lindley.api import (
    chat,
    connections,
    documents,
    duplicates,
    health,
    needs_ai,
    pages,
    search,
    suggestions,
)
from lindley.api import history as history_api
from lindley.api import settings as settings_api
from lindley.config import Settings, load_settings
from lindley.db.database import connect, init_db
from lindley.watcher.watcher import FolderWatcher
from lindley.worker.intake import absolute_paths
from lindley.worker.pipeline import follow_settings, recover_interrupted

# Built frontend (frontend/dist), served in production so the app is a single process.
FRONTEND_DIST = Path(__file__).resolve().parents[3] / "frontend" / "dist"

DEV_ORIGINS = ["http://localhost:5173", "http://127.0.0.1:5173"]


def create_app(
    settings: Settings | None = None, settings_path: Path | None = None, watch: bool = True
) -> FastAPI:
    """`watch=False` leaves the folder watcher off (tests, or serving without intake)."""
    settings = settings or load_settings(settings_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.db_path)
        conn = connect(settings.db_path)
        try:  # nothing else is running yet: whatever was running was cut off
            recover_interrupted(conn)
            absolute_paths(conn)
            follow_settings(conn, settings)  # settings.json may have changed while it was closed
        finally:
            conn.close()
        watcher = FolderWatcher(settings) if watch else None
        if watcher:
            watcher.start()
        app.state.watcher = watcher
        try:
            yield
        finally:
            if watcher:
                watcher.stop()

    app = FastAPI(title="Lindley", version=__version__, lifespan=lifespan)
    app.state.settings = settings
    app.state.settings_path = settings_path

    app.add_middleware(
        CORSMiddleware,
        allow_origins=DEV_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    for router in (
        health.router,
        settings_api.router,
        connections.router,
        documents.router,
        search.router,
        chat.router,
        duplicates.router,
        pages.router,
        history_api.router,
        suggestions.router,
        assembler_api.router,
        needs_ai.router,
    ):
        app.include_router(router, prefix="/api")

    if FRONTEND_DIST.is_dir():
        app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")

    return app
