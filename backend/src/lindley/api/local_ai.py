"""Lindley's own AI: what's downloaded, downloading what a person chooses, and removing a model.

Downloads run in the background, one at a time (lindley.localai.download); the status bar shows
them, and they're said done in a toast, like other work a person asked for.
"""

from __future__ import annotations

import shutil

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from lindley import activity
from lindley.config import Settings
from lindley.localai import server as local_server
from lindley.localai.catalog import MODELS, engine
from lindley.providers.registry import connectors

router = APIRouter(prefix="/local-ai", tags=["local-ai"])


def _free(path) -> int | None:
    """Bytes free on the disk the models go on (the nearest folder that's there)."""
    for p in (path, *path.parents):
        if p.exists():
            return shutil.disk_usage(p).free
    return None


@router.get("")
def status(request: Request) -> dict:
    settings: Settings = request.app.state.settings
    root = settings.ai.local.folder()
    asked = set(request.app.state.downloads.asked())
    e = engine()
    downloading = next((w for w in activity.current() if w["kind"] == "download"), None)
    return {
        "folder": str(root),
        "free": _free(root),
        "engine": {
            "build": e.build if e else None,
            "size": e.file.size if e else 0,
            "ready": bool(e and e.installed(root)),
        },
        "models": [
            {
                "id": m.id,
                "label": m.label,
                "size": m.size,
                "memory_gb": m.memory_gb,
                "jobs": sorted(m.jobs),
                "state": "ready"
                if m.installed(root)
                else "downloading"
                if m.id in asked
                else "missing",
            }
            for m in MODELS.values()
        ],
        "downloading": (
            {"label": downloading["label"], "done": downloading["done"], "of": downloading["of"]}
            if downloading
            else None
        ),
        "running": local_server.current().running(),
    }


class Models(BaseModel):
    models: list[str]


@router.post("/download")
def download(body: Models, request: Request) -> dict:
    """Download these models, and the engine if it's missing. Only ever because a person asked."""
    if engine() is None:
        raise HTTPException(409, "Lindley's own AI doesn't run on this kind of computer yet")
    unknown = [m for m in body.models if m not in MODELS]
    if unknown:
        raise HTTPException(404, f"Lindley's own AI has no model called {unknown[0]!r}")
    return {"queued": request.app.state.downloads.start(body.models)}


@router.post("/cancel")
def cancel(request: Request) -> dict:
    """Stop downloading. What came down so far is kept, and the next download goes on from it."""
    request.app.state.downloads.cancel()
    return {"ok": True}


@router.delete("/models/{model_id}")
def remove(model_id: str, request: Request) -> dict:
    """Delete a downloaded model, to free the disk. Not one a job uses."""
    settings: Settings = request.app.state.settings
    m = MODELS.get(model_id)
    if m is None:
        raise HTTPException(404, f"Lindley's own AI has no model called {model_id!r}")
    for job, j in settings.ai.jobs.items():
        cfg = settings.ai.providers.get(j.connection) if j.connection else None
        usual = connectors()["builtin"].info.default_models.get(job)
        if cfg is not None and cfg.type == "builtin" and (j.model or usual) == model_id:
            raise HTTPException(409, f"{m.label} is in use ({job}). Choose another AI first.")
    if model_id in request.app.state.downloads.asked():
        raise HTTPException(409, f"{m.label} is downloading. Cancel it first.")
    local_server.current().stop()  # Windows can't delete a file the server has open
    shutil.rmtree(m.folder(settings.ai.local.folder()), ignore_errors=True)
    return {"removed": model_id}
