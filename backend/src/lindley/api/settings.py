from fastapi import APIRouter, HTTPException, Request

from lindley.api.deps import Conn
from lindley.config import Settings, save_settings
from lindley.providers import allowance, keys
from lindley.providers.registry import connectors
from lindley.watcher.watcher import FolderWatcher

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def _problems(new: Settings) -> list[str]:
    """What's wrong with the AI settings: unknown connectors, and jobs given to a connection
    that isn't there or can't do them."""
    known, out = connectors(), []
    for name, cfg in new.ai.providers.items():
        if cfg.type not in known:
            out.append(f"The AI connection {name!r} has an unknown type, {cfg.type!r}")
    for job, j in new.ai.jobs.items():
        cfg = new.ai.providers.get(j.connection) if j.connection else None
        if j.connection and cfg is None:
            out.append(f"The job {job!r} is given to {j.connection!r}, which isn't set up")
        elif cfg is not None and cfg.type in known and job not in known[cfg.type].info.jobs:
            info = known[cfg.type].info
            out.append(f"{info.company or info.label} can't do the job {job!r}")
    return out


@router.put("")
def put_settings(new: Settings, request: Request) -> Settings:
    if problems := _problems(new):
        raise HTTPException(422, problems)
    old: Settings = request.app.state.settings
    save_settings(new, request.app.state.settings_path)
    request.app.state.settings = new
    if (watcher := getattr(request.app.state, "watcher", None)) is not None:
        # The watcher works from the settings it started with: start it again with these, so
        # new folders are watched, and an AI that may now run on its own gets what's waiting.
        watcher.stop()
        request.app.state.watcher = FolderWatcher(new)
        request.app.state.watcher.start()
    for name in set(old.ai.providers) - set(new.ai.providers):
        keys.delete_key(name)  # a connection removed takes its key with it
    return new


@router.get("/ai-calls")
def ai_calls_today(request: Request, conn: Conn) -> dict:
    """For each AI connection: its limits, and its calls today and this month, on its own and
    OKed."""
    settings: Settings = request.app.state.settings
    out = {}
    for name, cfg in settings.ai.providers.items():
        out[name] = {
            "allow": cfg.allow,
            "daily_limit": cfg.daily_limit,
            "monthly_limit": cfg.monthly_limit,
            "per_minute": cfg.per_minute,
            "at_once": cfg.at_once,
            "automatic_today": allowance.calls_today(conn, name, automatic=True),
            "oked_today": allowance.calls_today(conn, name, automatic=False),
            "automatic_month": allowance.calls_this_month(conn, name, automatic=True),
            "oked_month": allowance.calls_this_month(conn, name, automatic=False),
            "automatic_left": allowance.automatic_left(conn, settings, name),
            "key_hint": keys.key_hint(name),
        }
    return {"providers": out}
