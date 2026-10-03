from fastapi import APIRouter, Request

from lindley.api.deps import Conn
from lindley.config import Settings, save_settings
from lindley.providers import allowance

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
def get_settings(request: Request) -> Settings:
    return request.app.state.settings


@router.put("")
def put_settings(new: Settings, request: Request) -> Settings:
    save_settings(new, request.app.state.settings_path)
    request.app.state.settings = new
    return new


@router.get("/ai-calls")
def ai_calls_today(request: Request, conn: Conn) -> dict:
    """For each AI connection: when it may be used, and today's calls, on its own and OKed."""
    settings: Settings = request.app.state.settings
    out = {}
    for name, cfg in settings.ai.providers.items():
        out[name] = {
            "allow": cfg.allow,
            "daily_limit": cfg.daily_limit,
            "automatic_today": allowance.calls_today(conn, name, automatic=True),
            "oked_today": allowance.calls_today(conn, name, automatic=False),
            "automatic_left": allowance.automatic_left(conn, settings, name),
        }
    return {"providers": out}
