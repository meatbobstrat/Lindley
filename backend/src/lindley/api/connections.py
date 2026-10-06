"""AI connectors and connections: what can be connected, performance tiers, keys, and Test
connection."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from lindley.config import ProviderConfig, Settings, resolve_settings_path
from lindley.providers import keys
from lindley.providers.base import JOBS, Job, ProviderError
from lindley.providers.registry import connectors
from lindley.providers.tiers import TIERS

router = APIRouter(tags=["connections"])


@router.get("/connectors")
def list_connectors() -> list[dict]:
    """Every connector a person can choose, from the files in providers/connectors/. The UI
    builds its forms and job choices from these, so a new file needs no UI change."""
    out = []
    for c in connectors().values():
        i = c.info
        if i.hidden:
            continue
        out.append(
            {
                "id": i.id,
                "label": i.label,
                "short": i.short or i.label,
                "where": i.where,
                "company": i.company,
                "jobs": [j for j in JOBS if j in i.jobs],
                "default_models": dict(i.default_models),
                "default_base_url": i.default_base_url,
                "needs_key": i.needs_key,
                "key_url": i.key_url,
            }
        )
    return out


@router.get("/tiers")
def list_tiers() -> list[dict]:
    """The performance tiers a person can choose from, in providers/tiers.py. The UI sets each
    job's AI from the one chosen."""
    return [
        {
            "id": t.id,
            "label": t.label,
            "needs": t.needs,
            "does": t.does,
            "runs": t.runs,
            "models": dict(t.models),
        }
        for t in TIERS
    ]


@router.get("/setup")
def setup_needed(request: Request) -> dict:
    """First-run setup is needed until there's a settings file."""
    return {"needed": not resolve_settings_path(request.app.state.settings_path).exists()}


class Key(BaseModel):
    key: str = Field(min_length=1)


def _connection(request: Request, name: str) -> ProviderConfig:
    settings: Settings = request.app.state.settings
    if name not in settings.ai.providers:
        raise HTTPException(404, f"There's no AI connection called {name!r}")
    return settings.ai.providers[name]


@router.put("/connections/{name}/key")
def save_key(name: str, body: Key, request: Request) -> dict:
    """Save a connection's key in the credential store. It's never sent back: only its hint."""
    _connection(request, name)
    try:
        keys.set_key(name, body.key.strip())
    except ProviderError as e:
        raise HTTPException(500, str(e)) from e
    return {"key_hint": keys.key_hint(name)}


@router.delete("/connections/{name}/key")
def forget_key(name: str, request: Request) -> dict:
    _connection(request, name)
    keys.delete_key(name)
    return {"key_hint": None}


class Draft(BaseModel):
    """A connection as it's being set up, perhaps not saved yet."""

    connection: ProviderConfig
    name: str | None = None  # its id, if it's saved: its saved key is used
    key: str | None = None  # a key typed but not saved yet
    job: Job | None = None  # the model to look for: this job's (default: the first it can do)


@router.post("/connections/test")
def try_connection(draft: Draft, request: Request) -> dict:
    """Try the connection: a cheap call that checks the address, the key and the model."""
    connector = connectors().get(draft.connection.type)
    if connector is None:
        raise HTTPException(422, f"Unknown provider type: {draft.connection.type}")
    info, cfg = connector.info, draft.connection
    job = draft.job or next(j for j in JOBS if j in info.jobs)
    model = cfg.model if job != "embed" and cfg.model else info.default_models.get(job)
    # A saved key only goes where it was saved for: not to an address changed since
    saved: Settings = request.app.state.settings
    was = saved.ai.providers.get(draft.name) if draft.name else None
    name = draft.name if was and (was.type, was.base_url) == (cfg.type, cfg.base_url) else None
    key = (draft.key or "").strip() or cfg.api_key(name)
    if info.needs_key and not key:
        return {"ok": False, "message": f"Paste your API key from {info.company or info.label}."}
    try:
        message = connector.provider(config=cfg, model=model, api_key=key).check()
    except ProviderError as e:
        return {"ok": False, "message": str(e)}
    return {"ok": True, "message": message}
