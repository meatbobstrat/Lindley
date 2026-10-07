"""Whether Ask Lindley can answer questions, decided here and nowhere else.

It can when the chat job has a connection that can be used: a cloud AI with its key saved, or an
AI on computers a person controls (this one, or a server of their own). With none, the pane only
finds words and what's waiting for review. Which AI does the job is a person's choice in Settings
(or, later, the performance tier picked for the computer): this never looks at the hardware.

Nothing is sent to find out. An AI on this computer that isn't running is found when a question
is asked, and the answer says so.
"""

from __future__ import annotations

from typing import Literal, TypedDict

from lindley.config import ProviderConfig, Settings
from lindley.localai.catalog import MODELS
from lindley.providers.base import ProviderError
from lindley.providers.registry import build_provider, connectors

State = Literal["ready", "broken", "offer", "none"]


class Status(TypedDict):
    state: State
    connection: dict | None  # the chat job's connection: name, label, where, company
    reason: str  # in words, for a person
    offers: list[dict]  # connections that could answer, when none is chosen


def _about(name: str, cfg: ProviderConfig) -> dict:
    c = connectors().get(cfg.type)
    info = c.info if c else None
    return {
        "name": name,
        "label": cfg.label or (info.short or info.label if info else name),
        "where": info.where if info else "cloud",
        "company": info.company if info else None,
    }


def _problem(name: str, cfg: ProviderConfig, model: str | None = None) -> str | None:
    """Why this connection can't answer questions now, or None if it can."""
    c = connectors().get(cfg.type)
    if c is None:
        return f"Lindley has no connector for {cfg.type!r}"
    if "chat" not in c.info.jobs:
        return f"{c.info.company or c.info.label} can't answer questions"
    if c.info.needs_key and not cfg.api_key(name):
        return f"No API key is saved for {c.info.company or c.info.label}"
    try:
        p = build_provider(cfg, "chat", model, name=name)
        if cfg.type == "builtin":  # whether its model is downloaded: nothing is sent
            p.check()
    except ProviderError as e:
        return str(e)
    return None


def chat_status(settings: Settings) -> Status:
    ai = settings.ai
    name = ai.connection_for("chat")
    if name:
        cfg = ai.providers.get(name)
        if cfg is None:
            return Status(
                state="broken",
                connection=None,
                reason=f"The connection {name!r} chosen for Ask Lindley is gone",
                offers=[],
            )
        problem = _problem(name, cfg, ai.jobs["chat"].model if "chat" in ai.jobs else None)
        return Status(
            state="broken" if problem else "ready",
            connection=_about(name, cfg),
            reason=problem or "Ready",
            offers=[],
        )
    offers = [_about(n, c) for n, c in ai.providers.items() if _problem(n, c) is None]
    if offers:
        return Status(
            state="offer",
            connection=None,
            reason="No AI is chosen for Ask Lindley yet",
            offers=offers,
        )
    return Status(
        state="none", connection=None, reason="No AI that can answer questions is set up", offers=[]
    )


def budget(settings: Settings) -> int:
    """Characters of page text that go with a question, for the AI answering it."""
    name = settings.ai.connection_for("chat")
    cfg = settings.ai.providers.get(name) if name else None
    c = connectors().get(cfg.type) if cfg else None
    if cfg is not None and cfg.type == "builtin":
        # Lindley's own AI: the model's context is known. Half of it for the pages, at about
        # 3 characters a token; the rest for the question, earlier turns and the answer.
        job = settings.ai.jobs.get("chat")
        model = MODELS.get((job and job.model) or c.info.default_models["chat"])
        if model is not None:
            return min(settings.ask.cloud_chars, model.context // 2 * 3)
    local = c is not None and c.info.where == "local"
    return settings.ask.local_chars if local else settings.ask.cloud_chars
