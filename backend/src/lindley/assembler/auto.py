"""What Lindley does with an AI on its own, without a person asking: the watcher's work.

An AI is only called on its own when its connection's `allow` is "auto", and only while that
connection's daily and monthly limits aren't used up (lindley.providers.allowance). Each call
is recorded as automatic. What it may not do on its own waits in Needs AI (api/needs_ai.py),
for a person to send.
"""

from __future__ import annotations

import logging
import sqlite3

from lindley import activity
from lindley.assembler.answers import Progress
from lindley.assembler.run import RunReport, assemble
from lindley.config import Settings
from lindley.providers import allowance
from lindley.providers.base import ChatProvider, ProviderError, Usage
from lindley.providers.registry import connectors, get_provider
from lindley.worker.pipeline import Pipeline, WaitingRun, waiting_for_vision

log = logging.getLogger(__name__)


def chat_on_its_own(settings: Settings) -> ChatProvider | None:
    """The sorting AI, if it may ever run on its own; each run checks what's left of its
    limits."""
    cfg = allowance.provider_config(settings, settings.ai.connection_for("assemble"))
    if cfg is None or cfg.allow != "auto":
        return None
    try:
        return get_provider(settings.ai, "assemble")
    except ProviderError:
        return None


def judge_for(settings: Settings):
    """The AI that checks whether pages carry on from one another (the continues job), if it
    runs on a computer a person controls. That check is part of the rules' own sorting, many
    quick questions that cost nothing, so it runs on its own whatever the connection's
    `allow`. None: there's none, or its model isn't downloaded."""
    cfg = allowance.provider_config(settings, settings.ai.connection_for("continues"))
    c = connectors().get(cfg.type) if cfg else None
    if c is None or c.info.where != "local":
        return None
    try:
        judge = get_provider(settings.ai, "continues")
        if cfg.type == "builtin":  # whether its model is downloaded: nothing is started
            judge.check()
    except ProviderError:
        return None
    return judge


def sort_on_its_own(
    conn: sqlite3.Connection, settings: Settings, chat: ChatProvider | None = None
) -> RunReport:
    """Sort the Inbox, with the AI if it may run on its own now. `chat`: the sorting AI from
    chat_on_its_own, if the caller keeps one; else it's made here."""
    chat = chat if chat is not None else chat_on_its_own(settings)
    name = settings.ai.connection_for("assemble")
    if chat is not None and allowance.failing(conn, name):
        chat = None  # its last calls all failed: the rules sort alone for a while
    left = allowance.automatic_left(conn, settings, name) if chat else 0
    if chat is None or left == 0:
        return sort_with(conn, settings, None, True)
    # Shown in the status bar only once there's a question for the AI: mostly there's none
    label = _label(settings, name)
    with activity.doing("sort", 0, asked=False, connection=label, quiet=True) as step:
        return sort_with(conn, settings, chat, True, progress=step, max_ai_calls=left)


def sort_with(
    conn: sqlite3.Connection,
    settings: Settings,
    chat: ChatProvider | None,
    automatic: bool,
    progress: Progress | None = None,
    **kw,
) -> RunReport:
    """Sort the Inbox with `chat`, recording each call to it as it's made, whether it worked or
    failed: a job cut short, or a sort that then fails, still shows what it spent. `progress`
    is told how many calls are made, of how many are coming. The checks whether pages carry on
    are made too (judge_for), and recorded as theirs."""
    name = settings.ai.connection_for("assemble")
    checker = settings.ai.connection_for("continues")

    def record(ok: bool, used: list[Usage]) -> None:
        allowance.record(conn, name, "assemble", automatic, ok=ok, used=used)

    def judged(ok: bool, used: list[Usage]) -> None:
        allowance.record(conn, checker, "continues", True, ok=ok, used=used)

    return assemble(
        conn,
        settings.assembler,
        chat,
        on_call=record,
        on_progress=progress,
        judge=judge_for(settings),
        on_judge=judged,
        **kw,
    )


def _label(settings: Settings, name: str | None) -> str | None:
    """The name people see for a connection, as the status bar gives it."""
    cfg = settings.ai.providers.get(name) if name else None
    return (cfg.label if cfg else None) or name


def read_on_its_own(
    conn: sqlite3.Connection, settings: Settings, pipeline: Pipeline
) -> WaitingRun | None:
    """Send pages waiting for the vision model, if it may run on its own now: pages that
    arrived while it had to ask, or after its limits were used up. Pages whose call failed
    still wait for a person, and an AI whose calls keep failing is left alone for a while.
    None: nothing was sent. The watcher asks every second, so the quick checks go first."""
    name = settings.ai.connection_for("vision")
    if pipeline.vision is None:
        return None
    left = allowance.automatic_left(conn, settings, name)
    if left == 0 or allowance.failing(conn, name) or not waiting_for_vision(conn):
        return None
    with activity.doing("read", 0, asked=False, connection=_label(settings, name)) as step:
        run = pipeline.read_waiting(conn, automatic=True, limit=left, progress=step)
    if run.stopped:
        log.info("Reading hard pages on its own: %s", run.stopped)
    return run
