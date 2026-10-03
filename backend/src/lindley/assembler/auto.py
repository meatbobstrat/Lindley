"""What Lindley does with an AI on its own, without a person asking: the watcher's work.

An AI is only called on its own when its connection's `allow` is "auto", and only while that
connection's daily and monthly limits aren't used up (lindley.providers.allowance). Each call
is recorded as automatic. What it may not do on its own waits in Needs AI (api/needs_ai.py),
for a person to send.
"""

from __future__ import annotations

import logging
import sqlite3

from lindley.assembler.run import RunReport, assemble
from lindley.config import Settings
from lindley.providers import allowance
from lindley.providers.base import ChatProvider, ProviderError
from lindley.providers.registry import get_provider
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


def sort_on_its_own(
    conn: sqlite3.Connection, settings: Settings, chat: ChatProvider | None = None
) -> RunReport:
    """Sort the Inbox, with the AI if it may run on its own now. `chat`: the sorting AI from
    chat_on_its_own, if the caller keeps one; else it's made here."""
    chat = chat if chat is not None else chat_on_its_own(settings)
    name = settings.ai.connection_for("assemble")
    left = allowance.automatic_left(conn, settings, name) if chat else 0
    report = assemble(conn, settings.assembler, chat if left is None or left > 0 else None, left)
    if report.ai_calls:
        with conn:
            allowance.record(conn, name, "assemble", True, count=report.ai_calls)
    return report


def read_on_its_own(
    conn: sqlite3.Connection, settings: Settings, pipeline: Pipeline
) -> WaitingRun | None:
    """Send pages waiting for the vision model, if it may run on its own now: pages that
    arrived while it had to ask, or after its limits were used up. Pages whose call failed
    still wait for a person. None: nothing was sent."""
    name = settings.ai.connection_for("vision")
    if pipeline.vision is None or not waiting_for_vision(conn):
        return None
    left = allowance.automatic_left(conn, settings, name)
    if left == 0:
        return None
    run = pipeline.read_waiting(conn, automatic=True, limit=left)
    if run.stopped:
        log.info("Reading hard pages on its own: %s", run.stopped)
    return run
