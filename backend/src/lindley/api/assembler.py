"""Sorting the Inbox on a person's say-so.

POST /api/assembler/ask sends the pages a person chose to the AI that sorts pages, at once:
asking is the OK, whatever the connection's `allow`, and the call is recorded as one a person
asked for. An answer the AI gave before about the same pages is used again, with no call.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from lindley.api.deps import Conn
from lindley.assembler import assemble
from lindley.providers import allowance
from lindley.providers.base import ProviderError
from lindley.providers.registry import get_provider

router = APIRouter(prefix="/assembler", tags=["assembler"])


class AskRequest(BaseModel):
    page_ids: list[int] = Field(min_length=1)


@router.post("/ask")
def ask(body: AskRequest, request: Request, conn: Conn) -> dict:
    settings = request.app.state.settings
    name = settings.ai.connection_for("assemble")
    if not name:
        raise HTTPException(400, "No AI is set up to sort pages. Choose one in Settings.")
    try:
        chat = get_provider(settings.ai, "assemble")
    except ProviderError as e:
        raise HTTPException(400, str(e)) from e
    report = assemble(conn, settings.assembler, chat, asked=set(body.page_ids))
    if report.ai_calls:
        with conn:
            allowance.record(conn, name, "assemble", False, count=report.ai_calls)
    return {
        "ai_calls": report.ai_calls,
        "reused": report.ai_reused,
        "rejected": report.ai_rejected,
        "documents_created": report.documents_created,
        "pages_added": report.pages_added,
        "hints": report.hints,
    }
