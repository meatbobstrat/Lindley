"""Sorting the Inbox on a person's say-so.

POST /api/assembler/ask sends the pages a person chose to the AI that sorts pages, at once:
asking is the OK, whatever the connection's `allow`, and the call is recorded as one a person
asked for. An answer the AI gave before about the same pages is used again, with no call.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from lindley.api.deps import Conn
from lindley.config import Settings
from lindley.providers.base import ProviderError
from lindley.worker.ai_work import sort_as_asked

router = APIRouter(prefix="/assembler", tags=["assembler"])


class AskRequest(BaseModel):
    page_ids: list[int] = Field(min_length=1)


@router.post("/ask")
def ask(body: AskRequest, request: Request, conn: Conn) -> dict:
    return ask_about(conn, request.app.state.settings, set(body.page_ids))


def ask_about(conn: sqlite3.Connection, settings: Settings, page_ids: set[int]) -> dict:
    """Send these pages to the sorting AI at once, as a person asked, and wait for it. (The app
    sends them through Needs AI, in the background.)"""
    try:
        return sort_as_asked(conn, settings, page_ids)
    except ProviderError as e:
        raise HTTPException(400, str(e)) from e
