"""The places the app shows: the overview behind the folder tree, the Inbox, Set aside, and the
pages waiting for a person's review."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from lindley import __version__, activity, browse
from lindley.api.deps import Conn
from lindley.api.needs_ai import sort_items
from lindley.config import Settings
from lindley.organise import Change
from lindley.worker.pipeline import vision_queue

router = APIRouter(tags=["library"])


def review_below(request: Request) -> int:
    return request.app.state.settings.ocr.review_below


def errors(e: Exception) -> HTTPException:
    """lindley.organise's errors as HTTP: not there (404), or not possible now (409)."""
    return HTTPException(404 if isinstance(e, LookupError) else 409, str(e))


def change_json(c: Change) -> dict:
    return {
        "undo": c.batch,
        "document_id": c.document_id,
        "folder_id": c.folder_id,
        "removed": c.removed,
    }


@router.get("/overview")
def overview(request: Request, conn: Conn) -> dict:
    """Everything the folder tree and status bar show, in one call."""
    settings: Settings = request.app.state.settings
    work = request.app.state.ai_work
    sending = work.pages()  # sent already, on their way to the AI
    sorting = settings.ai.connection_for("assemble") is not None
    needs_ai = sum(r["page_id"] not in sending for r in vision_queue(conn))
    if sorting:  # with no AI to sort with, there's nothing to ask: a person sorts them
        needs_ai += sum(p["id"] not in sending for i in sort_items(conn) for p in i["pages"])
    return {
        "version": __version__,
        "counts": browse.counts(
            conn, settings.ocr.review_below, needs_ai, settings.ocr.engine != "vision"
        ),
        "documents": browse.documents(conn, settings.ocr.review_below),
        "folders": browse.folders(conn),
        # What the AI is doing now, what's queued behind it, and what came of what people asked
        "ai": {
            "working": activity.current(),
            "waiting": work.waiting(),
            "finished": activity.recent(),
        },
    }


@router.get("/inbox")
def inbox(request: Request, conn: Conn) -> dict:
    return {"pages": browse.inbox(conn, review_below(request))}


@router.get("/aside")
def aside(request: Request, conn: Conn) -> dict:
    return {"pages": browse.aside(conn, review_below(request))}


@router.get("/review")
def review(request: Request, conn: Conn) -> dict:
    groups = browse.review_groups(conn, review_below(request))
    return {
        "review_below": review_below(request),
        "count": sum(len(g["pages"]) for g in groups),
        "groups": groups,
    }
