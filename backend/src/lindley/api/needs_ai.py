"""Needs AI: scans Lindley couldn't read or sort well enough on its own, waiting for an AI.

Two kinds, like the two kinds of Duplicates:
- read: pages Tesseract read with less than ocr.confidence_threshold, waiting for the vision
  model (or whose vision call failed). A person may send a page under review too (read with
  less than ocr.review_below), though it doesn't wait here;
- sort: pages the rules couldn't sort into documents, one item per question the sorting AI
  would be asked, with the rules' own guess at the documents in it. A proposal marked
  "question": "place" asks which of a few likely documents (its "candidates") the pages
  belong to, rather than how to sort them.

When a connection may run on its own (`allow` "auto"), Lindley sends these itself as they
arrive, within its limits, so the list is usually empty. Otherwise they wait here until a
person sends them: one item, or all of them. Sending is the OK, and each call is recorded as one
a person asked for. Sending queues the work (lindley.worker.ai_work) and answers at once; pages
on their way to the AI are marked `sending` until it's done.
"""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from lindley import browse
from lindley.api.deps import Conn
from lindley.config import Settings
from lindley.providers import allowance
from lindley.providers.base import ProviderError
from lindley.providers.registry import connectors, get_provider
from lindley.worker.ai_work import AiWork, connection_label
from lindley.worker.pipeline import Pipeline, vision_queue

router = APIRouter(prefix="/needs-ai", tags=["needs-ai"])


def _connection(conn: sqlite3.Connection, settings: Settings, job: str) -> dict | None:
    """The connection that does a job, as a person needs to know it before sending."""
    name = settings.ai.connection_for(job)
    cfg = allowance.provider_config(settings, name)
    if cfg is None:
        return None
    info = getattr(connectors().get(cfg.type), "info", None)
    return {
        "name": name,
        "label": cfg.label or (info.label if info else cfg.type),
        "where": info.where if info else "cloud",
        "allow": cfg.allow,
        "automatic_left": allowance.automatic_left(conn, settings, name),
    }


def _in_inbox(conn: sqlite3.Connection, pages: list[int]) -> bool:
    marks = ",".join("?" * len(pages))
    return conn.execute(
        f"SELECT COUNT(*) FROM pages WHERE id IN ({marks})"
        " AND document_id IS NULL AND set_aside_at IS NULL",
        pages,
    ).fetchone()[0] == len(pages)


def _work(request: Request) -> AiWork:
    return request.app.state.ai_work


def sort_items(conn: sqlite3.Connection) -> list[dict]:
    out = []
    for r in conn.execute("SELECT * FROM needs_ai ORDER BY since, id").fetchall():
        pages = json.loads(r["pages"])
        if not _in_inbox(conn, pages):
            continue  # a person has placed some of them since; the next run drops it
        marks = ",".join("?" * len(pages))
        files = dict(
            conn.execute(
                f"SELECT p.id, s.original_name FROM pages p JOIN scans s ON s.id = p.scan_id"
                f" WHERE p.id IN ({marks})",
                pages,
            ).fetchall()
        )
        out.append(
            {
                "id": r["id"],
                "since": r["since"],
                "pages": [{"id": p, "file": files.get(p)} for p in pages],
                "proposal": json.loads(r["proposal"]),
            }
        )
    return out


@router.get("")
def list_needs_ai(request: Request, conn: Conn) -> dict:
    settings: Settings = request.app.state.settings
    sending = _work(request).pages()
    read = [
        {
            "page_id": r["page_id"],
            "file": r["file_name"],
            "confidence": r["confidence"],
            "failed": r["status"] == "failed",
            "why": r["error"],
            "document_id": r["document_id"],
            "document_name": r["document_name"],
            "position": r["position"],
            "sending": r["status"] == "running" or r["page_id"] in sending,
        }
        for r in vision_queue(conn, running=True)
    ]
    sort = sort_items(conn)
    for item in sort:
        item["sending"] = all(p["id"] in sending for p in item["pages"])
    return {
        "count": len(read) + len(sort),
        "read": {"connection": _connection(conn, settings, "vision"), "pages": read},
        "sort": {"connection": _connection(conn, settings, "assemble"), "items": sort},
    }


class ReadRequest(BaseModel):
    page_ids: list[int] | None = None  # None: every page waiting


def _under_review(conn: sqlite3.Connection, ids: set[int], settings: Settings) -> list[int]:
    """Of these pages, those waiting for a person's review (browse.state)."""
    if not ids:
        return []
    marks = ",".join("?" * len(ids))
    rows = browse.pages(conn, f"p.id IN ({marks})", tuple(ids), settings.ocr.review_below, "p.id")
    return [p["id"] for p in rows if p["state"] == "review"]


def _sent(new: list[int], already: list[int], label: str | None) -> dict:
    return {"queued": len(new), "already": len(already), "connection": label}


@router.post("/read")
def read(body: ReadRequest, request: Request, conn: Conn) -> dict:
    """Send pages to the vision model, failed ones included: a person asked. With page_ids,
    pages under review may be sent too. The pages are queued and read in the background."""
    settings: Settings = request.app.state.settings
    if Pipeline.from_settings(settings).vision is None:
        raise HTTPException(400, "No AI is set up to read hard pages. Choose one in Settings.")
    waiting = [r["page_id"] for r in vision_queue(conn, running=True)]
    if body.page_ids is not None:
        asked = set(body.page_ids)
        waiting = [p for p in waiting if p in asked]
        waiting += _under_review(conn, asked - set(waiting), settings)
    if not waiting:
        raise HTTPException(404, "Nothing here is waiting for the AI to read it")
    new, already = _work(request).read(waiting)
    return _sent(new, already, connection_label(settings, "vision"))


def _sort(request: Request, pages: list[int]) -> dict:
    settings: Settings = request.app.state.settings
    if not settings.ai.connection_for("assemble"):
        raise HTTPException(400, "No AI is set up to sort pages. Choose one in Settings.")
    try:
        get_provider(settings.ai, "assemble")  # one that can't be made fails now, not later
    except ProviderError as e:
        raise HTTPException(400, str(e)) from e
    new, already = _work(request).sort(pages)
    return _sent(new, already, connection_label(settings, "assemble"))


@router.post("/sort")
def sort_all(request: Request, conn: Conn) -> dict:
    """Send every question waiting for the sorting AI, in the background."""
    pages = [p["id"] for item in sort_items(conn) for p in item["pages"]]
    if not pages:
        raise HTTPException(404, "Nothing is waiting for the AI to sort")
    return _sort(request, pages)


@router.post("/{item_id}/sort")
def sort_one(item_id: int, request: Request, conn: Conn) -> dict:
    """Send one question to the sorting AI: these pages, which may be one document or more."""
    row = conn.execute("SELECT pages FROM needs_ai WHERE id = ?", (item_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "That isn't waiting for the AI any more")
    pages = json.loads(row[0])
    if not _in_inbox(conn, pages):
        raise HTTPException(
            409, "Some of these pages have been placed since, so this is out of date"
        )
    return _sort(request, pages)
