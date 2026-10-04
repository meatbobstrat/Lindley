"""Needs AI: scans Lindley couldn't read or sort well enough on its own, waiting for an AI.

Two kinds, like the two kinds of Duplicates:
- read: pages Tesseract read with less than ocr.confidence_threshold, waiting for the vision
  model (or whose vision call failed);
- sort: pages the rules couldn't sort into documents, one item per question the sorting AI
  would be asked, with the rules' own guess at the documents in it. A proposal marked
  "question": "place" asks which of a few likely documents (its "candidates") the pages
  belong to, rather than how to sort them.

When a connection may run on its own (`allow` "auto"), Lindley sends these itself as they
arrive, within its limits, so the list is usually empty. Otherwise they wait here until a
person sends them: one item, or all of them. Sending is the OK, and each call is recorded as one
a person asked for.
"""

from __future__ import annotations

import json
import sqlite3

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from lindley.api.assembler import ask_about
from lindley.api.deps import Conn
from lindley.assembler.auto import sort_on_its_own
from lindley.config import Settings
from lindley.providers import allowance
from lindley.providers.registry import connectors
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


def _sort_items(conn: sqlite3.Connection) -> list[dict]:
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
        }
        for r in vision_queue(conn)
    ]
    sort = _sort_items(conn)
    return {
        "count": len(read) + len(sort),
        "read": {"connection": _connection(conn, settings, "vision"), "pages": read},
        "sort": {"connection": _connection(conn, settings, "assemble"), "items": sort},
    }


class ReadRequest(BaseModel):
    page_ids: list[int] | None = None  # None: every page waiting


@router.post("/read")
def read(body: ReadRequest, request: Request, conn: Conn) -> dict:
    """Send pages to the vision model now, failed ones included: a person asked."""
    settings: Settings = request.app.state.settings
    pipeline = Pipeline.from_settings(settings)
    if pipeline.vision is None:
        raise HTTPException(400, "No AI is set up to read hard pages. Choose one in Settings.")
    run = pipeline.read_waiting(conn, body.page_ids, retry_failed=True)
    report = sort_on_its_own(conn, settings) if run.read else None  # new text: sort again
    return {
        "read": run.read,
        "failed": run.failed,
        "waiting": run.waiting,
        "stopped": run.stopped,
        "documents_created": report.documents_created if report else 0,
    }


@router.post("/sort")
def sort_all(request: Request, conn: Conn) -> dict:
    """Send every question waiting for the sorting AI now."""
    pages = {p["id"] for item in _sort_items(conn) for p in item["pages"]}
    if not pages:
        raise HTTPException(404, "Nothing is waiting for the AI to sort")
    return ask_about(conn, request.app.state.settings, pages)


@router.post("/{item_id}/sort")
def sort_one(item_id: int, request: Request, conn: Conn) -> dict:
    """Send one question to the sorting AI now: these pages, which may be one document or more."""
    row = conn.execute("SELECT pages FROM needs_ai WHERE id = ?", (item_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "That isn't waiting for the AI any more")
    pages = json.loads(row[0])
    if not _in_inbox(conn, pages):
        raise HTTPException(
            409, "Some of these pages have been placed since, so this is out of date"
        )
    return ask_about(conn, request.app.state.settings, set(pages))
