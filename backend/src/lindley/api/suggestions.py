"""The assembler's hints in the Inbox, and a person's answer to them.

A person is asked before any AI: "Do these go together?" (group_pages), "Add to …?"
(add_to_document), "Set aside?" (set_aside). Accepting makes the change and returns `undo`, the
batch to send to POST /api/undo/{batch} to take it back. Dismissing means it's never asked again.

A hint's payload may hold `candidates`: where its pages most likely belong, best first (at most
three; lindley.assembler.place). Each is an open document (`document`, its id) or other pages
in the Inbox (`pages`), with its name, whether the pages go at its start or end, a confidence
0-100 and the reasons, so a person can choose without looking through every document.
"""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException

from lindley.api.deps import Conn
from lindley.assembler import decide
from lindley.assembler.apply import HINT_KINDS

router = APIRouter(prefix="/suggestions", tags=["suggestions"])


@router.get("")
def list_suggestions(conn: Conn) -> dict:
    rows = conn.execute(
        "SELECT s.*, d.name AS document_name FROM suggestions s"
        " LEFT JOIN documents d ON d.id = s.document_id"
        f" WHERE s.status = 'open' AND s.kind IN {HINT_KINDS} ORDER BY s.confidence DESC, s.id"
    ).fetchall()
    return {
        "suggestions": [
            {
                "id": r["id"],
                "kind": r["kind"],
                "page_id": r["page_id"],
                "document_id": r["document_id"],
                "document_name": r["document_name"],
                "confidence": r["confidence"],
                "reasons": json.loads(r["reasons"] or "[]"),
                "payload": json.loads(r["payload"] or "{}"),
            }
            for r in rows
        ]
    }


@router.post("/{suggestion_id}/accept")
def accept(suggestion_id: int, conn: Conn) -> dict:
    try:
        a = decide.accept(conn, suggestion_id)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e
    except ValueError as e:
        raise HTTPException(409, str(e)) from e
    return {"document_id": a.document_id, "pages": list(a.pages), "undo": a.batch}


@router.post("/{suggestion_id}/dismiss")
def dismiss(suggestion_id: int, conn: Conn) -> dict:
    try:
        decide.dismiss(conn, suggestion_id)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True}
