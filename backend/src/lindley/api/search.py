"""Full-text search across the text of every page (lindley.search.fts)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from lindley.api.deps import Conn
from lindley.browse import page_number
from lindley.search.fts import HIT_END, HIT_START, search_pages

router = APIRouter(prefix="/search", tags=["search"])


@router.get("")
def search(conn: Conn, q: str, limit: Annotated[int, Query(ge=1, le=200)] = 50) -> dict:
    """Each result's `snippet` is a list of [text, found] runs: found is true for the words
    that matched."""
    results = []
    for r in search_pages(conn, q, limit):
        where = "document" if r["document_id"] else "aside" if r["set_aside_at"] else "inbox"
        results.append(
            {
                "page_id": r["page_id"],
                "file": r["file"],
                "where": where,
                "document_id": r["document_id"],
                "document_name": r["document_name"],
                "page_number": page_number(conn, r["document_id"], r["position"]),
                "image": f"/api/pages/{r['page_id']}/image",
                "snippet": _runs(r["snippet"]),
            }
        )
    return {"query": q, "results": results}


def _runs(snippet: str) -> list[list]:
    out: list[list] = []
    for i, part in enumerate(snippet.replace(HIT_END, HIT_START).split(HIT_START)):
        if part:
            out.append([" ".join(part.split()), i % 2 == 1])
    return out
