"""The Duplicates queue: pages and documents scanned more than once, and a person's decision.

Copies a person doesn't keep are set aside, marked as duplicates; nothing is deleted. Each
decision returns `undo`: the batch to send to POST /api/undo/{batch} to take it back.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from difflib import SequenceMatcher

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from lindley.api.deps import Conn
from lindley.duplicates import resolve
from lindley.duplicates.resolve import Copy, DuplicateSet

router = APIRouter(prefix="/duplicates", tags=["duplicates"])

EXCERPT = 280  # characters of each copy's text in the list


class KeepRequest(BaseModel):
    page_id: int


class KeepDocumentRequest(BaseModel):
    keep: int
    other: int


def _copy_json(c: Copy, full: bool) -> dict:
    out = asdict(c)
    text = out.pop("text")
    out["image"] = f"/api/pages/{c.page_id}/image"
    if full:
        out["text"] = text
    else:
        out["excerpt"] = text[:EXCERPT] + ("…" if len(text) > EXCERPT else "")
    return out


def _set_json(s: DuplicateSet, full: bool = False) -> dict:
    out = {
        "id": s.id,
        "kind": s.kind,
        "score": s.score,
        "reasons": s.reasons,
        "suggested": s.suggested,
        "why": s.why,
        "copies": [_copy_json(c, full) for c in s.copies],
    }
    if full:
        base = next(c for c in s.copies if c.page_id == s.suggested)
        for cj, c in zip(out["copies"], s.copies, strict=True):
            other = base if c is not base else next(x for x in s.copies if x is not base)
            cj["segments"] = differences(c.text, other.text)
    return out


def differences(text: str, other: str) -> list[list]:
    """`text` as runs of [words, differs]: differs is true where `other` doesn't have them.

    Words are compared ignoring case and punctuation; spacing and line breaks are kept.
    """
    tokens = re.findall(r"\S+|\s+", text)
    words = [i for i, t in enumerate(tokens) if not t.isspace()]
    other_words = [_key(t) for t in re.findall(r"\S+", other)]
    differs = [True] * len(tokens)
    sm = SequenceMatcher(None, [_key(tokens[i]) for i in words], other_words, autojunk=False)
    for block in sm.get_matching_blocks():
        for k in range(block.a, block.a + block.size):
            differs[words[k]] = False
    runs: list[list] = []
    for i, t in enumerate(tokens):
        flag = False if t.isspace() else differs[i]
        if t.isspace() and runs:
            runs[-1][0] += t  # spacing joins the run before it
        elif runs and runs[-1][1] == flag:
            runs[-1][0] += t
        else:
            runs.append([t, flag])
    return runs


def _key(word: str) -> str:
    return re.sub(r"[^a-z0-9]", "", word.lower())


@router.get("")
def list_duplicates(conn: Conn) -> dict:
    sets = resolve.open_sets(conn)
    docs = resolve.document_pairs(sets, conn)
    return {
        "count": len(sets),
        "sets": [_set_json(s) for s in sets],
        "documents": [
            {
                "documents": list(d.documents),
                "names": list(d.names),
                "sets": d.set_ids,
                "extra": {str(k): v for k, v in d.extra.items()},
            }
            for d in docs
        ],
    }


@router.get("/{set_id}")
def get_duplicate_set(set_id: int, conn: Conn) -> dict:
    s = resolve.get_set(conn, set_id)
    if s is None:
        raise HTTPException(404, "There's no open duplicate set with that id")
    return _set_json(s, full=True)


@router.post("/keep-document")
def keep_document(body: KeepDocumentRequest, conn: Conn) -> dict:
    d = resolve.keep_document(conn, body.keep, body.other)
    if not d.sets:
        raise HTTPException(404, "Those two documents don't share any open duplicates")
    return {"sets": d.sets, "set_aside": d.set_aside, "undo": d.batch}


@router.post("/{set_id}/keep")
def keep(set_id: int, body: KeepRequest, conn: Conn) -> dict:
    try:
        d = resolve.keep(conn, set_id, body.page_id)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"kept": body.page_id, "set_aside": d.set_aside, "undo": d.batch}


@router.post("/{set_id}/not-duplicates")
def not_duplicates(set_id: int, conn: Conn) -> dict:
    try:
        d = resolve.not_duplicates(conn, set_id)
    except LookupError as e:
        raise HTTPException(404, str(e)) from e
    return {"ok": True, "undo": d.batch}
