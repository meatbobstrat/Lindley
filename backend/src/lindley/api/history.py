"""Undo: take back a person's decision, all of it, if nothing has changed since."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from lindley import history
from lindley.api.deps import Conn

router = APIRouter(prefix="/undo", tags=["undo"])


@router.get("")
def latest(conn: Conn) -> dict:
    """The most recent decision that can still be undone (null if none)."""
    batch = history.latest(conn)
    actions = [
        r[0]
        for r in conn.execute("SELECT action FROM history WHERE batch = ? ORDER BY id", (batch,))
    ]
    return {"batch": batch, "actions": actions}


@router.post("")
def undo_latest(conn: Conn) -> dict:
    return _undo(conn, None)


@router.post("/{batch}")
def undo_batch(batch: int, conn: Conn) -> dict:
    return _undo(conn, batch)


def _undo(conn, batch: int | None) -> dict:
    try:
        done = history.undo(conn, batch)
    except history.UndoError as e:
        missing = "nothing" in str(e) or "no change" in str(e)
        raise HTTPException(404 if missing else 409, str(e)) from e
    return {"undone": done.batch, "actions": done.actions, "pages": done.pages}
