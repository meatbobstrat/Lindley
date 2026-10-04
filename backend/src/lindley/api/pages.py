"""Pages: one page with its text, its image (upright: EXIF and both rotations applied, and
reduced), and a person's changes: moving pages, turning them, and checking their text.

Each change returns `undo`: the batch to send to POST /api/undo/{batch} to take it back.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from PIL import Image
from pydantic import BaseModel, Field

from lindley import browse, organise
from lindley.api.deps import Conn
from lindley.api.library import change_json, errors, review_below
from lindley.worker.image import upright_page

router = APIRouter(prefix="/pages", tags=["pages"])


class Move(BaseModel):
    page_ids: list[int] = Field(min_length=1)
    to: Literal["document", "inbox", "aside"]
    document_id: int | None = None  # with to: document; the pages go at its end


class Rotate(BaseModel):
    page_ids: list[int] = Field(min_length=1)
    degrees: Literal[90, -90, 180]


class Text(BaseModel):
    text: str | None = None  # None: the text is right as it is


@router.post("/move")
def move(body: Move, conn: Conn) -> dict:
    try:
        c = organise.move_pages(conn, body.page_ids, body.to, body.document_id)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.post("/rotate")
def rotate(body: Rotate, conn: Conn) -> dict:
    try:
        c = organise.rotate(conn, body.page_ids, body.degrees)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.get("/{page_id}")
def get_page(page_id: int, request: Request, conn: Conn) -> dict:
    p = browse.page(conn, page_id, review_below(request))
    if p is None:
        raise HTTPException(404, f"There's no page {page_id}")
    return p


@router.put("/{page_id}/text")
def check_text(page_id: int, body: Text, conn: Conn) -> dict:
    """A person checked the text: as it is, or with their correction."""
    try:
        c = organise.check_text(conn, page_id, body.text)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.get("/{page_id}/image")
def page_image(
    page_id: int,
    conn: Conn,
    max_side: Annotated[int, Query(ge=64, le=4000)] = 1600,
) -> Response:
    row = conn.execute(
        "SELECT image_path, detected_rotation, user_rotation FROM pages WHERE id = ?", (page_id,)
    ).fetchone()
    if row is None or not row["image_path"] or not Path(row["image_path"]).is_file():
        raise HTTPException(404, "That page has no image")
    img = upright_page(Path(row["image_path"]), row["detected_rotation"] + row["user_rotation"])
    img.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=85)
    return Response(
        buf.getvalue(), media_type="image/jpeg", headers={"Cache-Control": "private, max-age=300"}
    )
