"""Pages: one page with its text, its image (upright: EXIF and both rotations applied, and
reduced), and a person's changes: moving pages, turning them, and checking their text.

Each change returns `undo`: the batch to send to POST /api/undo/{batch} to take it back.
"""

from __future__ import annotations

import io
import textwrap
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from PIL import Image, ImageDraw, ImageFont
from pydantic import BaseModel, Field

from lindley import browse, organise
from lindley.api.deps import Conn
from lindley.api.library import change_json, errors, review_below
from lindley.worker.image import upright_page
from lindley.worker.pipeline import mirrored

router = APIRouter(prefix="/pages", tags=["pages"])


class Move(BaseModel):
    page_ids: list[int] = Field(min_length=1)
    to: Literal["document", "inbox", "aside"]
    document_id: int | None = None  # with to: document; the pages go at its end


class Rotate(BaseModel):
    page_ids: list[int] = Field(min_length=1)
    degrees: Literal[90, -90, 180]


class Pages(BaseModel):
    page_ids: list[int] = Field(min_length=1)


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


@router.post("/flip")
def flip(body: Pages, conn: Conn) -> dict:
    """Turn pages round left to right: mirror images the right way round."""
    try:
        c = organise.flip(conn, body.page_ids)
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
    request: Request,
    conn: Conn,
    max_side: Annotated[int, Query(ge=64, le=4000)] = 1600,
    v: str | None = None,
) -> Response:
    """`v` (browse.page's image address has it) changes whenever the page is turned, so that
    image may be kept a while. One asked for without it is checked each time (its ETag): a
    page turned since is never shown the old way round."""
    row = conn.execute(
        "SELECT p.image_path, p.detected_rotation, p.user_rotation, p.detected_mirror,"
        " p.user_mirror, p.width_px, p.height_px, d.status AS document_status"
        " FROM pages p LEFT JOIN documents d ON d.id = p.document_id WHERE p.id = ?",
        (page_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(404, "There's no such page")
    if not row["image_path"] or not Path(row["image_path"]).is_file():
        if row["document_status"] != "exported":
            raise HTTPException(404, "That page has no image")
        return _let_go(row, max_side)  # its scan went with the PDF: Lindley let go of its copy
    rotation = row["detected_rotation"] + row["user_rotation"]
    headers = {
        "Cache-Control": "private, max-age=300" if v else "private, no-cache",
        "ETag": f'"{page_id}-{rotation % 360}-{int(mirrored(row))}-{max_side}"',
    }
    if request.headers.get("if-none-match") == headers["ETag"]:
        return Response(status_code=304, headers=headers)
    img = upright_page(Path(row["image_path"]), rotation, mirrored(row))
    img.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=85)
    return Response(buf.getvalue(), media_type="image/jpeg", headers=headers)


LET_GO = (
    "Exported. Lindley let go of its copy of this scan: the PDF and your original have it."
    " Reopen the document to see it here again."
)
SMALL = 400  # a sheet narrower than this (a thumbnail) just says Exported


def _let_go(row, max_side: int) -> Response:
    """In place of an exported page's image: a sheet the page's shape, saying where it went."""
    w, h = row["width_px"] or 850, row["height_px"] or 1100
    if (row["detected_rotation"] + row["user_rotation"]) % 180:
        w, h = h, w
    scale = min(max_side, 800) / max(w, h)
    w, h = max(1, round(w * scale)), max(1, round(h * scale))
    img = Image.new("RGB", (w, h), "#efece6")
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, w - 1, h - 1), outline="#c9c3b8", width=max(1, w // 200))
    size = max(9, w // 22) if w >= SMALL else max(9, w // 8)
    font = ImageFont.load_default(size=size)
    words = LET_GO if w >= SMALL else "Exported"
    lines = textwrap.wrap(words, width=max(12, int(w * 0.8 / (size * 0.55))))
    y = (h - len(lines) * size * 1.3) / 2
    for line in lines:
        draw.text((w / 2, y), line, fill="#6b655b", font=font, anchor="ma")
        y += size * 1.3
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return Response(buf.getvalue(), media_type="image/png", headers={"Cache-Control": "no-cache"})
