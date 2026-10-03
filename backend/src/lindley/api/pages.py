"""Page images for the UI: upright (EXIF and both rotations applied) and reduced."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response
from PIL import Image

from lindley.api.deps import Conn
from lindley.worker.image import upright_page

router = APIRouter(prefix="/pages", tags=["pages"])


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
