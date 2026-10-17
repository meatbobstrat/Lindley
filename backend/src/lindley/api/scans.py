"""Add scans… in the Inbox: files a person picks or drops onto the app, uploaded.

Each is imported as a scan the person added: hashed, copied into the library, and split into
pages, at once, so the Inbox shows them straight away. The folder watcher then reads them, and
sorts the Inbox once things settle.

Copied or moved, as the person chose (`mode`, else settings' add_mode). Copied: the person's
file stays where it is on their computer, and the upload's temporary copy is removed once
Lindley's own is checked. Moved: Lindley keeps the file, unchanged, and gives it back beside the
PDF when the document is exported, as for a scan moved in from a watched folder. A browser can't
move or delete a file on the person's computer, so theirs stays there too until the desktop app.
"""

from __future__ import annotations

import logging
import shutil
import uuid
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Form, Request, UploadFile

from lindley.api.deps import Conn
from lindley.config import Settings
from lindley.worker.intake import import_file, needs_reading

router = APIRouter(prefix="/scans", tags=["scans"])
log = logging.getLogger(__name__)


@router.post("")
def add_scans(
    files: list[UploadFile],
    request: Request,
    conn: Conn,
    mode: Annotated[Literal["copy", "move"] | None, Form()] = None,
) -> dict:
    settings: Settings = request.app.state.settings
    drop = settings.processing_dir / "added" / uuid.uuid4().hex
    drop.mkdir(parents=True)
    move = (mode or settings.add_mode) == "move"
    # Copied: the upload's temporary copy goes with its folder, below. Moved: it's taken into
    # Lindley's keeping, as the original.
    taking = settings.model_copy(update={"move_files": move})
    added, to_read = [], []
    try:
        for i, f in enumerate(files):
            name = Path(f.filename or "scan").name or "scan"
            try:  # one file that can't be taken doesn't stop the others
                path = drop / str(i) / name  # two files of the same name are both taken
                path.parent.mkdir()
                with path.open("wb") as out:
                    shutil.copyfileobj(f.file, out)
                r = import_file(conn, taking, path, origin="added")
                if r.status == "new":
                    # Unless Lindley took it into its keeping, the original is the person's own
                    # file, on their computer, where Lindley can't see it
                    with conn:
                        conn.execute(
                            "UPDATE scans SET source_path = ?, original_path = CASE"
                            " WHEN original_state = 'held' THEN original_path END WHERE id = ?",
                            (name, r.scan_id),
                        )
                if needs_reading(conn, r):
                    to_read.append(r.scan_id)
                added.append({"file": name, "status": r.status, "pages": r.pages, "error": r.error})
            except Exception as e:  # noqa: BLE001 - reported for that file
                log.exception("Couldn't add %s", name)
                if conn.in_transaction:
                    conn.rollback()
                added.append({"file": name, "status": "failed", "pages": 0, "error": str(e)})
    finally:
        shutil.rmtree(drop, ignore_errors=True)
        # Those taken are read, even if a later one failed
        if to_read and (watcher := getattr(request.app.state, "watcher", None)) is not None:
            watcher.read_later(to_read)
    return {"added": added, "reading": len(to_read), "mode": "move" if move else "copy"}
