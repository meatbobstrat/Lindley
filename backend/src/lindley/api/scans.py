"""Add scans… in the Inbox: files a person picks or drops onto the app, uploaded.

Each is imported as a scan the person added: hashed, copied into the library, and split into
pages, at once, so the Inbox shows them straight away. The upload's temporary copy is removed
once Lindley's own copy is checked; the person's original, on their computer, is never touched.
The folder watcher then reads them, and sorts the Inbox once things settle.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Request, UploadFile

from lindley.api.deps import Conn
from lindley.config import Settings
from lindley.worker.intake import import_file, needs_reading

router = APIRouter(prefix="/scans", tags=["scans"])


@router.post("")
def add_scans(files: list[UploadFile], request: Request, conn: Conn) -> dict:
    settings: Settings = request.app.state.settings
    drop = settings.processing_dir / "added" / uuid.uuid4().hex
    drop.mkdir(parents=True)
    # Moving takes the temporary copy away once it's safe in the library
    taking = settings.model_copy(update={"move_files": True})
    added, to_read = [], []
    try:
        for i, f in enumerate(files):
            name = Path(f.filename or "scan").name or "scan"
            path = drop / str(i) / name  # two files of the same name are both taken
            path.parent.mkdir()
            with path.open("wb") as out:
                shutil.copyfileobj(f.file, out)
            r = import_file(conn, taking, path, origin="added")
            if r.status == "new":
                with conn:  # the person's own file stays where it was: a copy
                    conn.execute(
                        "UPDATE scans SET import_mode = 'copy', source_path = ? WHERE id = ?",
                        (name, r.scan_id),
                    )
            if needs_reading(conn, r):
                to_read.append(r.scan_id)
            added.append({"file": name, "status": r.status, "pages": r.pages, "error": r.error})
    finally:
        shutil.rmtree(drop, ignore_errors=True)
    if to_read and (watcher := getattr(request.app.state, "watcher", None)) is not None:
        watcher.read_later(to_read)
    return {"added": added, "reading": len(to_read)}
