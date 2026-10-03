"""Documents: exporting one as a searchable PDF, and reopening it.

Listing and viewing documents are designed in the UI phase; that endpoint is a stub for now.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from lindley.api.deps import Conn
from lindley.export import export_document, latest_export, reopen_document

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("")
def not_implemented() -> None:
    raise HTTPException(status_code=501, detail="Not implemented yet")


@router.post("/{doc_id}/export")
def export(doc_id: int, conn: Conn, request: Request) -> dict:
    try:
        e = export_document(conn, request.app.state.settings.library_dir, doc_id)
    except LookupError as err:
        raise HTTPException(404, str(err)) from err
    except ValueError as err:
        raise HTTPException(409, str(err)) from err
    return {
        "document_id": e.document_id,
        "pdf_path": str(e.pdf_path),
        "file_name": e.pdf_path.name,
        "exported_at": e.exported_at,
        "pages": e.pages,
        "to_review": e.to_review,
        "unplaced": e.unplaced,
    }


@router.get("/{doc_id}/export")
def download(doc_id: int, conn: Conn) -> FileResponse:
    row = latest_export(conn, doc_id)
    if row is None or not Path(row["pdf_path"]).is_file():
        raise HTTPException(404, "That document hasn't been exported")
    path = Path(row["pdf_path"])
    return FileResponse(path, media_type="application/pdf", filename=path.name)


@router.post("/{doc_id}/reopen")
def reopen(doc_id: int, conn: Conn) -> dict:
    try:
        reopen_document(conn, doc_id)
    except LookupError as err:
        raise HTTPException(404, str(err)) from err
    return {"document_id": doc_id, "status": "progress"}
