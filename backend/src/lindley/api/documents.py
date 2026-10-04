"""Documents: listing and viewing them, starting one, naming and filing it, putting its pages
in order, exporting it as a searchable PDF, and reopening it.

Each change returns `undo`: the batch to send to POST /api/undo/{batch} to take it back.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from lindley import browse, organise
from lindley.api.deps import Conn
from lindley.api.library import change_json, errors, review_below
from lindley.export import export_document, latest_export, reopen_document

router = APIRouter(prefix="/documents", tags=["documents"])


class NewDocument(BaseModel):
    page_ids: list[int] = Field(min_length=1)
    name: str = ""
    folder_id: int | None = None
    suggested: bool = False  # the name is Lindley's, kept as it was


class DocumentChanges(BaseModel):
    """Only the fields sent are changed. folder_id null takes it out of any folder."""

    name: str | None = None
    doc_type: str | None = None
    doc_date: str | None = None  # partial ISO 8601: 1892, 1892-03, 1892-03-04
    folder_id: int | None = None


class Order(BaseModel):
    page_ids: list[int] = Field(min_length=1)


@router.get("")
def list_documents(request: Request, conn: Conn) -> dict:
    return {"documents": browse.documents(conn, review_below(request))}


@router.post("")
def new_document(body: NewDocument, conn: Conn) -> dict:
    try:
        c = organise.new_document(conn, body.page_ids, body.name, body.folder_id, body.suggested)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.get("/{doc_id}")
def get_document(doc_id: int, request: Request, conn: Conn) -> dict:
    d = browse.document(conn, doc_id, review_below(request))
    if d is None:
        raise HTTPException(404, f"There's no document {doc_id}")
    return d


@router.patch("/{doc_id}")
def update_document(doc_id: int, body: DocumentChanges, conn: Conn) -> dict:
    try:
        c = organise.update_document(conn, doc_id, body.model_dump(exclude_unset=True))
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.put("/{doc_id}/order")
def reorder(doc_id: int, body: Order, conn: Conn) -> dict:
    try:
        c = organise.reorder(conn, doc_id, body.page_ids)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.post("/{doc_id}/export")
def export(doc_id: int, conn: Conn, request: Request) -> dict:
    try:
        settings = request.app.state.settings
        e = export_document(conn, settings.library_dir, doc_id, settings.ocr.review_below)
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
