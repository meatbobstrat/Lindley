"""Documents: listing and viewing them, starting one, naming and filing it, putting its pages
in order, completing it, exporting it (a searchable PDF, and in move mode its originals,
to the output folder), and reopening it.

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
from lindley.export import (
    Exported,
    complete_document,
    export_document,
    latest_export,
    reopen_document,
)
from lindley.export.keeping import OriginalsNeeded

router = APIRouter(prefix="/documents", tags=["documents"])

NO_OUTPUT = "Choose where exported documents go first, in Settings › Library: the Exported folder."


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


@router.post("/{doc_id}/complete")
def complete(doc_id: int, conn: Conn, request: Request) -> dict:
    """A person says the document is right: it's locked, ready to export."""
    try:
        to_review = complete_document(conn, doc_id, request.app.state.settings.ocr.review_below)
    except LookupError as err:
        raise HTTPException(404, str(err)) from err
    except ValueError as err:
        raise HTTPException(409, str(err)) from err
    return {"document_id": doc_id, "status": "complete", "to_review": to_review}


class Many(BaseModel):
    document_ids: list[int] = Field(min_length=1)


@router.post("/export")
def export_many(body: Many, conn: Conn, request: Request) -> dict:
    """Export several completed documents at once: each that can be is, and each that can't
    says why."""
    exported, failed = [], []
    for doc_id in dict.fromkeys(body.document_ids):
        try:
            exported.append(_exported_json(_export(conn, request, doc_id)))
        except (LookupError, ValueError) as err:
            failed.append({"document_id": doc_id, "error": str(err)})
    return {"exported": exported, "failed": failed}


@router.post("/{doc_id}/export")
def export(doc_id: int, conn: Conn, request: Request) -> dict:
    try:
        e = _export(conn, request, doc_id)
    except LookupError as err:
        raise HTTPException(404, str(err)) from err
    except ValueError as err:
        raise HTTPException(409, str(err)) from err
    return _exported_json(e)


def _export(conn, request: Request, doc_id: int) -> Exported:
    settings = request.app.state.settings
    if settings.output_dir is None:
        raise ValueError(NO_OUTPUT)
    return export_document(
        conn, settings.output_dir, doc_id, settings.ocr.review_below, settings.library_dir
    )


def _exported_json(e: Exported) -> dict:
    return {
        "document_id": e.document_id,
        "pdf_path": str(e.pdf_path),
        "file_name": e.pdf_path.name,
        "folder": str(e.folder) if e.folder else None,
        "exported_at": e.exported_at,
        "pages": e.pages,
        "to_review": e.to_review,
        "unplaced": e.unplaced,
        "originals": [p.name for p in e.originals],
        "kept": e.kept,
        "problems": e.problems,
        "copies_kept": e.copies_kept,
    }


@router.get("/{doc_id}/export")
def download(doc_id: int, conn: Conn) -> FileResponse:
    row = latest_export(conn, doc_id)
    if row is None or not Path(row["pdf_path"]).is_file():
        raise HTTPException(404, "That document hasn't been exported")
    path = Path(row["pdf_path"])
    return FileResponse(path, media_type="application/pdf", filename=path.name)


@router.post("/{doc_id}/reopen")
def reopen(doc_id: int, conn: Conn, request: Request) -> dict:
    """Back to In progress. An exported document's scans that Lindley let go of are brought
    back from their originals first; 409, with the originals `needed`, when one can't be found."""
    try:
        reopen_document(conn, doc_id, request.app.state.settings)
    except LookupError as err:
        raise HTTPException(404, str(err)) from err
    except OriginalsNeeded as err:
        raise HTTPException(409, _needed_json(err)) from err
    except (ValueError, OSError) as err:
        raise HTTPException(409, str(err)) from err
    return {"document_id": doc_id, "status": "progress"}


def _needed_json(err: OriginalsNeeded) -> dict:
    return {"message": str(err), "needed": [{"scan_id": i, "file": n} for i, n in err.needed]}
