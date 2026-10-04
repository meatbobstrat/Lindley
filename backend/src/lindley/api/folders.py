"""Folders a person makes to file documents in. Each change returns `undo`."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from lindley import browse, organise
from lindley.api.deps import Conn
from lindley.api.library import change_json, errors

router = APIRouter(prefix="/folders", tags=["folders"])


class NewFolder(BaseModel):
    name: str = "New folder"
    parent_id: int | None = None


class Rename(BaseModel):
    name: str


@router.get("")
def list_folders(conn: Conn) -> dict:
    return {"folders": browse.folders(conn)}


@router.post("")
def new_folder(body: NewFolder, conn: Conn) -> dict:
    try:
        c = organise.new_folder(conn, body.name, body.parent_id)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)


@router.patch("/{folder_id}")
def rename(folder_id: int, body: Rename, conn: Conn) -> dict:
    try:
        c = organise.rename_folder(conn, folder_id, body.name)
    except (LookupError, ValueError) as e:
        raise errors(e) from e
    return change_json(c)
