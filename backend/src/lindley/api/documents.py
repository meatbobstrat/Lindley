"""List and view processed documents.

Stub: endpoints are designed in the UI phase and implemented in the backend phase.
"""

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("")
def not_implemented() -> None:
    raise HTTPException(status_code=501, detail="Not implemented yet")
