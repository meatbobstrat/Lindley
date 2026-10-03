from fastapi import APIRouter, Request

from lindley import __version__
from lindley.providers.base import JOBS

router = APIRouter(tags=["health"])


@router.get("/health")
def health(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "status": "ok",
        "version": __version__,
        "ocr_engine": settings.ocr.engine,
        # The AI connection doing each job, or None.
        "ai": {job: settings.ai.connection_for(job) for job in JOBS},
    }
