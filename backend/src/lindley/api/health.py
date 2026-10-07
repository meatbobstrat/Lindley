from fastapi import APIRouter, HTTPException, Request

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
        # Quit Lindley, in the app: only where it can stop itself (see quit_lindley)
        "can_quit": getattr(request.app.state, "quit", None) is not None,
    }


@router.post("/quit")
def quit_lindley(request: Request) -> dict:
    """Quit Lindley, in the app: it stops once this answer is sent, and work cut off is picked
    up when it next starts. Only the app started by `python -m lindley` (or from the menu)
    can stop from here: one with --reload, or under tests, can't."""
    quit = getattr(request.app.state, "quit", None)
    if quit is None:
        raise HTTPException(
            409, "This Lindley can't be stopped from the app: stop it where it was started"
        )
    quit()
    return {"stopping": True}
