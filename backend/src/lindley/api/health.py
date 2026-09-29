from fastapi import APIRouter, Request

from lindley import __version__

router = APIRouter(tags=["health"])


@router.get("/health")
def health(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "status": "ok",
        "version": __version__,
        "ocr_engine": settings.ocr.engine,
        "chat_provider": settings.ai.chat_provider,
    }
