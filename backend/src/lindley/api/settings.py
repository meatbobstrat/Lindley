from fastapi import APIRouter, Request

from lindley.config import Settings, save_settings

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
def get_settings(request: Request) -> Settings:
    return request.app.state.settings


@router.put("")
def put_settings(new: Settings, request: Request) -> Settings:
    save_settings(new, request.app.state.settings_path)
    request.app.state.settings = new
    return new
