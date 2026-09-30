"""Application settings: the settings.json schema, plus lookup, load and save."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from platformdirs import user_config_dir
from pydantic import BaseModel, Field

APP_NAME = "Lindley"
SETTINGS_ENV_VAR = "LINDLEY_SETTINGS"
SETTINGS_FILENAME = "settings.json"


class OcrSettings(BaseModel):
    engine: Literal["hybrid", "tesseract", "vision"] = "hybrid"
    tesseract_path: Path | None = None
    languages: list[str] = Field(default_factory=lambda: ["eng"])
    # Pages whose Tesseract confidence (0-100) falls below this are sent to the vision provider.
    confidence_threshold: int = Field(default=70, ge=0, le=100)
    vision_provider: str | None = "local"


class ProviderConfig(BaseModel):
    type: Literal["anthropic", "openai_compat", "fake"]
    base_url: str | None = None
    model: str | None = None
    # Name of the environment variable holding the API key; keys never live in settings.json.
    api_key_env: str | None = None

    def api_key(self) -> str | None:
        return os.environ.get(self.api_key_env) if self.api_key_env else None


def _default_providers() -> dict[str, ProviderConfig]:
    return {
        "local": ProviderConfig(
            type="openai_compat", base_url="http://localhost:11434/v1", model="llama3.1"
        )
    }


class AiSettings(BaseModel):
    chat_provider: str | None = "local"
    embedding_provider: str | None = "local"
    providers: dict[str, ProviderConfig] = Field(default_factory=_default_providers)


class AssemblerSettings(BaseModel):
    """How confident Lindley must be (0-100) before it groups Inbox pages into documents."""

    # At or above this, confident groups become Lindley documents; pages leave the Inbox.
    group_at: int = Field(default=75, ge=0, le=100)
    # At or above this, a page stays in the Inbox with an "Add to ...?" hint.
    hint_at: int = Field(default=45, ge=0, le=100)
    # Breaks between pages scored inside this band are checked with the chat AI, if one is set.
    ai_band: tuple[int, int] = (35, 75)
    use_ai: bool = True


class Settings(BaseModel):
    watch_folders: list[Path] = Field(default_factory=lambda: [Path("data/inbox")])
    processing_dir: Path = Path("data/processing")
    quarantine_dir: Path = Path("data/quarantine")
    library_dir: Path = Path("data/library")
    db_path: Path = Path("data/lindley.db")
    move_files: bool = False
    ocr: OcrSettings = Field(default_factory=OcrSettings)
    ai: AiSettings = Field(default_factory=AiSettings)
    assembler: AssemblerSettings = Field(default_factory=AssemblerSettings)


def default_settings_path() -> Path:
    return Path(user_config_dir(APP_NAME, appauthor=False)) / SETTINGS_FILENAME


def resolve_settings_path(explicit: str | Path | None = None) -> Path:
    """Find settings.json: explicit arg > $LINDLEY_SETTINGS > ./settings.json > user config."""
    if explicit:
        return Path(explicit)
    if env := os.environ.get(SETTINGS_ENV_VAR):
        return Path(env)
    local = Path.cwd() / SETTINGS_FILENAME
    if local.exists():
        return local
    return default_settings_path()


def load_settings(path: str | Path | None = None) -> Settings:
    """Load settings from the resolved path, falling back to defaults if the file is missing."""
    resolved = resolve_settings_path(path)
    if not resolved.exists():
        return Settings()
    return Settings.model_validate_json(resolved.read_text(encoding="utf-8"))


def save_settings(settings: Settings, path: str | Path | None = None) -> Path:
    resolved = resolve_settings_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(settings.model_dump_json(indent=2), encoding="utf-8")
    return resolved
