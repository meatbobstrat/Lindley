"""Application settings: the settings.json schema, plus lookup, load and save."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from platformdirs import user_config_dir
from pydantic import BaseModel, Field, model_validator

from lindley.providers.base import JOBS, Job

APP_NAME = "Lindley"
SETTINGS_ENV_VAR = "LINDLEY_SETTINGS"
SETTINGS_FILENAME = "settings.json"


class OcrSettings(BaseModel):
    engine: Literal["hybrid", "tesseract", "vision"] = "hybrid"
    tesseract_path: Path | None = None
    languages: list[str] = Field(default_factory=lambda: ["eng"])
    # Pages whose Tesseract confidence (0-100) falls below this need the vision provider.
    confidence_threshold: int = Field(default=70, ge=0, le=100)
    # Which AI reads them is ai.jobs.vision; whether they're sent at once or wait for a person's
    # OK is that connection's `allow`.
    # Pages are reduced to this many pixels on their longer side before they're sent.
    vision_max_side: int = Field(default=2000, ge=512)


class ProviderConfig(BaseModel):
    # The connector's id: a file in providers/connectors/, e.g. "local", "anthropic", "google".
    type: str
    # The name people see. The key in ai.providers is the connection's stable id.
    label: str | None = None
    base_url: str | None = None
    # The model for every job but embeddings, unless ai.jobs names one; else the connector's
    # default for the job.
    model: str | None = None
    # API keys never live in settings.json: they're in the system's credential store (see
    # providers/keys.py), or, when this names one, in an environment variable.
    api_key_env: str | None = None
    # When Lindley may use this AI on its own, for work in the background: reading hard pages,
    # sorting pages into documents. "ask": the work waits until a person OKs it, since a call
    # can cost money and sends pages away. "auto": it's sent as soon as there is some.
    # A question a person types in Ask Lindley is always sent: asking is the OK.
    allow: Literal["ask", "auto"] = "ask"
    # With "auto", at most this many calls a day on its own; then work waits for an OK again.
    # None: no limit.
    daily_limit: int | None = Field(default=None, ge=1)
    # The same for a calendar month. None: no limit.
    monthly_limit: int | None = Field(default=None, ge=1)
    # Throttling, for every call, OKed or not: at most this many calls a minute (None: any
    # number), and at most this many waiting for an answer at once.
    per_minute: int | None = Field(default=None, ge=1)
    at_once: int = Field(default=2, ge=1, le=32)
    timeout_s: int = Field(default=120, ge=5, le=3600)

    def api_key(self, name: str | None = None) -> str | None:
        """The key: from api_key_env if it's set, else saved for connection `name`."""
        if self.api_key_env and (key := os.environ.get(self.api_key_env)):
            return key
        if name:
            from lindley.providers.keys import get_key

            return get_key(name)
        return None


class JobConfig(BaseModel):
    """Which connection does a job, and with which model (None: the connection's)."""

    connection: str | None = None
    model: str | None = None


def _no_jobs() -> dict[Job, JobConfig]:
    return {job: JobConfig() for job in JOBS}


class AiSettings(BaseModel):
    """AI connections, and which one does each job. Out of the box there are none: Lindley
    works with its rules and Tesseract, and a person matches pages to documents by hand."""

    providers: dict[str, ProviderConfig] = Field(default_factory=dict)
    # vision: reading hard pages; assemble: sorting pages into documents; chat: Ask Lindley;
    # embed: finding related pages.
    jobs: dict[Job, JobConfig] = Field(default_factory=_no_jobs)

    def connection_for(self, job: Job) -> str | None:
        j = self.jobs.get(job)
        return j.connection if j else None


class AssemblerSettings(BaseModel):
    """How confident Lindley must be (0-100) before it groups Inbox pages into documents."""

    # At or above this, confident groups become Lindley documents; pages leave the Inbox.
    group_at: int = Field(default=75, ge=0, le=100)
    # At or above this, a page stays in the Inbox with an "Add to ...?" hint.
    hint_at: int = Field(default=45, ge=0, le=100)
    # Breaks between pages scored inside this band are checked with the chat AI, when the chat
    # provider may be used (its `allow`, or a person's OK).
    ai_band: tuple[int, int] = (35, 75)


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

    @model_validator(mode="before")
    @classmethod
    def _earlier_settings(cls, data: object) -> object:
        """Carry over settings files from before `allow`: ocr.vision_mode = "auto" and
        assembler.use_ai = true meant the vision and chat providers could run on their own."""
        if not isinstance(data, dict):
            return data
        ocr, assembler, ai = (data.get(k) for k in ("ocr", "assembler", "ai"))
        ai = ai if isinstance(ai, dict) else {}
        on = []
        if isinstance(ocr, dict) and ocr.pop("vision_mode", None) == "auto":
            on.append(ocr.get("vision_provider", "local"))
        if isinstance(assembler, dict) and assembler.pop("use_ai", False):
            on.append(ai.get("chat_provider", "local"))
        providers = ai.get("providers")
        for name in on:
            if isinstance(providers, dict) and isinstance(providers.get(name), dict):
                providers[name].setdefault("allow", "auto")
        _jobs_from_earlier(data)
        # Google connections once went through Google's OpenAI-compatible address; its own
        # library uses its own.
        for p in providers.values() if isinstance(providers, dict) else ():
            old_url = isinstance(p, dict) and (p.get("base_url") or "").rstrip("/")
            if old_url and p.get("type") == "google" and old_url.endswith("/openai"):
                p["base_url"] = None
        return data


def _jobs_from_earlier(data: dict) -> None:
    """Carry over settings files from before ai.jobs: ocr.vision_provider, ai.chat_provider
    (sorting pages and Ask Lindley) and ai.embedding_provider, each "local" when left out."""
    ocr, ai = data.get("ocr"), data.get("ai")
    vision = ocr.pop("vision_provider", "local") if isinstance(ocr, dict) else "local"
    if not isinstance(ai, dict):
        return
    chat, embed = ai.pop("chat_provider", "local"), ai.pop("embedding_provider", "local")
    providers = ai.get("providers")
    if "jobs" in ai or not isinstance(providers, dict):
        return
    for p in providers.values():
        if isinstance(p, dict) and p.get("type") == "openai_compat" and _private(p.get("base_url")):
            p["type"] = "local"
    old = {"vision": vision, "assemble": chat, "chat": chat, "embed": embed}
    ai["jobs"] = {job: {"connection": c if c in providers else None} for job, c in old.items()}


def _private(url: str | None) -> bool:
    """An address on this computer or the local network."""
    host = urlparse(url or "").hostname or ""
    return (
        host in ("localhost", "::1")
        or "." not in host
        or bool(re.match(r"^(127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)", host))
        or host.endswith((".local", ".lan", ".home", ".internal"))
    )


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
