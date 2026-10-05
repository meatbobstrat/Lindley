"""Provider-neutral interfaces for AI models used by chat, OCR and search.

Connectors (one file each in `connectors/`) implement these so the rest of the app never
depends on a specific vendor's API or on local-vs-hosted models.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Literal, Protocol, runtime_checkable

# The jobs Lindley gives an AI. They match ai_calls.purpose in the database.
Job = Literal["vision", "assemble", "chat", "embed"]
JOBS: tuple[Job, ...] = ("vision", "assemble", "chat", "embed")


@dataclass(frozen=True)
class ChatMessage:
    role: Literal["system", "user", "assistant"]
    content: str


@dataclass(frozen=True)
class Transcription:
    text: str
    # Model-reported or estimated confidence, 0-100; None if the provider can't say.
    confidence: float | None = None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Usage:
    """What one call used, as the AI reported it: the tokens it was sent (input, not counting
    any read from or written to its cache) and the tokens it wrote, thinking included."""

    model: str | None
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@runtime_checkable
class ChatProvider(Protocol):
    def chat(self, messages: list[ChatMessage]) -> str: ...

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]: ...


@runtime_checkable
class VisionProvider(Protocol):
    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription: ...


@runtime_checkable
class EmbeddingProvider(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]: ...


@dataclass(frozen=True)
class ConnectorInfo:
    """What a connector is, for settings and the UI. Each connector file defines one as INFO."""

    id: str  # the provider's `type` in settings.json
    label: str  # e.g. "Anthropic (Claude)"
    where: Literal["local", "cloud"]  # local: runs on computers you control
    jobs: frozenset[Job]  # the jobs it can do
    default_models: Mapping[Job, str] = field(default_factory=dict)
    company: str | None = None  # who sees your pages; None when it runs on your own computers
    default_base_url: str | None = None
    needs_key: bool = False
    key_url: str | None = None  # where to get a key
    hidden: bool = False  # not offered in the UI (the fake connector for tests)
    short: str | None = None  # a new connection's name, e.g. "Claude" (None: the label)
    timeout_s: int = 120  # how long to wait for an answer, unless the connection says


class ProviderError(RuntimeError):
    """Raised when a provider is misconfigured or a call fails. `busy`: the AI said it's busy
    (lindley.providers.throttle tries again), after `retry_after` seconds if it said."""

    def __init__(self, message: str, *, busy: bool = False, retry_after: float | None = None):
        super().__init__(message)
        self.busy = busy
        self.retry_after = retry_after
