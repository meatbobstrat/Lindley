"""Provider-neutral interfaces for AI models used by chat, OCR and search.

Concrete adapters (Anthropic, OpenAI-compatible, fake) implement these so the rest of
the app never depends on a specific vendor SDK or on local-vs-hosted models.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Literal, Protocol, runtime_checkable


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


class ProviderError(RuntimeError):
    """Raised when a provider is misconfigured or a call fails."""
