"""Anthropic (Claude) adapter. Requires the `anthropic` extra.

Stub: signatures only; implemented in the AI phase.
"""

from __future__ import annotations

from collections.abc import Iterator

from lindley.providers.base import ChatMessage, Transcription

DEFAULT_MODEL = "claude-sonnet-5"


class AnthropicProvider:
    """Implements ChatProvider and VisionProvider. Claude has no embeddings endpoint."""

    def __init__(self, model: str | None = None, api_key: str | None = None) -> None:
        self.model = model or DEFAULT_MODEL
        self.api_key = api_key

    def chat(self, messages: list[ChatMessage]) -> str:
        raise NotImplementedError

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        raise NotImplementedError

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        raise NotImplementedError
