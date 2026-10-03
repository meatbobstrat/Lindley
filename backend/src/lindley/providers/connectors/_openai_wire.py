"""Shared helper for connectors that speak the OpenAI Chat Completions API.

Stub: signatures only; implemented with the HTTP connectors.
"""

from __future__ import annotations

from collections.abc import Iterator

from lindley.providers.base import ChatMessage, ConnectorInfo, Transcription


class OpenAIWire:
    """Implements ChatProvider, VisionProvider and EmbeddingProvider over the OpenAI API."""

    info: ConnectorInfo

    def __init__(self, config=None, model: str | None = None, api_key: str | None = None) -> None:
        self.base_url = (config.base_url if config else None) or self.info.default_base_url
        self.model = model or self.info.default_models.get("chat")
        self.api_key = api_key

    def chat(self, messages: list[ChatMessage]) -> str:
        raise NotImplementedError

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        raise NotImplementedError

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        raise NotImplementedError

    def embed(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError

    def check(self) -> str:
        raise NotImplementedError
