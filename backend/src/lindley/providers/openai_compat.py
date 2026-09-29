"""Adapter for any OpenAI-compatible API: OpenAI, Ollama, LM Studio, vLLM, etc.

Local servers work by pointing `base_url` at them (e.g. http://localhost:11434/v1 for Ollama).
Stub: signatures only; implemented in the AI phase.
"""

from __future__ import annotations

from collections.abc import Iterator

from lindley.providers.base import ChatMessage, Transcription


class OpenAICompatProvider:
    """Implements ChatProvider, VisionProvider (for vision-capable models) and EmbeddingProvider."""

    def __init__(
        self, base_url: str | None = None, model: str | None = None, api_key: str | None = None
    ) -> None:
        self.base_url = base_url
        self.model = model
        self.api_key = api_key

    def chat(self, messages: list[ChatMessage]) -> str:
        raise NotImplementedError

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        raise NotImplementedError

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        raise NotImplementedError

    def embed(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError
