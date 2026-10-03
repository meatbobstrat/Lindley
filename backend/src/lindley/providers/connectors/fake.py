"""Deterministic in-process connector for tests and offline development."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from lindley.providers.base import JOBS, ChatMessage, ConnectorInfo, Transcription

EMBEDDING_DIM = 8

INFO = ConnectorInfo(
    id="fake",
    label="Fake AI for tests",
    where="local",
    jobs=frozenset(JOBS),
    default_models={job: "fake" for job in JOBS},
    hidden=True,
)


class FakeProvider:
    """Implements ChatProvider, VisionProvider and EmbeddingProvider."""

    def __init__(self, config=None, model: str | None = None, api_key: str | None = None) -> None:
        self.model = model or "fake"

    def chat(self, messages: list[ChatMessage]) -> str:
        last = messages[-1].content if messages else ""
        return f"[{self.model}] {last}"

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        yield from self.chat(messages).split(" ")

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        return Transcription(text=f"<{len(image)} bytes transcribed>", confidence=100.0)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [
            [b / 255 for b in hashlib.sha256(t.encode()).digest()[:EMBEDDING_DIM]] for t in texts
        ]

    def check(self) -> str:
        return f"Connected. {self.model} answered."


Provider = FakeProvider
