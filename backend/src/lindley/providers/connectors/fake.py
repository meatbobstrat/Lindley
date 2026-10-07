"""Deterministic in-process connector for tests and offline development."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from lindley.providers.base import JOBS, ChatMessage, ConnectorInfo, Transcription, Usage

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

    on_usage = None  # set by throttle.Guarded: told a word a token

    def __init__(self, config=None, model: str | None = None, api_key: str | None = None) -> None:
        self.model = model or "fake"

    def _used(self, sent: int, written: str) -> None:
        if self.on_usage is not None:
            self.on_usage(Usage(self.model, sent, len(written.split())))

    def chat(self, messages: list[ChatMessage]) -> str:
        last = messages[-1].content if messages else ""
        reply = f"[{self.model}] {last}"
        self._used(sum(len(m.content.split()) for m in messages), reply)
        return reply

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        yield from self.chat(messages).split(" ")

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        text = f"<{len(image)} bytes transcribed>"
        self._used(1000, text)  # an image is about a thousand tokens
        return Transcription(text=text, confidence=100.0)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [
            [b / 255 for b in hashlib.sha256(t.encode()).digest()[:EMBEDDING_DIM]] for t in texts
        ]

    def p_yes(self, system: str, question: str) -> float:
        """Undecided, so the rules decide as they would alone."""
        self._used(len(question.split()), "yes")
        return 0.5

    def check(self) -> str:
        return f"Connected. {self.model} answered."


Provider = FakeProvider
