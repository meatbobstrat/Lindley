"""Anthropic's Claude, through the Messages API. Not private. Claude has no embeddings.

Stub: signatures only; implemented with the HTTP connectors.
"""

from __future__ import annotations

from collections.abc import Iterator

from lindley.providers.base import ChatMessage, ConnectorInfo, Transcription

INFO = ConnectorInfo(
    id="anthropic",
    label="Anthropic (Claude)",
    where="cloud",
    company="Anthropic",
    jobs=frozenset({"vision", "assemble", "chat"}),
    default_models={
        "vision": "claude-opus-5",
        "assemble": "claude-opus-5",
        "chat": "claude-opus-5",
    },
    default_base_url="https://api.anthropic.com",
    needs_key=True,
    key_url="https://platform.claude.com/settings/keys",
)


class Provider:
    """Implements ChatProvider and VisionProvider."""

    def __init__(self, config=None, model: str | None = None, api_key: str | None = None) -> None:
        self.base_url = (config.base_url if config else None) or INFO.default_base_url
        self.model = model or INFO.default_models["chat"]
        self.api_key = api_key

    def chat(self, messages: list[ChatMessage]) -> str:
        raise NotImplementedError

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        raise NotImplementedError

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        raise NotImplementedError

    def check(self) -> str:
        raise NotImplementedError
