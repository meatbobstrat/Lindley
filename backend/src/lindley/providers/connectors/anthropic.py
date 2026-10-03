"""Anthropic's Claude, through the Messages API. Not private. Claude has no embeddings."""

from __future__ import annotations

import json
from collections.abc import Iterator

import httpx

from lindley.providers.base import (
    ChatMessage,
    ConnectorInfo,
    ProviderBusy,
    ProviderError,
    Transcription,
)
from lindley.providers.connectors._http import b64, image_type, make_client, send, stream_data
from lindley.providers.prompts import transcribe_prompt

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

API_VERSION = "2023-06-01"
MAX_TOKENS = 16000
# If one of these models declines a request, Anthropic runs it again on the model it
# recommends for that kind of refusal, in the same call, instead of returning the refusal.
FALLBACK_BETA = "server-side-fallback-2026-07-01"
FALLBACK_MODELS = frozenset(
    {"claude-opus-5", "claude-opus-5-5", "claude-fable-5", "claude-fable-5-1"}
)


class Provider:
    """Implements ChatProvider and VisionProvider."""

    def __init__(
        self,
        config=None,
        model: str | None = None,
        api_key: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        base = (config.base_url if config else None) or INFO.default_base_url
        self.base_url = base.rstrip("/")
        self.model = model or INFO.default_models["chat"]
        self.api_key = api_key
        self.http = make_client(config, client)

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            raise ProviderError("No API key is saved for Anthropic")
        headers = {"x-api-key": self.api_key, "anthropic-version": API_VERSION}
        if self.model in FALLBACK_MODELS:
            headers["anthropic-beta"] = FALLBACK_BETA
        return headers

    def _body(self, messages: list[dict], system: str = "", stream: bool = False) -> dict:
        body: dict = {"model": self.model, "max_tokens": MAX_TOKENS, "messages": messages}
        if system:
            body["system"] = system
        if stream:
            body["stream"] = True
        if self.model in FALLBACK_MODELS:
            body["fallbacks"] = "default"
        return body

    @staticmethod
    def _split(messages: list[ChatMessage]) -> tuple[str, list[dict]]:
        system = "\n\n".join(m.content for m in messages if m.role == "system")
        rest = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
        return system, rest

    def _create(self, messages: list[dict], system: str = "") -> str:
        data = send(
            self.http,
            "POST",
            f"{self.base_url}/v1/messages",
            "Anthropic",
            headers=self._headers(),
            json=self._body(messages, system),
        )
        if data.get("stop_reason") == "refusal":
            raise ProviderError("Claude declined to answer this")
        return "".join(b.get("text", "") for b in data.get("content") or [] if b["type"] == "text")

    def chat(self, messages: list[ChatMessage]) -> str:
        system, rest = self._split(messages)
        return self._create(rest, system)

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        system, rest = self._split(messages)
        url = f"{self.base_url}/v1/messages"
        body = self._body(rest, system, stream=True)
        for data in stream_data(self.http, url, "Anthropic", headers=self._headers(), json=body):
            try:
                event = json.loads(data)
            except json.JSONDecodeError:
                continue
            kind = event.get("type")
            if kind == "content_block_delta" and event["delta"].get("type") == "text_delta":
                yield event["delta"]["text"]
            elif kind == "message_delta" and event["delta"].get("stop_reason") == "refusal":
                raise ProviderError("Claude declined to answer this")
            elif kind == "error":
                err = event.get("error") or {}
                if err.get("type") in ("overloaded_error", "rate_limit_error"):
                    raise ProviderBusy(f"Anthropic is busy: {err.get('message', '')}")
                raise ProviderError(f"Anthropic: {err.get('message', err)}")

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        content = [
            {
                "type": "image",
                "source": {"type": "base64", "media_type": image_type(image), "data": b64(image)},
            },
            {"type": "text", "text": transcribe_prompt(hints)},
        ]
        text = self._create([{"role": "user", "content": content}])
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def check(self) -> str:
        data = send(
            self.http,
            "GET",
            f"{self.base_url}/v1/models/{self.model}",
            "Anthropic",
            headers=self._headers(),
        )
        return f"Connected. Anthropic has {data.get('display_name') or self.model}."
