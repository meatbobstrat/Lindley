"""Anthropic's Claude, through Anthropic's library and the Messages API. Not private. Claude has
no embeddings."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Literal

import anthropic

from lindley.providers.base import ChatMessage, ConnectorInfo, ProviderError, Transcription
from lindley.providers.connectors._common import b64, cut_off, image_type, sdk_errors
from lindley.providers.prompts import transcribe_prompt

INFO = ConnectorInfo(
    id="anthropic",
    label="Anthropic (Claude)",
    short="Claude",
    where="cloud",
    company="Anthropic",
    jobs=frozenset({"vision", "assemble", "chat"}),
    default_models={
        "vision": "claude-opus-5-5",
        "assemble": "claude-opus-5-5",
        "chat": "claude-opus-5-5",
    },
    default_base_url="https://api.anthropic.com",
    needs_key=True,
    key_url="https://platform.claude.com/settings/keys",
)

MAX_TOKENS = 16000
DECLINED = "Claude declined to answer this"
# If one of these models declines a request, Anthropic runs it again on the model it
# recommends for that kind of refusal, in the same call, instead of returning the refusal.
FALLBACK_BETA = "server-side-fallback-2026-07-01"
FALLBACK_MODELS = frozenset(
    {"claude-opus-5", "claude-opus-5-5", "claude-sonnet-5-5", "claude-fable-5", "claude-fable-5-1"}
)
# How hard the model thinks: low, medium, high, xhigh or max. None: the model's own default
# (medium on Opus 5.5). Thinking can't be turned off on the newest models; less effort is the
# way to spend fewer tokens on it.
Effort = Literal["low", "medium", "high", "xhigh", "max"]


def _finished(stop_reason: str | None) -> None:
    if stop_reason == "refusal":
        raise ProviderError(DECLINED)
    if stop_reason == "max_tokens":
        raise cut_off("Anthropic")


def _split(messages: list[ChatMessage]) -> tuple[str | None, list[dict]]:
    system = "\n\n".join(m.content for m in messages if m.role == "system") or None
    rest = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
    return system, rest


class Provider:
    """Implements ChatProvider and VisionProvider."""

    def __init__(
        self,
        config=None,
        model: str | None = None,
        api_key: str | None = None,
        http_client=None,  # an httpx2.Client, for tests
        effort: Effort | None = None,
    ) -> None:
        self.model = model or INFO.default_models["chat"]
        self.effort = effort
        # With no key there's no client: the library would use ANTHROPIC_API_KEY instead.
        self._client = (
            anthropic.Anthropic(
                api_key=api_key,
                base_url=(config.base_url if config else None) or INFO.default_base_url,
                timeout=(config.timeout_s if config else None) or INFO.timeout_s,
                max_retries=0,  # see _common: a busy AI is tried again by the throttle
                http_client=http_client,
            )
            if api_key
            else None
        )

    def _messages(self):
        """The messages API, and what to add to each request for this model."""
        if self._client is None:
            raise ProviderError("No API key is saved for Anthropic")
        if self.model in FALLBACK_MODELS:
            return self._client.beta.messages, {"betas": [FALLBACK_BETA], "fallbacks": "default"}
        return self._client.messages, {}

    def _params(self, system: str | None, messages: list[dict]) -> dict:
        params: dict = {"model": self.model, "max_tokens": MAX_TOKENS, "messages": messages}
        if system:
            params["system"] = system
        if self.effort:
            params["output_config"] = {"effort": self.effort}
        return params

    def _create(self, system: str | None, messages: list[dict]) -> str:
        api, extra = self._messages()
        with sdk_errors(anthropic, "Anthropic"):
            r = api.create(**self._params(system, messages), **extra)
        _finished(r.stop_reason)
        return "".join(b.text for b in r.content if b.type == "text")

    def chat(self, messages: list[ChatMessage]) -> str:
        return self._create(*_split(messages))

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        api, extra = self._messages()
        with (
            sdk_errors(anthropic, "Anthropic"),
            api.stream(**self._params(*_split(messages)), **extra) as stream,
        ):
            yield from stream.text_stream
            _finished(stream.get_final_message().stop_reason)

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        content = [
            {
                "type": "image",
                "source": {"type": "base64", "media_type": image_type(image), "data": b64(image)},
            },
            {"type": "text", "text": transcribe_prompt(hints)},
        ]
        text = self._create(None, [{"role": "user", "content": content}])
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def check(self) -> str:
        if self._client is None:
            raise ProviderError("No API key is saved for Anthropic")
        with sdk_errors(anthropic, "Anthropic"):
            model = self._client.models.retrieve(self.model)
        return f"Connected. Anthropic has {model.display_name or self.model}."
