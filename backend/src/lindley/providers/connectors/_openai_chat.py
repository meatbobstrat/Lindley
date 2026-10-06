"""Shared helper for connectors that speak the OpenAI Chat Completions API: Ollama, LM Studio,
vLLM and other OpenAI-compatible services. They all document using OpenAI's own library with
their address as `base_url`, so that's what this does.

A connector built on it is a subclass that sets `info`. (OpenAI's own connector uses the
newer Responses API for chat, and this class for embeddings.)
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from urllib.parse import urlparse

import openai

from lindley.providers.base import (
    ChatMessage,
    ConnectorInfo,
    ProviderError,
    Transcription,
    Usage,
)
from lindley.providers.connectors._common import b64, cut_off, image_type, sdk_errors
from lindley.providers.prompts import transcribe_prompt


class OpenAIChat:
    """Implements ChatProvider, VisionProvider and EmbeddingProvider over the OpenAI API."""

    info: ConnectorInfo
    # Sent with each page to read and each question asked in one piece (chat: the assembler's
    # sorting questions), which want a short answer, e.g. to turn a local model's thinking off.
    # A server that doesn't take them is asked again without them, once, and isn't sent them
    # again. A streamed answer (Ask Lindley) is sent without them.
    quick_options: dict = {}
    # Told what each call used, even one that then fails (it's charged): set by throttle.Guarded.
    on_usage: Callable[[Usage], None] | None = None

    def __init__(
        self,
        config=None,
        model: str | None = None,
        api_key: str | None = None,
        http_client=None,  # an httpx2.Client, for tests
    ) -> None:
        base = (config.base_url if config else None) or self.info.default_base_url or ""
        self.base_url = base.rstrip("/")
        self.model = model or self.info.default_models.get("chat")
        host = urlparse(self.base_url).hostname or self.base_url
        self.who = self.info.company or f"The AI at {host}"
        # With no address there's no client: the library would fall back to OpenAI's own.
        self._client = (
            openai.OpenAI(
                # Always a key: with none, the library sends OPENAI_API_KEY from the environment,
                # which mustn't reach another service. Local servers ignore it.
                api_key=api_key or "none",
                base_url=self.base_url,
                timeout=(config.timeout_s if config else None) or self.info.timeout_s,
                max_retries=0,  # see _common: a busy AI is tried again by the throttle
                http_client=http_client,
            )
            if self.base_url
            else None
        )

    @property
    def client(self) -> openai.OpenAI:
        if self._client is None:
            raise ProviderError("No address is set for this AI")
        return self._client

    def _errors(self):
        return sdk_errors(openai, self.who, self.base_url)

    def _used(self, sent: int | None, written: int | None, details=None) -> None:
        """Tell what a call used. OpenAI's APIs count the tokens read from a cache, and those
        written to it, among those sent (`details` says how many); Usage doesn't."""
        if self.on_usage is not None:
            cached = getattr(details, "cached_tokens", None) or 0
            cache_write = getattr(details, "cache_write_tokens", None) or 0
            rest = max(0, (sent or 0) - cached - cache_write)
            self.on_usage(Usage(self.model, rest, written or 0, cached, cache_write))

    def _completed(self, usage) -> None:
        """Chat Completions' usage, when the server says (Ollama and LM Studio do)."""
        if usage is not None:
            details = getattr(usage, "prompt_tokens_details", None)
            self._used(usage.prompt_tokens, usage.completion_tokens, details)

    def _complete(self, messages: list[dict], **options) -> str:
        with self._errors():
            try:
                r = self.client.chat.completions.create(
                    model=self.model, messages=messages, **options
                )
            except (openai.BadRequestError, openai.UnprocessableEntityError):
                if not options:
                    raise
                # Refused before the AI did any work, so this isn't repeating a failed call
                self.quick_options = {}
                r = self.client.chat.completions.create(model=self.model, messages=messages)
        self._completed(r.usage)
        if not r.choices:
            raise ProviderError(f"{self.who} sent back an answer with no text")
        self._finished(r.choices[0].finish_reason)
        return r.choices[0].message.content or ""

    def _finished(self, reason: str | None) -> None:
        """An answer cut off, or held back, is a failed call. An empty one that finished is an
        answer: a page with nothing written on it is read as nothing."""
        if reason == "length":
            raise cut_off(self.who)
        if reason == "content_filter":
            raise ProviderError(f"{self.who} declined to answer this")

    def chat(self, messages: list[ChatMessage]) -> str:
        return self._complete(
            [{"role": m.role, "content": m.content} for m in messages], **self.quick_options
        )

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        sent = [{"role": m.role, "content": m.content} for m in messages]
        with self._errors():
            try:  # what it used comes in a last chunk of its own, after the answer's end
                stream = self.client.chat.completions.create(
                    model=self.model,
                    messages=sent,
                    stream=True,
                    stream_options={"include_usage": True},
                )
            except (openai.BadRequestError, openai.UnprocessableEntityError):
                # Refused before the AI did any work: a server that doesn't take the option
                stream = self.client.chat.completions.create(
                    model=self.model, messages=sent, stream=True
                )
            reason = None
            for chunk in stream:
                self._completed(chunk.usage)
                if not chunk.choices:
                    continue
                if piece := chunk.choices[0].delta.content:
                    yield piece
                reason = chunk.choices[0].finish_reason or reason
            self._finished(reason)

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        url = f"data:{image_type(image)};base64,{b64(image)}"
        content = [
            {"type": "text", "text": transcribe_prompt(hints)},
            {"type": "image_url", "image_url": {"url": url}},
        ]
        text = self._complete([{"role": "user", "content": content}], **self.quick_options)
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def embed(self, texts: list[str]) -> list[list[float]]:
        with self._errors():
            r = self.client.embeddings.create(model=self.model, input=texts)
        if r.usage is not None:
            self._used(r.usage.prompt_tokens, 0)
        rows = sorted(r.data, key=lambda d: d.index)
        if len(rows) != len(texts):
            raise ProviderError(f"{self.who} sent back {len(rows)} embeddings for {len(texts)}")
        return [list(d.embedding) for d in rows]

    def check(self) -> str:
        """Ask for the server's models, and look for ours among them."""
        with self._errors():
            ids = [m.id.removeprefix("models/") for m in self.client.models.list()]
        if any(i == self.model or i.split(":")[0] == self.model for i in ids):
            return f"Connected. {self.who} has {self.model}."
        have = ", ".join(sorted(ids)[:8]) or "none"
        raise ProviderError(f"{self.who} answered, but has no model {self.model}. It has: {have}")
