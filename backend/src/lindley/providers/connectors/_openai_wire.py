"""Shared helper for connectors that speak the OpenAI Chat Completions API: OpenAI itself,
Google's OpenAI-compatible endpoint, Ollama, LM Studio, vLLM and others.

A connector built on it is a subclass that sets `info`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from urllib.parse import urlparse

import httpx

from lindley.providers.base import ChatMessage, ConnectorInfo, ProviderError, Transcription
from lindley.providers.connectors._http import b64, image_type, make_client, send, stream_data
from lindley.providers.prompts import transcribe_prompt


class OpenAIWire:
    """Implements ChatProvider, VisionProvider and EmbeddingProvider over the OpenAI API."""

    info: ConnectorInfo

    def __init__(
        self,
        config=None,
        model: str | None = None,
        api_key: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        base = (config.base_url if config else None) or self.info.default_base_url or ""
        self.base_url = base.rstrip("/")
        self.model = model or self.info.default_models.get("chat")
        self.api_key = api_key
        self.http = make_client(config, client)
        host = urlparse(self.base_url).hostname or self.base_url
        self.who = self.info.company or f"The AI at {host}"

    def _url(self, path: str) -> str:
        if not self.base_url:
            raise ProviderError("No address is set for this AI")
        return self.base_url + path

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

    def _complete(self, messages: list[dict]) -> str:
        data = send(
            self.http,
            "POST",
            self._url("/chat/completions"),
            self.who,
            headers=self._headers(),
            json={"model": self.model, "messages": messages},
        )
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise ProviderError(f"{self.who} sent back an answer with no text") from e
        if isinstance(content, list):  # some servers answer in parts
            content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
        return content or ""

    def chat(self, messages: list[ChatMessage]) -> str:
        return self._complete([{"role": m.role, "content": m.content} for m in messages])

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        body = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "stream": True,
        }
        url = self._url("/chat/completions")
        for data in stream_data(self.http, url, self.who, headers=self._headers(), json=body):
            if data == "[DONE]":
                return
            try:
                choices = json.loads(data).get("choices") or [{}]
            except json.JSONDecodeError:
                continue
            if piece := (choices[0].get("delta") or {}).get("content"):
                yield piece

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        url = f"data:{image_type(image)};base64,{b64(image)}"
        content = [
            {"type": "text", "text": transcribe_prompt(hints)},
            {"type": "image_url", "image_url": {"url": url}},
        ]
        text = self._complete([{"role": "user", "content": content}])
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def embed(self, texts: list[str]) -> list[list[float]]:
        data = send(
            self.http,
            "POST",
            self._url("/embeddings"),
            self.who,
            headers=self._headers(),
            json={"model": self.model, "input": texts},
        )
        rows = sorted(data.get("data") or [], key=lambda d: d.get("index", 0))
        if len(rows) != len(texts):
            raise ProviderError(f"{self.who} sent back {len(rows)} embeddings for {len(texts)}")
        return [r["embedding"] for r in rows]

    def check(self) -> str:
        """Ask for the server's models, and look for ours among them."""
        data = send(self.http, "GET", self._url("/models"), self.who, headers=self._headers())
        ids = [str(m.get("id", "")).removeprefix("models/") for m in data.get("data") or []]
        if any(i == self.model or i.split(":")[0] == self.model for i in ids):
            return f"Connected. {self.who} has {self.model}."
        have = ", ".join(sorted(ids)[:8]) or "none"
        raise ProviderError(f"{self.who} answered, but has no model {self.model}. It has: {have}")
