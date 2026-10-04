"""Google's Gemini, through Google's library (google-genai): the Interactions API for chat and
reading pages, which Google recommends for new projects, and embed_content for embeddings.
Not private."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import httpx
from google import genai
from google.genai import types
from google.genai._gaos.utils.retries import BackoffStrategy, RetryConfig

from lindley.providers.base import JOBS, ChatMessage, ConnectorInfo, ProviderError, Transcription
from lindley.providers.connectors._common import (
    b64,
    cut_off,
    detail,
    failure,
    image_type,
    retry_after,
)
from lindley.providers.prompts import transcribe_prompt

INFO = ConnectorInfo(
    id="google",
    label="Google (Gemini)",
    short="Gemini",
    where="cloud",
    company="Google",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "gemini-3.8-flash",
        "assemble": "gemini-3.8-flash",
        "chat": "gemini-3.8-flash",
        "embed": "gemini-embedding-2",
    },
    default_base_url="https://generativelanguage.googleapis.com",
    needs_key=True,
    key_url="https://aistudio.google.com/apikey",
)


_NO_RETRIES = RetryConfig("none", BackoffStrategy(0, 0, 1, 0), False)


@contextmanager
def _errors() -> Iterator[None]:
    """The library's errors, in words for a person. Its Interactions errors aren't public
    classes, so they're known by where they come from and their status code."""
    try:
        yield
    except httpx.TimeoutException as e:
        raise ProviderError("Google took too long to answer") from e
    except httpx.HTTPError as e:
        raise ProviderError(f"Couldn't reach Google: {e}") from e
    except Exception as e:
        if not type(e).__module__.startswith("google.genai"):
            raise
        if "Timeout" in type(e).__name__:
            raise ProviderError("Google took too long to answer") from e
        status = getattr(e, "status_code", None) or getattr(e, "code", None)
        if not isinstance(status, int):
            raise ProviderError(f"Couldn't reach Google: {e}") from e
        body = getattr(e, "body", None) or getattr(e, "details", None)
        response = getattr(e, "response", None)
        headers = getattr(e, "headers", None) or getattr(response, "headers", None)
        why = detail(body, getattr(e, "message", None) or e)
        raise failure("Google", status, why, retry_after(headers)) from e


def _finished(status: str | None) -> None:
    """An interaction that stopped before its end, or failed, is a failed call."""
    if status in ("incomplete", "budget_exceeded"):
        raise cut_off("Google")
    if status in ("failed", "cancelled"):
        raise ProviderError(f"Google couldn't finish the answer ({status})")


def _text(interaction) -> str:
    _finished(getattr(interaction, "status", None))
    return interaction.output_text or ""


def _content(text: str) -> list[dict]:
    return [{"type": "text", "text": text}]


def _steps(messages: list[ChatMessage]) -> tuple[str | None, list[dict]]:
    """The system prompt, and the conversation as Interactions steps."""
    system = "\n\n".join(m.content for m in messages if m.role == "system") or None
    steps = [
        {
            "type": "model_output" if m.role == "assistant" else "user_input",
            "content": _content(m.content),
        }
        for m in messages
        if m.role != "system"
    ]
    return system, steps


class Provider:
    """Implements ChatProvider, VisionProvider and EmbeddingProvider."""

    def __init__(
        self,
        config=None,
        model: str | None = None,
        api_key: str | None = None,
        http_client: httpx.Client | None = None,  # for tests
    ) -> None:
        self.model = model or INFO.default_models["chat"]
        # With no key there's no client: the library would use GEMINI_API_KEY instead.
        self._client = (
            genai.Client(
                vertexai=False,
                api_key=api_key,
                http_options=types.HttpOptions(
                    base_url=(config.base_url if config else None) or INFO.default_base_url,
                    timeout=((config.timeout_s if config else None) or INFO.timeout_s) * 1000,
                    # One try: the library would repeat a call that took too long, too (see
                    # _common). A busy Gemini is tried again by the throttle.
                    retry_options=types.HttpRetryOptions(attempts=1),
                    httpx_client=http_client,
                ),
            )
            if api_key
            else None
        )

    @property
    def client(self) -> genai.Client:
        if self._client is None:
            raise ProviderError("No API key is saved for Google")
        return self._client

    def _interact(self, system: str | None, steps: list[dict], **kw):
        interactions = self.client.interactions
        # Its Interactions client takes HttpRetryOptions.attempts as a count of retries, not of
        # tries, so it's told plainly not to try again.
        interactions.sdk_configuration.retry_config = _NO_RETRIES
        # store=False: Google keeps interactions unless asked not to. The whole conversation is
        # sent each time instead.
        return interactions.create(
            model=self.model, system_instruction=system, input=steps, store=False, **kw
        )

    def chat(self, messages: list[ChatMessage]) -> str:
        with _errors():
            return _text(self._interact(*_steps(messages)))

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        with _errors():
            for event in self._interact(*_steps(messages), stream=True):
                if event.event_type == "step.delta" and event.delta.type == "text":
                    yield event.delta.text
                elif event.event_type == "interaction.completed":
                    _finished(getattr(event.interaction, "status", None))
                elif event.event_type == "error":
                    why = event.error.message if event.error else "the answer failed"
                    raise ProviderError(f"Google: {why}")

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        content = [
            *_content(transcribe_prompt(hints)),
            {"type": "image", "mime_type": image_type(image), "data": b64(image)},
        ]
        with _errors():
            text = _text(self._interact(None, [{"type": "user_input", "content": content}]))
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def embed(self, texts: list[str]) -> list[list[float]]:
        with _errors():
            r = self.client.models.embed_content(model=self.model, contents=texts)
        rows = r.embeddings or []
        if len(rows) != len(texts):
            raise ProviderError(f"Google sent back {len(rows)} embeddings for {len(texts)}")
        return [list(e.values or []) for e in rows]

    def check(self) -> str:
        with _errors():
            model = self.client.models.get(model=self.model)
        return f"Connected. Google has {model.display_name or self.model}."
