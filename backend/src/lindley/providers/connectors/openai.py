"""OpenAI's cloud, through OpenAI's library and its Responses API, which OpenAI recommends for
new projects. Every page and question sent to it leaves this computer."""

from __future__ import annotations

from collections.abc import Iterator

import openai

from lindley.providers.base import JOBS, ChatMessage, ConnectorInfo, ProviderError, Transcription
from lindley.providers.connectors._common import b64, image_type
from lindley.providers.connectors._openai_chat import OpenAIChat
from lindley.providers.prompts import transcribe_prompt

INFO = ConnectorInfo(
    id="openai",
    label="OpenAI",
    short="OpenAI",
    where="cloud",
    company="OpenAI",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "gpt-6.1-sol",
        "assemble": "gpt-6.1-sol",
        "chat": "gpt-6.1-sol",
        "embed": "text-embedding-3-small",
    },
    default_base_url="https://api.openai.com/v1",
    needs_key=True,
    key_url="https://platform.openai.com/api-keys",
)

DECLINED = "OpenAI declined to answer this"


def _split(messages: list[ChatMessage]) -> tuple[str | openai.Omit, list[dict]]:
    system = "\n\n".join(m.content for m in messages if m.role == "system") or openai.omit
    rest = [{"role": m.role, "content": m.content} for m in messages if m.role != "system"]
    return system, rest


class Provider(OpenAIChat):
    """Chat and reading pages through Responses; embeddings and the model list as for any
    OpenAI-compatible service."""

    info = INFO

    def _respond(self, instructions: str | openai.Omit, items: list[dict]) -> str:
        with self._errors():
            # store=False: OpenAI keeps responses unless asked not to
            r = self.client.responses.create(
                model=self.model, instructions=instructions, input=items, store=False
            )
        for item in r.output:
            if item.type == "message" and any(c.type == "refusal" for c in item.content):
                raise ProviderError(DECLINED)
        return r.output_text

    def chat(self, messages: list[ChatMessage]) -> str:
        return self._respond(*_split(messages))

    def chat_stream(self, messages: list[ChatMessage]) -> Iterator[str]:
        instructions, items = _split(messages)
        with self._errors():
            stream = self.client.responses.create(
                model=self.model, instructions=instructions, input=items, store=False, stream=True
            )
            for event in stream:
                if event.type == "response.output_text.delta":
                    yield event.delta
                elif event.type == "response.refusal.delta":
                    raise ProviderError(DECLINED)
                elif event.type == "response.failed":
                    error = event.response.error
                    raise ProviderError(
                        f"OpenAI: {error.message if error else 'the answer failed'}"
                    )
                elif event.type == "error":
                    raise ProviderError(f"OpenAI: {event.message}")

    def transcribe(self, image: bytes, hints: str | None = None) -> Transcription:
        content = [
            {"type": "input_text", "text": transcribe_prompt(hints)},
            {
                "type": "input_image",
                "image_url": f"data:{image_type(image)};base64,{b64(image)}",
                "detail": "high",  # handwriting needs the page at full size
            },
        ]
        text = self._respond(openai.omit, [{"role": "user", "content": content}])
        return Transcription(text=text.strip(), metadata={"model": self.model})

    def check(self) -> str:
        with self._errors():
            model = self.client.models.retrieve(self.model)
        return f"Connected. OpenAI has {model.id}."
