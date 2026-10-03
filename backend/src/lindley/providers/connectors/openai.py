"""OpenAI's cloud. Every page and question sent to it leaves this computer."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_wire import OpenAIWire

INFO = ConnectorInfo(
    id="openai",
    label="OpenAI",
    where="cloud",
    company="OpenAI",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "gpt-4o",
        "assemble": "gpt-4o",
        "chat": "gpt-4o",
        "embed": "text-embedding-3-small",
    },
    default_base_url="https://api.openai.com/v1",
    needs_key=True,
    key_url="https://platform.openai.com/api-keys",
)


class Provider(OpenAIWire):
    info = INFO
