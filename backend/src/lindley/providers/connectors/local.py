"""An AI on a computer you control: Ollama, LM Studio, vLLM or similar, through OpenAI's library
at its address, as they document. Private."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_chat import OpenAIChat

INFO = ConnectorInfo(
    id="local",
    label="Ollama or LM Studio on a computer you own",
    where="local",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "llama3.2-vision",
        "assemble": "llama3.2-vision",
        "chat": "llama3.2-vision",
        "embed": "nomic-embed-text",
    },
    default_base_url="http://localhost:11434/v1",
)


class Provider(OpenAIChat):
    info = INFO
