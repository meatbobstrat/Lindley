"""An AI on a computer you control: Ollama, LM Studio, vLLM or similar, through OpenAI's library
at its address, as they document. Private."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_chat import OpenAIChat

INFO = ConnectorInfo(
    id="local",
    label="Ollama or LM Studio on a computer you own",
    short="AI on this computer",
    where="local",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "gemma4:e4b",
        "assemble": "gemma4:e4b",
        "chat": "gemma4:e4b",
        "embed": "nomic-embed-text",
    },
    default_base_url="http://localhost:11434/v1",
    # A laptop with no graphics card may take minutes over a page
    timeout_s=600,
)


class Provider(OpenAIChat):
    info = INFO
    # Reading a page needs no thinking, and a thinking model can use up a local AI's small
    # context thinking and send back nothing. "none" turns it off on Ollama (and LM Studio).
    transcribe_options = {"reasoning_effort": "none"}
