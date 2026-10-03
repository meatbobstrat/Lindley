"""Google's Gemini, through Google's OpenAI-compatible endpoint. Not private."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_wire import OpenAIWire

INFO = ConnectorInfo(
    id="google",
    label="Google (Gemini)",
    where="cloud",
    company="Google",
    jobs=frozenset(JOBS),
    default_models={
        "vision": "gemini-2.5-flash",
        "assemble": "gemini-2.5-flash",
        "chat": "gemini-2.5-flash",
        "embed": "gemini-embedding-001",
    },
    default_base_url="https://generativelanguage.googleapis.com/v1beta/openai",
    needs_key=True,
    key_url="https://aistudio.google.com/apikey",
)


class Provider(OpenAIWire):
    info = INFO
