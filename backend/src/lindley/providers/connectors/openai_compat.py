"""Any other service that speaks the OpenAI API, at an address you give it."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_wire import OpenAIWire

INFO = ConnectorInfo(
    id="openai_compat",
    label="Another OpenAI-compatible service",
    where="cloud",
    jobs=frozenset(JOBS),
)


class Provider(OpenAIWire):
    info = INFO
