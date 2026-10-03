"""Any other service that speaks the OpenAI API, at an address you give it, through OpenAI's
library."""

from __future__ import annotations

from lindley.providers.base import JOBS, ConnectorInfo
from lindley.providers.connectors._openai_chat import OpenAIChat

INFO = ConnectorInfo(
    id="openai_compat",
    label="Another OpenAI-compatible service",
    where="cloud",
    jobs=frozenset(JOBS),
)


class Provider(OpenAIChat):
    info = INFO
