"""Any other service that speaks the OpenAI API, at an address you give it, through OpenAI's
library."""

from __future__ import annotations

from lindley.providers.base import NOT_CONTINUES, ConnectorInfo
from lindley.providers.connectors._openai_chat import OpenAIChat

INFO = ConnectorInfo(
    id="openai_compat",
    label="Another OpenAI-compatible service",
    short="Cloud AI",
    where="cloud",
    jobs=NOT_CONTINUES,
    needs_key=True,  # a service on a computer you control is a `local` connection instead
)


class Provider(OpenAIChat):
    info = INFO
