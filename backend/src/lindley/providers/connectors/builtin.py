"""Lindley's own AI: llama.cpp's llama-server, which Lindley runs itself with the models it
downloaded (lindley.localai). Private: nothing leaves this computer.

It speaks the OpenAI Chat Completions API, through OpenAI's library as llama.cpp documents. The
server's address is only known once it's running, so it's started when the first call needs it.
"""

from __future__ import annotations

import httpx2
import openai

from lindley.localai import server as local_server
from lindley.localai.catalog import MODELS, engine
from lindley.providers.base import JOBS, ConnectorInfo, ProviderError
from lindley.providers.connectors._openai_chat import OpenAIChat

INFO = ConnectorInfo(
    id="builtin",
    label="Lindley's own AI, on this computer",
    short="Lindley's own AI",
    where="local",
    jobs=frozenset(JOBS),
    # Ids in lindley.localai.catalog
    default_models={
        "vision": "gemma-4-e4b",
        "assemble": "gemma-4-e4b",
        "chat": "gemma-4-e4b",
        "embed": "embeddinggemma",
        "continues": "qwen3.5-4b",
    },
    # A laptop with no graphics card may take minutes over a page, and loading a model the
    # first time it's asked for takes a while too
    timeout_s=600,
)


def _fresh_connections() -> httpx2.Client:
    """A connection for each call. llama-server closes one it has kept open after a while, and a
    call sent on it just then is lost ("Server disconnected without sending a response"): on
    the bench, about one call in 400. On this computer a new connection costs nothing."""
    return httpx2.Client(limits=httpx2.Limits(max_keepalive_connections=0))


class Provider(OpenAIChat):
    info = INFO
    # Reading a page, or answering a sorting question in JSON, needs no thinking (a thinking
    # model can spend its whole context thinking). llama.cpp's switch is the chat template's
    # enable_thinking; it has no per-request reasoning_effort.
    quick_options = {"extra_body": {"chat_template_kwargs": {"enable_thinking": False}}}
    # A typed page is about 600 tokens; past this, a model is repeating itself
    read_most = 2048

    def __init__(self, config=None, model=None, api_key=None, http_client=None) -> None:
        super().__init__(config, model, api_key, http_client)
        self.who = "Lindley's own AI"
        self._http = http_client
        self._timeout = (config.timeout_s if config else None) or INFO.timeout_s

    @property
    def client(self) -> openai.OpenAI:
        if self._client is None:
            self.base_url = local_server.current().url(self.model)
            self._client = openai.OpenAI(
                api_key="none",
                base_url=self.base_url,
                timeout=self._timeout,
                max_retries=0,  # see _common: a busy AI is tried again by the throttle
                http_client=self._http or _fresh_connections(),
            )
        return self._client

    def check(self) -> str:
        """Whether this model and the engine are downloaded. The server isn't started: it
        starts when a job first needs it."""
        root = local_server.current().local.folder()
        m = MODELS.get(self.model or "")
        if m is None:
            raise ProviderError(f"Lindley's own AI has no model called {self.model!r}")
        e = engine()
        if not m.installed(root) or (e is not None and not e.installed(root)):
            raise ProviderError(f"{m.label} isn't downloaded yet. Download it in Settings › AI.")
        return f"Ready. {m.label} is downloaded, and starts when it's first needed."
