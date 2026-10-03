"""Find the connectors and build provider instances from settings.

Every module in `lindley.providers.connectors` (except `_helpers`) is a connector: it defines
INFO, a ConnectorInfo, and Provider, the class that talks to the AI.
"""

from __future__ import annotations

import functools
import importlib
import logging
import pkgutil
from dataclasses import dataclass

from lindley.config import AiSettings, ProviderConfig
from lindley.providers.base import (
    ChatProvider,
    ConnectorInfo,
    EmbeddingProvider,
    Job,
    ProviderError,
    VisionProvider,
)

log = logging.getLogger(__name__)

CONNECTORS_PACKAGE = "lindley.providers.connectors"

Provider = ChatProvider | VisionProvider | EmbeddingProvider


@dataclass(frozen=True)
class Connector:
    info: ConnectorInfo
    provider: type


def discover(package: str = CONNECTORS_PACKAGE) -> dict[str, Connector]:
    """Every connector in a package, by id. One that fails to load is logged and skipped."""
    found: dict[str, Connector] = {}
    for m in pkgutil.iter_modules(importlib.import_module(package).__path__):
        if m.name.startswith("_"):
            continue
        try:
            module = importlib.import_module(f"{package}.{m.name}")
            info, cls = module.INFO, module.Provider
        except Exception:  # noqa: BLE001 - one broken connector mustn't stop Lindley starting
            log.exception("The AI connector %r could not be loaded and was skipped", m.name)
            continue
        if info.id in found:
            raise ProviderError(f"Two AI connectors use the id {info.id!r}")
        found[info.id] = Connector(info, cls)
    return found


@functools.cache
def connectors() -> dict[str, Connector]:
    return discover()


def build_provider(config: ProviderConfig, job: Job = "chat", model: str | None = None) -> Provider:
    """A provider for one job: its model is `model`, else the connection's (except for
    embeddings, which need a model of their own), else the connector's default for that job."""
    connector = connectors().get(config.type)
    if connector is None:
        raise ProviderError(f"Unknown provider type: {config.type}")
    info = connector.info
    if job not in info.jobs:
        raise ProviderError(f"{info.company or info.label} can't do this job ({job})")
    model = model or (config.model if job != "embed" else None) or info.default_models.get(job)
    return connector.provider(config=config, model=model, api_key=config.api_key())


def get_provider(ai: AiSettings, job: Job) -> Provider:
    """The provider settings give a job, e.g. get_provider(s.ai, "vision")."""
    name = ai.connection_for(job)
    if not name:
        raise ProviderError(f"No AI is set up for this job ({job})")
    if name not in ai.providers:
        raise ProviderError(f"Provider '{name}' is not configured")
    return build_provider(ai.providers[name], job, ai.jobs[job].model)
