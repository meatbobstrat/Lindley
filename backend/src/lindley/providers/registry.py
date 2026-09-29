"""Build provider instances from settings."""

from __future__ import annotations

from lindley.config import AiSettings, ProviderConfig
from lindley.providers.anthropic import AnthropicProvider
from lindley.providers.base import ProviderError
from lindley.providers.fake import FakeProvider
from lindley.providers.openai_compat import OpenAICompatProvider

Provider = AnthropicProvider | OpenAICompatProvider | FakeProvider


def build_provider(config: ProviderConfig) -> Provider:
    match config.type:
        case "anthropic":
            return AnthropicProvider(model=config.model, api_key=config.api_key())
        case "openai_compat":
            return OpenAICompatProvider(
                base_url=config.base_url, model=config.model, api_key=config.api_key()
            )
        case "fake":
            return FakeProvider(model=config.model)
    raise ProviderError(f"Unknown provider type: {config.type}")


def get_provider(ai: AiSettings, name: str | None) -> Provider:
    """Look up a named provider from settings, e.g. get_provider(s.ai, s.ai.chat_provider)."""
    if not name:
        raise ProviderError("No provider selected")
    if name not in ai.providers:
        raise ProviderError(f"Provider '{name}' is not configured")
    return build_provider(ai.providers[name])
