import pytest

from lindley.config import AiSettings, ProviderConfig
from lindley.providers.anthropic import AnthropicProvider
from lindley.providers.base import (
    ChatMessage,
    ChatProvider,
    EmbeddingProvider,
    ProviderError,
    VisionProvider,
)
from lindley.providers.fake import FakeProvider
from lindley.providers.openai_compat import OpenAICompatProvider
from lindley.providers.registry import build_provider, get_provider


def test_registry_builds_each_type():
    assert isinstance(build_provider(ProviderConfig(type="fake")), FakeProvider)
    assert isinstance(build_provider(ProviderConfig(type="anthropic")), AnthropicProvider)
    local = build_provider(
        ProviderConfig(type="openai_compat", base_url="http://localhost:11434/v1", model="x")
    )
    assert isinstance(local, OpenAICompatProvider)
    assert local.base_url == "http://localhost:11434/v1"


def test_fake_provider_satisfies_all_interfaces():
    p = FakeProvider()
    assert isinstance(p, ChatProvider)
    assert isinstance(p, VisionProvider)
    assert isinstance(p, EmbeddingProvider)
    assert p.chat([ChatMessage("user", "hi")]) == "[fake] hi"
    assert p.embed(["a", "b"])[0] != p.embed(["a", "b"])[1]


def test_get_provider_errors():
    ai = AiSettings(providers={"f": ProviderConfig(type="fake")})
    assert isinstance(get_provider(ai, "f"), FakeProvider)
    with pytest.raises(ProviderError):
        get_provider(ai, "missing")
    with pytest.raises(ProviderError):
        get_provider(ai, None)
