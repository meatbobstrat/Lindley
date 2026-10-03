import sys

import pytest

from lindley.config import AiSettings, ProviderConfig
from lindley.providers.base import (
    ChatMessage,
    ChatProvider,
    EmbeddingProvider,
    ProviderError,
    VisionProvider,
)
from lindley.providers.connectors.fake import FakeProvider
from lindley.providers.registry import build_provider, connectors, discover, get_provider

CONNECTOR = """
from lindley.providers.base import ConnectorInfo
INFO = ConnectorInfo(id={id!r}, label="Test", where="local", jobs=frozenset({{"chat"}}))
class Provider:
    def __init__(self, config=None, model=None, api_key=None):
        self.model = model
"""


@pytest.fixture
def package(tmp_path, monkeypatch):
    """A connectors package of our own: (folder to drop files in, its import name)."""
    name = f"connectors_{tmp_path.name}"
    (tmp_path / name).mkdir()
    (tmp_path / name / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path / name, name
    for mod in [m for m in sys.modules if m.startswith(name)]:
        del sys.modules[mod]


def test_the_built_in_connectors_are_found():
    found = connectors()
    assert {"local", "anthropic", "openai", "google", "openai_compat", "fake"} <= set(found)
    assert found["fake"].info.hidden
    assert "embed" not in found["anthropic"].info.jobs
    assert all(c.info.id == k for k, c in found.items())


def test_a_dropped_in_file_is_a_connector_and_helpers_are_not(package):
    folder, name = package
    (folder / "mine.py").write_text(CONNECTOR.format(id="mine"))
    (folder / "_shared.py").write_text("INFO = None")
    found = discover(name)
    assert set(found) == {"mine"}
    assert found["mine"].provider(model="m").model == "m"


def test_a_broken_connector_is_skipped(package, caplog):
    folder, name = package
    (folder / "good.py").write_text(CONNECTOR.format(id="good"))
    (folder / "broken.py").write_text("this is not python")
    (folder / "no_info.py").write_text("class Provider: pass")
    assert set(discover(name)) == {"good"}
    assert "broken" in caplog.text and "no_info" in caplog.text


def test_two_connectors_with_one_id_are_refused(package):
    folder, name = package
    (folder / "a.py").write_text(CONNECTOR.format(id="same"))
    (folder / "b.py").write_text(CONNECTOR.format(id="same"))
    with pytest.raises(ProviderError, match="same"):
        discover(name)


def test_registry_builds_each_type():
    assert isinstance(build_provider(ProviderConfig(type="fake")), FakeProvider)
    local = build_provider(ProviderConfig(type="local", model="x"))
    assert local.base_url == "http://localhost:11434/v1" and local.model == "x"
    assert build_provider(ProviderConfig(type="anthropic")).model == "claude-opus-5"
    with pytest.raises(ProviderError, match="Unknown"):
        build_provider(ProviderConfig(type="nobody"))


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
