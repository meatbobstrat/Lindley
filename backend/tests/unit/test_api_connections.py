"""The API for AI connectors and connections: what can be connected, keys, setup, checks."""

import pytest

from lindley.providers.base import ProviderError
from lindley.providers.connectors import fake
from lindley.providers.keys import SERVICE


def test_connectors_offered(client):
    found = {c["id"]: c for c in client.get("/api/connectors").json()}
    assert {"local", "anthropic", "openai", "google", "openai_compat"} == set(found)
    assert found["anthropic"]["jobs"] == ["vision", "assemble", "chat"]
    assert found["anthropic"]["needs_key"] and found["anthropic"]["where"] == "cloud"
    assert found["local"]["where"] == "local" and not found["local"]["needs_key"]
    assert found["local"]["default_models"]["embed"] == "embeddinggemma"
    assert found["openai_compat"]["needs_key"] and found["openai_compat"]["where"] == "cloud"
    assert [found[c]["short"] for c in ("local", "anthropic", "google", "openai_compat")] == [
        "AI on this computer",
        "Claude",
        "Gemini",
        "Cloud AI",
    ]


def test_setup_is_needed_until_settings_are_saved(client):
    assert client.get("/api/setup").json() == {"needed": True}
    assert client.put("/api/settings", json=client.get("/api/settings").json()).status_code == 200
    assert client.get("/api/setup").json() == {"needed": False}


def test_keys_go_in_the_credential_store_and_never_come_back(client, keys):
    r = client.put("/api/connections/local/key", json={"key": "  sk-secret-abcd "})
    assert r.json() == {"key_hint": "abcd"}
    assert keys.keys[(SERVICE, "local")] == "sk-secret-abcd"
    assert "sk-secret" not in client.get("/api/settings").text
    assert "sk-secret" not in client.get("/api/settings/ai-calls").text
    assert client.get("/api/settings/ai-calls").json()["providers"]["local"]["key_hint"] == "abcd"
    assert client.delete("/api/connections/local/key").json() == {"key_hint": None}
    assert (SERVICE, "local") not in keys.keys
    assert client.put("/api/connections/nobody/key", json={"key": "x"}).status_code == 404
    assert client.put("/api/connections/local/key", json={"key": ""}).status_code == 422


def test_a_connection_removed_takes_its_key_with_it(client, keys):
    client.put("/api/connections/local/key", json={"key": "sk-1234"})
    current = client.get("/api/settings").json()
    current["ai"]["providers"] = {}
    current["ai"]["jobs"] = {}
    assert client.put("/api/settings", json=current).status_code == 200
    assert keys.keys == {}


def test_settings_that_cant_work_are_refused(client):
    current = client.get("/api/settings").json()
    current["ai"]["providers"]["claude"] = {"type": "anthropic"}
    current["ai"]["providers"]["odd"] = {"type": "nobody"}
    current["ai"]["jobs"]["embed"] = {"connection": "claude"}
    current["ai"]["jobs"]["chat"] = {"connection": "gone"}
    r = client.put("/api/settings", json=current)
    assert r.status_code == 422
    problems = " ".join(r.json()["detail"])
    assert "'odd' has an unknown type" in problems
    assert "Anthropic can't do the job 'embed'" in problems
    assert "'gone', which isn't set up" in problems


def test_try_a_connection(client, monkeypatch):
    draft = {"connection": {"type": "fake", "model": "m"}}
    assert client.post("/api/connections/test", json=draft).json() == {
        "ok": True,
        "message": "Connected. m answered.",
    }

    def refused(self):
        raise ProviderError("The AI at localhost refused the key")

    monkeypatch.setattr(fake.FakeProvider, "check", refused)
    assert client.post("/api/connections/test", json=draft).json() == {
        "ok": False,
        "message": "The AI at localhost refused the key",
    }
    no_key = {"connection": {"type": "google"}}
    r = client.post("/api/connections/test", json=no_key).json()
    assert r == {"ok": False, "message": "Paste your API key from Google."}
    unknown = {"connection": {"type": "nobody"}}
    assert client.post("/api/connections/test", json=unknown).status_code == 422


@pytest.mark.parametrize(("job", "model"), [("embed", "fake"), ("chat", "m")])
def test_trying_a_connection_looks_for_the_jobs_model(client, monkeypatch, job, model):
    seen = []
    monkeypatch.setattr(fake.FakeProvider, "check", lambda self: seen.append(self.model) or "ok")
    client.post(
        "/api/connections/test", json={"connection": {"type": "fake", "model": "m"}, "job": job}
    )
    assert seen == [model]


def test_a_saved_key_only_goes_where_it_was_saved_for(client, keys):
    current = client.get("/api/settings").json()
    current["ai"]["providers"]["claude"] = {"type": "anthropic"}
    assert client.put("/api/settings", json=current).status_code == 200
    client.put("/api/connections/claude/key", json={"key": "sk-ant-secret"})
    elsewhere = {"type": "anthropic", "base_url": "https://evil.example"}
    r = client.post("/api/connections/test", json={"connection": elsewhere, "name": "claude"})
    assert r.json() == {"ok": False, "message": "Paste your API key from Anthropic."}
