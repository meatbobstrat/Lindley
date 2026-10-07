"""The two choices: how much AI this computer runs (a tier), and who does the rest (the help)."""

from lindley.config import Settings
from lindley.localai.catalog import MODELS
from lindley.providers.registry import connectors
from lindley.providers.tiers import HELPS, TIERS


def tier(id: str):
    return next(t for t in TIERS if t.id == id)


def test_every_tier_runs_on_lindleys_own_ai():
    own = connectors()["builtin"].info
    assert [t.id for t in TIERS] == ["low", "middle", "high"]
    for t in TIERS:
        assert t.label and t.needs and t.does
        assert set(t.local) <= own.jobs
        for job, model in t.local.items():
            assert job in MODELS[model].jobs  # each model can do its job
    assert [h.id for h in HELPS] == ["none", "server", "cloud"]


def test_the_tiers_grow_with_the_computer():
    low, middle, high = tier("low"), tier("middle"), tier("high")
    assert not low.local and low.downloads() == []
    assert set(middle.local) == {"continues"}  # small: the help does the rest
    assert set(high.local) == {"vision", "assemble", "chat", "continues"}
    assert set(middle.local) < set(high.local)
    # Nothing downloads EmbeddingGemma until something uses the embed job
    assert all("embed" not in t.local for t in TIERS)
    assert high.downloads() == list(dict.fromkeys(high.local.values()))


def test_tiers_and_helps_are_offered(client):
    found = client.get("/api/tiers").json()
    tiers = {t["id"]: t for t in found["tiers"]}
    assert tiers["high"]["local"]["vision"] == "gemma-4-e4b"
    assert tiers["middle"]["downloads"] == [tier("middle").local["continues"]]
    assert [h["id"] for h in found["helps"]] == ["none", "server", "cloud"]
    assert all(h["label"] and h["does"] for h in found["helps"])


def test_the_choices_are_saved(client):
    current = client.get("/api/settings").json()
    assert current["ai"]["tier"] is None and current["ai"]["help"] is None
    current["ai"]["tier"] = "middle"
    current["ai"]["help"] = "local"
    saved = client.put("/api/settings", json=current).json()["ai"]
    assert (saved["tier"], saved["help"]) == ("middle", "local")
    current["ai"]["tier"] = "turbo"
    current["ai"]["help"] = "nowhere"
    r = client.put("/api/settings", json=current)
    assert r.status_code == 422
    assert "no performance tier called 'turbo'" in r.text
    assert "'nowhere', isn't an AI connection" in r.text


def test_earlier_tiers_are_carried_over():
    def ai(tier: str, jobs: dict | None = None):
        return Settings.model_validate(
            {"ai": {"providers": {"srv": {"type": "local"}}, "jobs": jobs or {}, "tier": tier}}
        ).ai

    assert (ai("basic").tier, ai("basic").help) == ("low", None)
    # A server or a cloud AI that did every job: Low, with it as the help
    server = ai("server", {"vision": {"connection": "srv"}, "chat": {"connection": "srv"}})
    assert (server.tier, server.help) == ("low", "srv")
    # Tiers that ran on Ollama: their jobs are kept, as a person's own choice
    full = ai("full", {"vision": {"connection": "srv", "model": "gemma4:e4b"}})
    assert full.tier is None and full.jobs["vision"].model == "gemma4:e4b"
    assert ai("high").tier == "high"  # already new
