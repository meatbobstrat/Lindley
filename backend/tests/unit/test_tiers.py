"""Performance tiers: what each one asks of the computer, and the AI each job uses."""

from lindley.providers.registry import connectors
from lindley.providers.tiers import TIERS


def test_every_tier_can_be_set_up():
    local = connectors()["local"].info
    assert [t.id for t in TIERS] == ["basic", "light", "full", "power", "server", "cloud"]
    for t in TIERS:
        assert t.label and t.needs and t.does
        # Only a tier on this computer names its models, each for a job the local AI can do
        assert bool(t.models) == (t.runs == "this")
        assert set(t.models) <= local.jobs


def test_the_tiers_grow_with_the_computer():
    light, full, power = (next(t for t in TIERS if t.id == i) for i in ("light", "full", "power"))
    assert set(light.models) == {"vision", "embed"}  # no sorting or questions on 8 GB
    assert set(full.models) == set(power.models) == {"vision", "assemble", "chat", "embed"}
    assert full.models["chat"] == connectors()["local"].info.default_models["chat"]
    assert light.models["embed"] == full.models["embed"] == "embeddinggemma"
    assert light.models["vision"] == full.models["vision"]  # E2B left half a page out


def test_tiers_are_offered(client):
    found = {t["id"]: t for t in client.get("/api/tiers").json()}
    assert found["full"]["runs"] == "this"
    assert found["full"]["models"]["vision"] == "gemma4:e4b"
    assert found["cloud"] == {
        "id": "cloud",
        "label": "Cloud",
        "needs": found["cloud"]["needs"],
        "does": found["cloud"]["does"],
        "runs": "cloud",
        "models": {},
    }


def test_the_tier_chosen_is_saved(client):
    current = client.get("/api/settings").json()
    assert current["ai"]["tier"] is None
    current["ai"]["tier"] = "light"
    assert client.put("/api/settings", json=current).json()["ai"]["tier"] == "light"
    current["ai"]["tier"] = "turbo"
    r = client.put("/api/settings", json=current)
    assert r.status_code == 422
    assert "no performance tier called 'turbo'" in r.text
