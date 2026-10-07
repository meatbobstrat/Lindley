"""The two choices: how much AI this computer runs (a tier), and who does the rest (the help)."""

import pytest

from lindley.config import Settings
from lindley.localai import computer
from lindley.localai.catalog import MODELS, Engine, File
from lindley.localai.computer import GB, Computer, Graphics
from lindley.providers import tiers
from lindley.providers.registry import connectors
from lindley.providers.tiers import HELPS, TIERS, hundred_pages, on_card, suggest


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


ENGINE = Engine("b1", File("e.zip", "https://example.com/e.zip", 30_000_000, "0" * 64), "srv")


@pytest.fixture
def has_engine(monkeypatch):
    """An engine for this kind of computer, whatever the tests run on."""
    from lindley.api import local_ai

    monkeypatch.setattr(tiers, "engine", lambda: ENGINE)
    monkeypatch.setattr(local_ai, "engine", lambda: ENGINE)


def pc(memory_gb: float | None, card_gb: float | None = None) -> Computer:
    cards = (Graphics("NVIDIA GeForce RTX 3060", int(card_gb * GB)),) if card_gb else ()
    memory = int(memory_gb * GB) if memory_gb is not None else None
    return Computer("Some processor", 8, memory, cards)


@pytest.mark.parametrize(
    ("memory", "card", "suggested"),
    [
        (4, None, "low"),
        (7.8, None, "middle"),  # a computer says a little less than its 8 GB
        (8, 4, "middle"),  # a card too small for High's model
        (15.8, None, "high"),
        (8, 8, "high"),
        (None, None, "low"),
    ],
)
def test_a_tier_is_suggested_for_the_computer(has_engine, tmp_path, memory, card, suggested):
    tier_id, why = suggest(pc(memory, card), None, tmp_path)
    assert tier_id == suggested
    assert why.endswith(".")


def test_why_says_what_the_computer_has(has_engine, tmp_path):
    _, why = suggest(pc(32, 8), None, tmp_path)
    assert why == (
        "This computer has 32 GB of memory and a graphics card with 8 GB of its own (NVIDIA "
        "GeForce RTX 3060), so it can run High, on its graphics card."
    )
    _, why = suggest(pc(16), None, tmp_path)
    assert "on its processor" in why
    _, why = suggest(pc(8), None, tmp_path)
    assert why == (
        "This computer has 8 GB of memory, so it can run Middle. High needs 16 GB of memory, or "
        "a graphics card."
    )


def test_no_tier_runs_where_lindleys_own_ai_doesnt(monkeypatch, tmp_path):
    monkeypatch.setattr(tiers, "engine", lambda: None)
    tier_id, why = suggest(pc(64, 24), None, tmp_path)
    assert tier_id == "low" and "doesn’t run on this kind of computer" in why


def test_a_tier_whose_download_doesnt_fit_isnt_suggested(has_engine, tmp_path):
    high = tiers.download_size(tier("high"), tmp_path)
    middle = tiers.download_size(tier("middle"), tmp_path)
    assert high > middle > ENGINE.file.size  # the engine comes with the first model
    tier_id, why = suggest(pc(32), high, tmp_path)  # room for it, but none to spare
    assert tier_id == "middle"
    assert "There isn’t room on the disk for High’s download" in why
    tier_id, why = suggest(pc(32), middle, tmp_path)
    assert tier_id == "low" and "for Middle’s download" in why
    assert suggest(pc(32), 100 * GB, tmp_path)[0] == "high"


def test_how_long_100_pages_take(has_engine):
    low, middle, high = (hundred_pages(tier(t), False, 3) for t in ("low", "middle", "high"))
    assert low["typed"] < middle["typed"] < high["typed"]
    # Handwriting goes to the help on Low and Middle; on High it's read here, slowly
    assert low["handwritten"] is None and middle["handwritten"] is None
    assert high["handwritten"] > high["typed"]
    on_the_card = hundred_pages(tier("high"), True, 3)
    assert on_the_card["handwritten"] < high["handwritten"] / 10
    # Tesseract reads a few at once
    assert hundred_pages(tier("low"), False, 1)["typed"] == pytest.approx(3 * low["typed"])


def test_high_runs_on_a_card_big_enough():
    assert on_card(tier("high"), pc(8, 8))
    assert not on_card(tier("high"), pc(32, 4))
    assert on_card(tier("middle"), pc(8, 4))  # Middle's smaller model fits a smaller card
    assert not on_card(tier("low"), pc(8, 8))  # Low runs no AI


def test_the_app_looks_at_the_computer(client, has_engine, monkeypatch):
    monkeypatch.setattr(computer, "look", lambda: pc(32, 8))
    c = client.get("/api/local-ai/computer").json()
    assert c["processor"] == "Some processor" and c["threads"] == 8
    assert c["memory"] == 32 * GB
    assert c["graphics"] == [{"name": "NVIDIA GeForce RTX 3060", "memory": 8 * GB}]
    assert c["free"] > 0 and c["engine"] is True
    assert c["suggested"] == "high" and "graphics card" in c["why"]
    assert set(c["times"]) == {"low", "middle", "high"}
    assert c["times"]["low"]["handwritten"] is None
    assert c["measured_on"]
