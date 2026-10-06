"""Performance tiers: how much AI a computer can run, and so which AI each job uses.

A person picks one in Setup or Settings, and the UI gives each job its connection and model from
it (ai.tier records which). The choice of AI for each job stays underneath: changing one makes
the settings a person's own. Models are Ollama's names for now, on the local connection; the
measurements behind them are in design/database.md, "Local models on a CPU".
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from lindley.providers.base import Job


@dataclass(frozen=True)
class Tier:
    id: str  # ai.tier in settings.json
    label: str
    needs: str  # what the computer needs, in words
    does: str  # what the AI does, and how fast
    # Where its AI runs: nowhere, this computer (the local connection), a computer on your
    # network (the local connection at its address), or a cloud company (a key).
    runs: Literal["none", "this", "network", "cloud"]
    # For a tier on this computer: the model each job uses. Jobs left out get no AI. Elsewhere,
    # each job its connection can do uses the connection's model.
    models: Mapping[Job, str] = field(default_factory=dict)


# EmbeddingGemma found a page's own document 125 times in 147, nomic-embed-text 114
EMBED = "embeddinggemma"

TIERS: tuple[Tier, ...] = (
    Tier(
        id="basic",
        label="Basic",
        needs="Any computer.",
        does="Tesseract reads printed and typed pages, and Lindley's rules sort them into "
        "documents. No AI: handwriting and pages Tesseract can't read wait for you.",
        runs="none",
    ),
    Tier(
        id="light",
        label="Light",
        needs="8 GB of memory.",
        # E4B, not E2B: on the bench E2B left words and half a page out (design/database.md,
        # "Local models"). The smaller computer only makes it slower
        does="Gemma 4 E4B on this computer reads handwriting, slowly: a few minutes a page. "
        "EmbeddingGemma finds pages about the same thing. Sorting and questions use Lindley's "
        "rules.",
        runs="this",
        models={"vision": "gemma4:e4b", "embed": EMBED},
    ),
    Tier(
        id="full",
        label="Full local",
        needs="16 GB of memory, and a processor from the last few years or built-in graphics.",
        does="Gemma 4 E4B on this computer reads handwriting, sorts pages and answers your "
        "questions, at a minute or two a hard page.",
        runs="this",
        models={
            "vision": "gemma4:e4b",
            "assemble": "gemma4:e4b",
            "chat": "gemma4:e4b",
            "embed": EMBED,
        },
    ),
    Tier(
        id="power",
        label="Power",
        needs="32 GB of memory, or a graphics card with 8 GB or more.",
        # Gemma 4 26B-A4B uses 3.8B of its 25B weights for each token, so it runs on a
        # processor with the memory; 12B suits an 8 GB card, 31B one with 24 GB or more
        does="Gemma 4 26B on this computer: quicker and more accurate at every job. With a "
        "graphics card, Gemma 4 12B (8 GB) or 31B (24 GB) can be set for each job in Settings.",
        runs="this",
        models={
            "vision": "gemma4:26b",
            "assemble": "gemma4:26b",
            "chat": "gemma4:26b",
            "embed": EMBED,
        },
    ),
    Tier(
        id="server",
        label="Your own AI server",
        needs="A computer on your network running Ollama or LM Studio.",
        does="That computer does every job, with its power. Your scans stay on your network.",
        runs="network",
    ),
    Tier(
        id="cloud",
        label="Cloud",
        needs="Any computer, and an API key from an AI company.",
        does="A cloud AI reads, sorts and answers well and quickly. Pages leave this computer, "
        "and each one costs money.",
        runs="cloud",
    ),
)

TIER_IDS = frozenset(t.id for t in TIERS)
