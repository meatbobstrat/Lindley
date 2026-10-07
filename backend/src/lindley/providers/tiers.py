"""How much AI this computer runs, and who does the rest: the two choices Setup and Settings ask.

A tier says which jobs Lindley's own AI does on this computer, and with which model. Help is a
connection that does the jobs the tier leaves: your own AI server, or a cloud AI with a key, or
nobody. Every job done on this computer is one that isn't sent anywhere or paid for, so the cost
of each page falls, and privacy rises, from Low to High.

The UI gives each job its AI from the two (frontend lib/tiers.ts, withTier): the tier's model on
Lindley's own AI, else the help if it can do the job, else nothing. ai.tier and ai.help record
the choices; the choice of AI for each job stays underneath, and changing one makes the settings
a person's own (ai.tier is then None). The measurements behind the models are in
design/database.md, "Local models on a CPU".
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
    does: str  # what it does on this computer, and what's left for the help
    # The model of Lindley's own AI (lindley.localai.catalog) for each job done on this computer
    local: Mapping[Job, str] = field(default_factory=dict)

    def downloads(self) -> list[str]:
        """The models the tier needs downloaded, each once."""
        return list(dict.fromkeys(self.local.values()))


@dataclass(frozen=True)
class Help:
    id: Literal["none", "server", "cloud"]
    label: str
    does: str  # where pages go, and what it costs


# Whether a page runs on: Gemma 4 E2B, 3 GB and 1.75 s a pair on a 2019 desktop processor. Qwen3.5
# 4B sees it better (0.93 against 0.86 where the rules see a sentence run on) but takes 4.9 GB
# and 4.7 s, and as evidence all three sorted the 23 documents alike (design/database.md)
CONTINUES = "gemma-4-e2b"
# Gemma 4 E4B reads handwriting nearly as Claude does (CER 0.15 against it), and sorts; on High
# it checks whether pages run on too (0.89), so one model is all High downloads
READS = "gemma-4-e4b"

TIERS: tuple[Tier, ...] = (
    Tier(
        id="low",
        label="Low",
        needs="Any computer, even an old or slow one.",
        does="No AI runs on this computer: Tesseract reads printed and typed pages, and Lindley’s "
        "rules sort them. Everything else goes to the help you choose below, or waits for you.",
    ),
    Tier(
        id="middle",
        label="Middle",
        needs="8 GB of memory.",
        does="A small AI on this computer checks whether each page carries on from the last, so "
        "sorting needs the help less. Reading handwriting, sorting what’s left and answering "
        "questions go to the help you choose below.",
        local={"continues": CONTINUES},
    ),
    Tier(
        id="high",
        label="High",
        needs="16 GB of memory, or a graphics card.",
        does="Gemma 4 on this computer reads handwriting, sorts pages and answers your questions, "
        "at a minute or two a hard page without a graphics card. Nothing needs to leave it.",
        local={"vision": READS, "assemble": READS, "chat": READS, "continues": READS},
    ),
)

HELPS: tuple[Help, ...] = (
    Help(
        id="none",
        label="Nobody",
        does="What this computer doesn’t do waits for you: handwriting waits for your review, you "
        "match the pages Lindley isn’t sure of, and Ask Lindley only finds words.",
    ),
    Help(
        id="server",
        label="Your own AI server",
        does="A computer you own, on your network, does the rest: Ollama, LM Studio or "
        "llama-server. Your scans stay with you.",
    ),
    Help(
        id="cloud",
        label="A cloud AI",
        does="An AI company does the rest, with an API key: quick and good. The pages it’s sent "
        "leave this computer, and each one costs money.",
    ),
)

TIER_IDS = frozenset(t.id for t in TIERS)
