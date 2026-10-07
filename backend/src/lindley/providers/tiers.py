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

`suggest` picks the tier for this computer (lindley.localai.computer) and says why;
`hundred_pages` says how long 100 pages would take on it, from what was measured on one computer
(the 2019 desktop below). Benches on more computers will make both better.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from lindley.localai.catalog import MODELS, engine
from lindley.localai.computer import GB, Computer
from lindley.providers.base import Job


@dataclass(frozen=True)
class Tier:
    id: str  # ai.tier in settings.json
    label: str
    needs: str  # what the computer needs, in words
    does: str  # what it does on this computer, and what's left for the help
    # The model of Lindley's own AI (lindley.localai.catalog) for each job done on this computer
    local: Mapping[Job, str] = field(default_factory=dict)
    memory_gb: float = 0  # the memory it needs, or
    card_gb: float | None = None  # a graphics card with this much memory of its own

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
        memory_gb=8,
        card_gb=4,  # Gemma 4 E2B takes 3 GB
    ),
    Tier(
        id="high",
        label="High",
        needs="16 GB of memory, or a graphics card.",
        does="Gemma 4 on this computer reads handwriting, sorts pages and answers your questions, "
        "at a minute or two a hard page without a graphics card. Nothing needs to leave it.",
        local={"vision": READS, "assemble": READS, "chat": READS, "continues": READS},
        memory_gb=16,
        card_gb=6,  # Gemma 4 E4B took 4.7 GB on the 8 GB card
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


# ------------------------------------------------------------------ for this computer

MEASURED_ON = "a 2019 desktop (Intel Core i5-9400, and an NVIDIA RTX 2080 SUPER with 8 GB)"
# A computer says a little less than its memory (15.8 GB for 16): this much of it is enough
CLOSE_ENOUGH = 0.9
SPARE = 1 * GB  # room left on the disk after a tier's download


def _gb(n: int) -> str:
    return f"{round(n / GB)} GB"


def on_card(t: Tier, c: Computer) -> bool:
    """Whether the tier's models run on this computer's graphics card."""
    card = c.card
    return (
        t.card_gb is not None and card is not None and card.memory >= t.card_gb * GB * CLOSE_ENOUGH
    )


def _fits(t: Tier, c: Computer) -> bool:
    return (c.memory is not None and c.memory >= t.memory_gb * GB * CLOSE_ENOUGH) or on_card(t, c)


def download_size(t: Tier, root: Path) -> int:
    """Bytes still to download for the tier: its models, and the engine if it's missing."""
    need = sum(MODELS[m].size for m in t.downloads() if not MODELS[m].installed(root))
    e = engine()
    if need and e is not None and not e.installed(root):
        need += e.file.size
    return need


def suggest(c: Computer, free: int | None, root: Path) -> tuple[str, str]:
    """The tier for this computer, and why, in a sentence or two."""
    low = TIERS[0]
    if engine() is None:
        return low.id, (
            "Lindley’s own AI doesn’t run on this kind of computer yet, so Low: the help you "
            "choose does the AI’s work."
        )
    if c.memory is None and c.card is None:
        return low.id, "Lindley couldn’t tell how much memory this computer has, so Low."
    fits = [t for t in TIERS if _fits(t, c)]
    best = fits[-1]
    has = f"{_gb(c.memory)} of memory" if c.memory is not None else "a graphics card"
    if (card := c.card) is not None and best.card_gb is not None:
        has += f" and a graphics card with {_gb(card.memory)} of its own ({card.name})"
    why = f"This computer has {has}, so it can run {best.label}"
    if best is TIERS[-1]:
        why += (
            ", on its graphics card."
            if on_card(best, c)
            else ", on its processor: handwriting takes a few minutes a page."
        )
    else:
        better = TIERS[TIERS.index(best) + 1]
        why += f". {better.label} needs {better.needs[0].lower()}{better.needs[1:]}"
    while best is not low and free is not None and download_size(best, root) + SPARE > free:
        lower = TIERS[TIERS.index(best) - 1]
        why += (
            f" There isn’t room on the disk for {best.label}’s download"
            f" ({download_size(best, root) / 1e9:.1f} GB), so {lower.label}."
        )
        best = lower
    return best.id, why


# How long things took on the 2019 desktop (MEASURED_ON; design/database.md, "Lindley's own AI,
# measured"), in seconds. On the processor, and on the graphics card (None: not measured there)
READ_S = 4.6  # Tesseract (3.5) and the image checks (1.1), a page: intake_steps, the dev library
HARD = 48 / 344  # typed pages Tesseract reads below 70%, sent to the reading AI (the Claude test)
CONTINUES_A_PAGE = 305 / 147  # questions whether pages run on, 305 for the 23 documents' pages
# Sorting questions: 47 for them with Gemma 4 E4B in the timed run (30 in an earlier one)
SORTS_A_PAGE = 47 / 147
# A text question on the card where it wasn't measured there: Qwen3.5 4B took 0.36 s a pair on
# the card, 4.7 on the processor
CARD_QUICKER = 13


@dataclass(frozen=True)
class Speed:
    load: tuple[float, float | None]  # loading the model, once
    continues: tuple[float, float | None]  # a question whether pages run on
    read: tuple[float, float | None] = (0, None)  # reading a hard page
    sort: tuple[float, float | None] = (0, None)  # a sorting question


SPEEDS = {
    "gemma-4-e2b": Speed(load=(10, None), continues=(1.75, None)),
    # Sorting: 116 s a question on the processor (3 timed, 56-178), 2.8 on the card (47 timed)
    "gemma-4-e4b": Speed(load=(54, 9), continues=(3.2, 0.15), read=(165, 6), sort=(116, 2.8)),
}


def _on(pair: tuple[float, float | None], card: bool) -> float:
    cpu, gpu = pair
    return (gpu if gpu is not None else cpu / CARD_QUICKER) if card else cpu


def hundred_pages(t: Tier, card: bool, workers: int) -> dict[str, float | None]:
    """Seconds 100 pages would take on this computer, typed and handwritten. Tesseract reads a
    few pages at once (`workers`); the AI one question at a time. Handwritten is None when this
    computer doesn't read handwriting (it goes to the help, or waits for a person)."""
    pages = 100
    reading = pages * READ_S / max(1, workers)
    ai = sum(_on(SPEEDS[m].load, card) for m in t.downloads())
    if m := t.local.get("continues"):
        ai += pages * CONTINUES_A_PAGE * _on(SPEEDS[m].continues, card)
    if m := t.local.get("assemble"):
        ai += pages * SORTS_A_PAGE * _on(SPEEDS[m].sort, card)
    typed = reading + ai
    if not (m := t.local.get("vision")):
        return {"typed": typed, "handwritten": None}
    hard = _on(SPEEDS[m].read, card)
    return {"typed": typed + pages * HARD * hard, "handwritten": typed + pages * hard}
