"""Evidence that two pages belong to the same document, and how strong it is.

Each piece of evidence is a named feature: a page number that runs on, a sentence cut off at
the bottom of one page and finished at the top of the next, rare words both pages use. A small
logistic model weighs them into one score, the chance the two pages belong together. Its
weights are fitted to pages whose right answer is known (see lindley.assembler.learn), not set
by hand. The model only scores; the reasons a person reads come from the evidence itself.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime

from lindley.assembler.model import Page
from lindley.assembler.terms import overlap
from lindley.assembler.weights import WEIGHTS
from lindley.worker.image import same_picture

SOURCE = "assembler v2"
STRONG_KINDS = {"letter", "receipt", "deed", "diary"}

FEATURES = (
    "bias",
    "adjacent",  # scanned one straight after the other
    "starts",  # the second page looks like the first page of a document
    "ends",  # the first page looks like the last page of one
    "kind_clash",  # one looks like a letter, the other like a receipt
    "number_next",  # page numbers run on: 3, then 4
    "number_near",  # page numbers a little apart: 4, then 3 (swapped), or 3, then 5
    "number_far",  # page numbers far apart, or the same
    "runs_on",  # a sentence cut off at the bottom of one is finished at the top of the other
    "runs_on_apart",  # the same, for pages not scanned together: weaker, since in a typescript
    # nearly every page ends mid-sentence
    "a_ends_mid",  # only the first page stops mid-sentence
    "b_starts_mid",  # only the second page starts mid-sentence
    "letterhead",  # the same letterhead
    "names",  # names and places both mention
    "paper_same",  # the paper is the same colour
    "paper_differs",  # the paper is clearly a different colour
    "diary",  # both are diary entries
    "layout_alike",  # typed alike: same margins, line spacing and line length
    "layout_differs",  # set out differently
    "words_shared",  # rare words both pages use
    "word_split",  # a word hyphenated at the bottom of one page is finished at the top of the other
    "pause_long",  # scanned one after the other, but with a long pause between
    "size_differs",  # sheets of clearly different sizes
    "settings_differ",  # scanned at another resolution, or in colour and in grey
    "script_differs",  # one handwritten, the other typed or printed
)
PAUSE_S = 600  # a pause this long between two scans is a long one
SIZE_IN = 0.5  # sheets differing by more than this, in inches, are different sizes
_HYPHENATED = re.compile(r"[A-Za-z]{2,}[-=¬]$")  # see clues.HYPHENS
_HAND, _TYPED = {"handwritten"}, {"printed", "typed"}


@dataclass
class Link:
    relation: str  # matches page_links.relation
    score: float  # 0-1
    note: str  # readable by a person


@dataclass
class Pair:
    score: float  # 0-1: same document?
    links: list[Link] = field(default_factory=list)
    breaks: list[str] = field(default_factory=list)  # why they might not go together
    features: dict[str, float] = field(default_factory=dict)


def adjacent(a: Page, b: Page) -> bool:
    """b was scanned straight after a."""
    if a.scan_id == b.scan_id:
        return b.page_index == a.page_index + 1
    ca, cb = a.clues, b.clues
    return (
        ca.file_seq is not None
        and cb.file_seq is not None
        and ca.file_prefix == cb.file_prefix
        and cb.file_seq == ca.file_seq + 1
    )


def _snip(s: str, tail: bool) -> str:
    words = s.split()
    return ("…" + " ".join(words[-5:])) if tail else (" ".join(words[:5]) + "…")


def _paper_gap(a: str | None, b: str | None) -> int | None:
    if not (a and b and len(a) == 7 and len(b) == 7):
        return None
    return sum(abs(int(a[i : i + 2], 16) - int(b[i : i + 2], 16)) for i in (1, 3, 5))


def _pause(a: str | None, b: str | None) -> float | None:
    """Seconds from one scan to the next, if both times are known."""
    try:
        return (datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds()
    except (TypeError, ValueError):
        return None


def _and(words: list[str]) -> str:
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " and " + words[-1]


def counts(feature: str) -> bool:
    """Whether a piece of evidence counts towards pages going together, so it may be given to a
    person as a reason. Evidence weighed at nothing or against isn't."""
    return WEIGHTS.get(feature, 0.0) > 0


def score(features: dict[str, float], weights: dict[str, float] | None = None) -> float:
    w = WEIGHTS if weights is None else weights
    z = sum(w.get(k, 0.0) * v for k, v in features.items())
    return 1 / (1 + math.exp(-max(-30.0, min(30.0, z))))


def pair(a: Page, b: Page, is_adjacent: bool | None = None) -> Pair:
    """How likely it is that page b follows page a in the same document."""
    ca, cb = a.clues, b.clues
    if b.id in a.copies or a.id in b.copies:
        return Pair(0.0, breaks=["the two scans are copies of the same page"])
    adj = adjacent(a, b) if is_adjacent is None else is_adjacent
    f = dict.fromkeys(FEATURES, 0.0)
    f["bias"] = 1.0
    p = Pair(0.0, features=f)
    if adj:
        f["adjacent"] = 1.0
        p.links.append(
            Link(
                "adjacent_file",
                0.6,
                f"{a.file_name} and {b.file_name} were scanned one after the other",
            )
        )
    if "blank" in (ca.kind, cb.kind) or "notes" in (ca.kind, cb.kind):
        p.score = 0.05
        p.breaks.append("a blank page or a separate note")
        return p

    if cb.starts_doc:
        f["starts"] = 1.0
        p.breaks.append(f"the second page {cb.starts_doc}")
    if ca.ends_doc:
        f["ends"] = 1.0
        p.breaks.append(f"the first page {ca.ends_doc}")
    if ca.kind in STRONG_KINDS and cb.kind in STRONG_KINDS and ca.kind != cb.kind:
        f["kind_clash"] = 1.0
        p.breaks.append(f"one looks like a {ca.kind}, the other like a {cb.kind}")

    ma, mb = ca.marker, cb.marker
    if ma and mb and (ma[1] == mb[1] or not (ma[1] and mb[1])):
        sure = 1.0 if ca.marker_sure and cb.marker_sure else 0.6
        gap = mb[0] - ma[0]
        if gap == 1:
            f["number_next"] = sure
            p.links.append(Link("continues", 0.95, f"Page numbers run {ma[0]} → {mb[0]}"))
        elif -2 <= gap <= 3 and gap != 0:
            f["number_near"] = sure
        else:
            f["number_far"] = sure
            p.breaks.append(f"they're numbered {ma[0]} and {mb[0]}")
    elif ma and mb:
        f["number_far"] = 1.0
        p.breaks.append(f"they're numbered {ma[0]} of {ma[1]} and {mb[0]} of {mb[1]}")

    if ca.ends_mid and cb.starts_mid:
        f["runs_on" if adj else "runs_on_apart"] = 1.0
        p.links.append(
            Link(
                "continues",
                0.85,
                f"A sentence runs on from “{_snip(ca.last_line, True)}” "
                f"to “{_snip(cb.first_line, False)}”",
            )
        )
        if _HYPHENATED.search(ca.last_line):
            f["word_split"] = 1.0
            if counts("word_split"):
                p.links.append(
                    Link(
                        "continues",
                        0.9,
                        f"A word is split over the page break: "
                        f"“{ca.last_line.split()[-1]}” “{cb.first_line.split()[0]}”",
                    )
                )
    elif ca.ends_mid:
        f["a_ends_mid"] = 1.0
    elif cb.starts_mid:
        f["b_starts_mid"] = 1.0
    if adj and (pause := _pause(a.when, b.when)) is not None and pause > PAUSE_S:
        f["pause_long"] = 1.0
    sa, sb = a.size_in, b.size_in
    if sa and sb and max(abs(sa[0] - sb[0]), abs(sa[1] - sb[1])) > SIZE_IN:
        f["size_differs"] = 1.0
        p.breaks.append("the sheets are different sizes")
    if (a.dpi and b.dpi and a.dpi != b.dpi) or (
        a.color_mode and b.color_mode and a.color_mode != b.color_mode
    ):
        f["settings_differ"] = 1.0
    if {a.script, b.script} & _HAND and {a.script, b.script} & _TYPED:
        f["script_differs"] = 1.0

    if ca.letterhead and ca.letterhead == cb.letterhead:
        f["letterhead"] = 1.0
        if counts("letterhead"):
            p.links.append(Link("same_letterhead", 0.7, f"Same letterhead, “{ca.letterhead}”"))
    shared = sorted((ca.people & cb.people) | (ca.places & cb.places))
    if shared:
        f["names"] = min(len(shared), 3) / 3
    if shared and counts("names"):
        p.links.append(
            Link(
                "shared_names",
                min(0.4 + 0.1 * len(shared), 0.7),
                "Both mention " + _and(shared[:3]),
            )
        )
    gap = _paper_gap(a.paper_color, b.paper_color)
    if gap is not None and gap <= 24:
        f["paper_same"] = 1.0
        if counts("paper_same"):
            p.links.append(Link("same_paper", 0.3, "The paper is the same colour"))
    elif gap is not None and gap > 60:
        f["paper_differs"] = 1.0
    if ca.kind == cb.kind == "diary":
        f["diary"] = 1.0
        p.links.append(Link("similar_text", 0.5, "Both are diary entries"))
    if a.layout and b.layout:
        d = a.layout.differs(b.layout)
        if d < 0.5:
            f["layout_alike"] = 1.0
            if counts("layout_alike"):
                p.links.append(
                    Link("same_writer", 0.4, "Typed alike: the same margins and line spacing")
                )
        elif d > 1:
            f["layout_differs"] = min((d - 1) / 3, 1.0)
            if d > 2:
                p.breaks.append("they're set out differently on the page")
    if a.terms and b.terms:
        likeness, words = overlap(a.terms, b.terms)
        if likeness > 0:
            f["words_shared"] = min(likeness / 0.2, 1.0)
            if likeness >= 0.05 and words and counts("words_shared"):
                p.links.append(
                    Link(
                        "similar_text",
                        round(min(0.3 + likeness, 0.8), 2),
                        "Both use the words " + _and([f"“{w}”" for w in words]),
                    )
                )
    if same_picture(a.phash, b.phash):
        p.links.append(Link("duplicate", 0.95, "The two scans look identical"))
    p.score = score(f)
    return p
