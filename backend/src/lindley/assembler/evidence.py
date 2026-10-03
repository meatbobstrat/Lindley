"""Evidence that two pages belong to the same document, and how strong it is."""

from __future__ import annotations

from dataclasses import dataclass, field

from lindley.assembler.model import Page
from lindley.worker.image import same_picture

SOURCE = "assembler v1"
STRONG_KINDS = {"letter", "receipt", "deed", "diary"}


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


def _hex_close(a: str | None, b: str | None) -> bool:
    if not (a and b and len(a) == 7 and len(b) == 7):
        return False
    return sum(abs(int(a[i : i + 2], 16) - int(b[i : i + 2], 16)) for i in (1, 3, 5)) <= 24


def pair(a: Page, b: Page, is_adjacent: bool | None = None) -> Pair:
    """How likely it is that page b follows page a in the same document."""
    ca, cb = a.clues, b.clues
    adj = adjacent(a, b) if is_adjacent is None else is_adjacent
    p = Pair(0.62 if adj else 0.3)
    if adj:
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
        p.score -= 0.45
        p.breaks.append(f"the second page {cb.starts_doc}")
    if ca.ends_doc:
        p.score -= 0.4
        p.breaks.append(f"the first page {ca.ends_doc}")
    if ca.kind in STRONG_KINDS and cb.kind in STRONG_KINDS and ca.kind != cb.kind:
        p.score -= 0.35
        p.breaks.append(f"one looks like a {ca.kind}, the other like a {cb.kind}")

    ma, mb = ca.marker, cb.marker
    if ma and mb and mb[0] == ma[0] + 1 and (ma[1] == mb[1] or not (ma[1] and mb[1])):
        p.links.append(Link("continues", 0.95, f"Page numbers run {ma[0]} → {mb[0]}"))
        p.score = max(p.score, 0.95)
    if ca.ends_mid and cb.starts_mid:
        p.links.append(
            Link(
                "continues",
                0.85,
                f"A sentence runs on from “{_snip(ca.last_line, True)}” "
                f"to “{_snip(cb.first_line, False)}”",
            )
        )
        p.score += 0.3
    elif ca.ends_mid or cb.starts_mid:
        p.score += 0.08
    if ca.letterhead and ca.letterhead == cb.letterhead:
        p.links.append(Link("same_letterhead", 0.7, f"Same letterhead, “{ca.letterhead}”"))
        p.score += 0.15
    shared = sorted((ca.people & cb.people) | (ca.places & cb.places))
    if shared:
        p.links.append(
            Link(
                "shared_names",
                min(0.4 + 0.1 * len(shared), 0.7),
                "Both mention " + " and ".join(shared[:3]),
            )
        )
        p.score += min(0.08 * len(shared), 0.16)
    if _hex_close(a.paper_color, b.paper_color):
        p.links.append(Link("same_paper", 0.3, "The paper is the same colour"))
        p.score += 0.05
    if ca.kind == cb.kind == "diary":
        p.links.append(Link("similar_text", 0.5, "Both are diary entries"))
        p.score += 0.2
    if same_picture(a.phash, b.phash):
        p.links.append(Link("duplicate", 0.95, "The two scans look identical"))
    p.score = max(0.0, min(1.0, p.score))
    return p
