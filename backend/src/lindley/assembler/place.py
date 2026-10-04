"""Where pages the rules couldn't settle most likely belong: a short list, best first.

A page left in the Inbox might continue a document already made, start one, or go with other
pages still in the Inbox. Rather than set it against every document, a person or the AI looks at
the few it most likely belongs to, each with how sure Lindley is and why: the page it would join
reads on from it, they share a folder, and so on (lindley.assembler.evidence).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePath

from lindley.assembler.evidence import Pair, pair
from lindley.assembler.model import Group, Page

TOP = 3  # candidates kept for each group
FLOOR = 10  # a candidate scoring below this, 0-100, isn't worth anyone's time


@dataclass
class DocEnds:
    """An open document, as far as placing pages goes: its first and last pages."""

    id: int
    name: str
    first: Page
    last: Page
    touched: bool  # a person has named, changed or reviewed it: Lindley only suggests
    page_ids: frozenset[int] = frozenset()


@dataclass
class Candidate:
    """Somewhere a group of pages may belong: an open document, or another group in the Inbox."""

    document: DocEnds | None  # one of these two
    group: Group | None
    at_end: bool  # the pages go after it; else before
    score: int  # 0-100: that they go there
    reasons: list[str]
    joins: Page  # the page they'd sit next to

    @property
    def name(self) -> str:
        return self.document.name if self.document else self.group.name

    def payload(self) -> dict:
        """As kept with a hint, for a person to read."""
        where = {"document": self.document.id} if self.document else {"pages": self.group.ids}
        return where | {
            "name": self.name,
            "at": "end" if self.at_end else "start",
            "confidence": self.score,
            "reasons": self.reasons,
        }


def _reasons(p: Pair, a: Page, b: Page) -> list[str]:
    out = [k.note for k in p.links if k.relation != "adjacent_file"] or [k.note for k in p.links]
    if p.features.get("folder_shared", 0) >= 0.5:
        out.append(f"Both are in the folder {PurePath(a.folder).name}")
    return out


def _tries(g: Group, first: Page, last: Page) -> list[tuple[bool, Page, Pair]]:
    """The ways g may join pages running from first to last: after them, or before."""
    out = []
    if not g.pages[0].clues.starts_doc and not last.clues.ends_doc:
        out.append((True, last, pair(last, g.pages[0])))
    if not g.pages[-1].clues.ends_doc and not first.clues.starts_doc:
        out.append((False, first, pair(g.pages[-1], first)))
    return out


def _best(g: Group, first: Page, last: Page) -> tuple[bool, Page, Pair] | None:
    best = None
    for at_end, joins, p in _tries(g, first, last):
        if not best or round(100 * p.score) > round(100 * best[2].score):
            best = (at_end, joins, p)
    return best


def candidates(
    g: Group,
    docs: list[DocEnds],
    others: list[Group] = (),
    top: int = TOP,
    floor: int = FLOOR,
) -> list[Candidate]:
    """Where g most likely belongs, best first: open documents, and `others` (groups still in
    the Inbox), scoring `floor` or more. A document already holding a copy of one of its pages
    is never one."""
    out = []
    mine = {p.id for p in g.pages}
    for d in docs:
        if any(p.copies & d.page_ids for p in g.pages):
            continue
        if best := _best(g, d.first, d.last):
            at_end, joins, p = best
            out.append(
                Candidate(
                    d, None, at_end, round(100 * p.score), _reasons(p, joins, g.pages[0]), joins
                )
            )
    for o in others:
        if o is g or o.set_aside or mine & set(o.ids):
            continue
        if any(p.copies & set(o.ids) for p in g.pages):
            continue
        if best := _best(g, o.pages[0], o.pages[-1]):
            at_end, joins, p = best
            out.append(
                Candidate(
                    None, o, at_end, round(100 * p.score), _reasons(p, joins, g.pages[0]), joins
                )
            )
    out.sort(key=lambda c: -c.score)  # stable: ties keep the documents' order
    return [c for c in out if c.score >= floor][:top]
