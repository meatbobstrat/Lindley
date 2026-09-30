"""Cut a stream of scanned pages into documents, put each in order, and say how sure we are.

This is "page stream segmentation": pages usually arrive in the order they were scanned, and
documents are usually scanned one after another, so the job is mostly deciding where one
document ends and the next begins.
"""

from __future__ import annotations

from collections import Counter

from lindley.assembler.clues import MONTH_NAMES
from lindley.assembler.evidence import Pair, adjacent, pair
from lindley.assembler.model import Group, Page

CUT_BELOW = 0.5  # neighbouring pages scoring below this are split into different documents


def scan_order(pages: list[Page]) -> list[Page]:
    return sorted(pages, key=Page.scan_key)


def pair_scores(pages: list[Page]) -> list[Pair]:
    """Scores between each page and the next, in scan order."""
    return [pair(a, b) for a, b in zip(pages, pages[1:], strict=False)]


def split(pages: list[Page], pairs: list[Pair]) -> list[tuple[int, int]]:
    """(start, end) index ranges of each run of pages that belong together."""
    runs, start = [], 0
    for i, p in enumerate(pairs):
        if p.score < CUT_BELOW:
            runs.append((start, i + 1))
            start = i + 1
    if pages:
        runs.append((start, len(pages)))
    return runs


def order(pages: list[Page]) -> tuple[list[Page], bool]:
    """Reading order within a group, and whether the order is settled by clear evidence."""
    if len(pages) == 1:
        return pages, True
    keys, last = [], 0.0
    for p in pages:
        c = p.clues
        if c.marker:
            k = float(c.marker[0])
        elif c.starts_doc:
            k = 0.5
        elif c.ends_doc:
            k = 10_000.0
        else:
            k = last + 0.01
        keys.append(k)
        if k < 10_000:
            last = k
    ordered = [p for _, _, p in sorted(zip(keys, range(len(pages)), pages, strict=True))]
    # Settled when at most one page's place is a guess, or every step is backed by evidence.
    pinned = sum(1 for p in pages if p.clues.marker or p.clues.starts_doc or p.clues.ends_doc)
    chained = all(
        pair(a, b, True).links and any(k.relation == "continues" for k in pair(a, b, True).links)
        for a, b in zip(ordered, ordered[1:], strict=False)
    )
    return ordered, len(pages) - pinned <= 1 or chained


def _month(iso: str) -> str:
    parts = iso.split("-")
    return f"{MONTH_NAMES[int(parts[1])]} {parts[0]}" if len(parts) > 1 else parts[0]


def describe(pages: list[Page]) -> tuple[str | None, str, bool, str | None]:
    """(kind, name, name is a placeholder, date) for a group, from its clues."""
    kinds = Counter(p.clues.kind for p in pages if p.clues.kind not in (None, "blank", "notes"))
    kind = kinds.most_common(1)[0][0] if kinds else None
    dates = [d for p in pages for d in p.clues.dates]
    best = max(dates, key=lambda d: (d[2], len(d[1])), default=None)
    date = best[1] if best else None
    when = f", {_month(date)}" if date else ""
    first, last = pages[0].clues, pages[-1].clues
    if kind == "letter":
        if last.signature:
            return kind, f"Letter from {last.signature}{when}", False, date
        names = sorted(first.people)
        if first.salutation and names:
            return kind, f"Letter to {' '.join(names)}{when}", False, date
        return kind, f"Letter{when}", True, date
    if kind == "receipt":
        who = first.letterhead.title() if first.letterhead else ""
        return kind, f"Receipt{', ' + who if who else ''}{when}", not who, date
    if kind == "deed":
        return kind, f"Deed{when}", False, date
    if kind == "diary":
        return kind, f"Diary{when or ' pages'}", False, date
    words = " ".join(first.first_line.split()[:5])
    return kind, f"Pages starting “{words}…”", True, date


def _confidence(
    pages: list[Page], inside: list[Pair], left: Pair | None, right: Pair | None
) -> int:
    within = min((p.score for p in inside), default=1.0)
    edge = 1 - max(left.score if left else 0.0, right.score if right else 0.0)
    conf = min(within, edge)
    first, last = pages[0].clues, pages[-1].clues
    has_start, has_end = bool(first.starts_doc), bool(last.ends_doc)
    # A group bracketed by a clear start and a clear end, with no break inside, is one document.
    if has_start and has_end and within >= CUT_BELOW and len(pages) <= 8:
        conf = max(conf, min(0.85, edge + 0.3))
    # Diaries have no greeting or signature. For anything else, a missing start or end
    # suggests a page is missing.
    if not all(p.clues.kind == "diary" for p in pages):
        conf *= (1.0 if has_start else 0.8) * (1.0 if has_end else 0.85)
    if not any(p.clues.kind for p in pages) and not (has_start or has_end):
        conf *= 0.6
    return round(100 * max(0.0, min(conf, 0.97)))


def _reasons(pages: list[Page], inside: list[Pair]) -> list[str]:
    out = []
    first, last = pages[0].clues, pages[-1].clues
    markers = [p.clues.marker[0] for p in pages if p.clues.marker]
    if len(markers) >= 2 and markers == sorted(markers):
        out.append(f"Page numbers {markers[0]}–{markers[-1]} run in order")
    if first.starts_doc:
        out.append(f"The first page {first.starts_doc}")
    if last.ends_doc and len(pages) > 1:
        out.append(f"The last page {last.ends_doc}")
    elif last.ends_doc:
        out.append(f"It {last.ends_doc}")
    for p in inside:
        for link in p.links:
            if (
                link.relation in ("continues", "same_letterhead", "shared_names")
                and link.note not in out
            ):
                out.append(link.note)
    if (
        len(out) < 2
        and len(pages) > 1
        and all(any(k.relation == "adjacent_file" for k in p.links) for p in inside)
    ):
        out.append("They were scanned one after another with nothing to suggest a break")
    return out[:5]


def make_group(
    pages: list[Page], inside: list[Pair], left: Pair | None, right: Pair | None
) -> Group:
    ordered, settled = order(pages)
    kind, name, guess, date = describe(ordered)
    g = Group(
        ordered,
        _confidence(ordered, inside, left, right),
        _reasons(ordered, inside),
        kind,
        name,
        guess,
        date,
        settled,
    )
    if len(pages) == 1 and pages[0].clues.kind in ("blank", "notes"):
        g.set_aside = True
        g.reasons = [
            "It looks blank"
            if pages[0].clues.kind == "blank"
            else "It's a short note that doesn't belong to the pages around it"
        ]
    return g


def segment(pages: list[Page]) -> tuple[list[Group], list[Pair], list[Page]]:
    """Groups of pages in scan order, with the neighbour scores and the scan order used."""
    ordered = scan_order(pages)
    pairs = pair_scores(ordered)
    groups = []
    for s, e in split(ordered, pairs):
        groups.append(
            make_group(
                ordered[s:e],
                pairs[s : e - 1],
                pairs[s - 1] if s > 0 else None,
                pairs[e - 1] if e - 1 < len(pairs) else None,
            )
        )
    return stitch(groups), pairs, ordered


def stitch(groups: list[Group]) -> list[Group]:
    """Join a group missing its end to a later group missing its start, when one clearly continues
    the other. This catches pages of one document that weren't scanned next to each other."""
    out = list(groups)
    changed = True
    while changed:
        changed = False
        for i, g in enumerate(out):
            if g.set_aside or g.pages[-1].clues.ends_doc:
                continue
            for j, h in enumerate(out):
                if j == i or h.set_aside or h.pages[0].clues.starts_doc:
                    continue
                a, b = g.pages[-1], h.pages[0]
                # b scanned just before a is the usual sign of two pages fed through swapped.
                swapped = adjacent(b, a)
                link = pair(a, b, is_adjacent=swapped or adjacent(a, b))
                if link.score >= (0.6 if swapped else 0.75):
                    inside = [
                        pair(a, b, True)
                        for a, b in zip(g.pages + h.pages, (g.pages + h.pages)[1:], strict=False)
                    ]
                    merged = make_group(g.pages + h.pages, inside, None, None)
                    merged.reasons.append("Two parts scanned apart were joined")
                    out = [x for k, x in enumerate(out) if k not in (i, j)] + [merged]
                    changed = True
                    break
            if changed:
                break
    return out
