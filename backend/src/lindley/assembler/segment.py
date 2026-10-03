"""Cut a stream of scanned pages into documents, put each in order, and say how sure we are.

This is "page stream segmentation": pages usually arrive in the order they were scanned, and
documents are usually scanned one after another, so the job is mostly deciding where one
document ends and the next begins.

Pages are linked to the page that follows them (`link`): each page has at most one page after it
and one before, and no loop, and the chains are the documents in reading order. Neighbours in scan
order are linked first, since scan order is the strongest single hint. Then loose ends are joined
over the whole Inbox, best links first, and only where the link is clear and has no close rival.
It's a path cover, the usual cheap way to put pages scanned apart back together.
"""

from __future__ import annotations

import math
from collections import Counter

from lindley.assembler.clues import MONTH_NAMES
from lindley.assembler.evidence import STRONG_KINDS, Pair, adjacent, pair, score
from lindley.assembler.model import Group, Page, weigh_terms
from lindley.assembler.weights import GROUP_WEIGHTS

CUT_BELOW = 0.5  # neighbouring pages scoring below this are split into different documents
SURE_LINK = 0.7  # a group's order is settled when every step in it scores at least this
JOIN_AT = 0.75  # pages scanned apart are joined when one continues the other this clearly
SWAPPED_AT = 0.6  # the same for two pages fed through the scanner the wrong way round
MARGIN = 0.1  # ... and nothing else comes this close for either of them


def scan_order(pages: list[Page]) -> list[Page]:
    return sorted(pages, key=Page.scan_key)


def pair_scores(pages: list[Page]) -> list[Pair]:
    """Scores between each page and the next, in scan order."""
    return [pair(a, b) for a, b in zip(pages, pages[1:], strict=False)]


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


# What a group's confidence is fitted on (lindley.assembler.learn, scripts/fit_assembler.py
# --groups): the chance that these pages, and only these, are one document.
GROUP_FEATURES = (
    "bias",
    "weakest",  # the weakest link inside it, in log-odds (0 for a single page)
    "edge",  # the strongest link across its ends, in log-odds: how nearly it was longer
    "single",  # one page
    "start",  # its first page looks like the first page of a document
    "end",  # its last page looks like the last page of one
    "known_kind",  # a letter, receipt, deed or diary
    "numbered",  # page numbers run in order over two or more of its pages
    "length",  # how many pages, in log
)


def _logit(p: float) -> float:
    p = min(max(p, 0.01), 0.99)
    return math.log(p / (1 - p))


def group_features(
    pages: list[Page], inside: list[Pair], left: Pair | None, right: Pair | None
) -> dict[str, float]:
    edge = max(left.score if left else 0.0, right.score if right else 0.0)
    markers = [p.clues.marker[0] for p in pages if p.clues.marker]
    return {
        "bias": 1.0,
        "weakest": _logit(min(p.score for p in inside)) if inside else 0.0,
        "edge": _logit(edge),
        "single": float(len(pages) == 1),
        "start": float(bool(pages[0].clues.starts_doc)),
        "end": float(bool(pages[-1].clues.ends_doc)),
        "known_kind": float(any(p.clues.kind in STRONG_KINDS for p in pages)),
        "numbered": float(len(markers) >= 2 and markers == sorted(markers)),
        "length": math.log(len(pages)),
    }


def _confidence(
    pages: list[Page], inside: list[Pair], left: Pair | None, right: Pair | None
) -> int:
    if GROUP_WEIGHTS:
        p = score(group_features(pages, inside, left, right), GROUP_WEIGHTS)
        return round(100 * min(p, 0.97))
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
        if missing := missing_pages(pages):
            out.append(missing)
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


def missing_pages(pages: list[Page]) -> str | None:
    """Pages the page numbers say are missing: 'Page 4 seems to be missing'. Only when every
    page is numbered, so a page whose number didn't read isn't taken for a missing one."""
    numbers = [p.clues.marker[0] for p in pages if p.clues.marker]
    if len(numbers) != len(pages) or len(numbers) < 2 or numbers != sorted(set(numbers)):
        return None
    gaps = sorted(set(range(numbers[0], numbers[-1] + 1)) - set(numbers))
    if not gaps or len(gaps) > 3:
        return None
    if len(gaps) == 1:
        return f"Page {gaps[0]} seems to be missing"
    return f"Pages {', '.join(map(str, gaps[:-1]))} and {gaps[-1]} seem to be missing"


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
    g.features = group_features(ordered, inside, left, right)
    if len(pages) == 1 and pages[0].clues.kind in ("blank", "notes"):
        g.set_aside = True
        g.reasons = [
            "It looks blank"
            if pages[0].clues.kind == "blank"
            else "It's a short note that doesn't belong to the pages around it"
        ]
    return g


def rescans(ordered: list[Page]) -> dict[int, Page]:
    """Pages that are another scan of a page earlier in the stream, each with that page. A
    page is often scanned again when the first scan came out badly."""
    kept: dict[int, Page] = {}
    out: dict[int, Page] = {}
    for p in ordered:
        if first := next((kept[i] for i in sorted(p.copies) if i in kept), None):
            out[p.id] = first
        else:
            kept[p.id] = p
    return out


def segment(pages: list[Page]) -> tuple[list[Group], list[Pair], list[Page]]:
    """Groups of pages, with the neighbour scores and the scan order used.

    A page scanned again is set aside, so the pages either side of it still join up; which
    copy to keep is a person's choice (lindley.duplicates)."""
    if not any(p.terms for p in pages):
        weigh_terms(pages)
    ordered = scan_order(pages)
    again = rescans(ordered)
    stream = [p for p in ordered if p.id not in again]
    pairs = pair_scores(stream)
    groups = linked_groups(stream, pairs)
    for p in ordered:
        if first := again.get(p.id):
            g = make_group([p], [], None, None)
            g.set_aside = True
            g.reasons = [f"It looks like {first.file_name} scanned again"]
            groups.append(g)
    return groups, pairs, stream


# ---------------------------------------------------------------- Linking over the whole Inbox


def link(ordered: list[Page], pairs: list[Pair]) -> tuple[list[list[int]], dict]:
    """Chains of page indexes, each in reading order, and the links joining pages scanned apart.

    1. Neighbours in scan order are linked wherever they score CUT_BELOW or more.
    2. Loose ends are joined, best first: a chain that doesn't end to one that doesn't start,
       when one clearly continues the other (JOIN_AT; SWAPPED_AT for two pages fed through the
       wrong way round) and nothing else comes within MARGIN for either end. Page numbers alone
       don't do it when several documents have a page 4 to follow a page 3."""
    n = len(ordered)
    after: list[int | None] = [None] * n
    before: list[int | None] = [None] * n
    root = list(range(n))

    def find(i: int) -> int:
        while root[i] != i:
            root[i] = root[root[i]]
            i = root[i]
        return i

    def join(i: int, j: int) -> None:
        after[i], before[j] = j, i
        root[find(i)] = find(j)

    for i, p in enumerate(pairs):
        if p.score >= CUT_BELOW:
            join(i, i + 1)

    tails = [i for i in range(n) if after[i] is None and not ordered[i].clues.ends_doc]
    heads = [j for j in range(n) if before[j] is None and not ordered[j].clues.starts_doc]
    edges: dict[tuple[int, int], Pair] = {}
    for i in tails:
        for j in heads:
            if find(i) != find(j):
                a, b = ordered[i], ordered[j]
                swapped = j == i - 1  # b was scanned just before a
                edges[(i, j)] = pair(a, b, swapped or adjacent(a, b))
    ranked = sorted(edges.items(), key=lambda e: (-e[1].score, e[0]))

    def bar(i: int, j: int) -> float:
        return SWAPPED_AT if j == i - 1 else JOIN_AT

    for (i, j), p in ranked:
        if p.score < min(JOIN_AT, SWAPPED_AT):
            break
        if p.score < bar(i, j) or after[i] is not None or before[j] is not None:
            continue
        if find(i) == find(j):
            continue
        rivals = [
            q.score
            for (x, y), q in edges.items()
            if (x, y) != (i, j)
            and (x == i or y == j)
            and after[x] is None
            and before[y] is None
            and find(x) != find(y)
        ]
        if max(rivals, default=0.0) > p.score - MARGIN:
            continue
        join(i, j)

    chains = []
    for i in range(n):
        if before[i] is None:
            chain = [i]
            while (k := after[chain[-1]]) is not None:
                chain.append(k)
            chains.append(chain)
    return sorted(chains, key=min), edges


def linked_groups(ordered: list[Page], pairs: list[Pair]) -> list[Group]:
    chains, edges = link(ordered, pairs)
    groups = []
    for chain in chains:
        steps = list(zip(chain, chain[1:], strict=False))
        inside = [pairs[i] if j == i + 1 else edges[(i, j)] for i, j in steps]
        members = set(chain)
        # How sure the breaks around it are: its neighbours in scan order, unless they're in it
        first, last = chain[0], chain[-1]
        left = pairs[first - 1] if first > 0 and first - 1 not in members else None
        right = pairs[last] if last < len(pairs) and last + 1 not in members else None
        g = make_group([ordered[i] for i in chain], inside, left, right)
        if any(j != i + 1 for i, j in steps):
            g.reasons.append("Parts scanned apart were joined")
        if not g.order_settled and [p.id for p in g.pages] == [ordered[i].id for i in chain]:
            g.order_settled = all(p.score >= SURE_LINK for p in inside)
        groups.append(g)
    return groups
