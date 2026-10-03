"""Words that two pages share and few other pages use: names, places, the subject of a story.

Each page's words are weighed by how rare they are (tf-idf), so "Tonopah" on two pages counts for
a lot and "the" for nothing. Rarity is measured over every page in the library when it's given
(`Library`), since a handful of Inbox pages says little about which words are rare. OCR garbage
is mostly unique to its page, so it never pairs pages up.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass

_WORD = re.compile(r"[A-Za-z][a-z]{3,}")  # 4+ letters, capitalised or not, nothing mixed
_STOP_WORDS = """that this with from have were they them then than their there these those what
    when where
    which while will would could should shall been being into upon over under about after
    before again also only some such very more most much many other each every both your
    yours ours mine here just like said says made make came come went goes going done does
    doing take took give gave know knew well even ever never once must might first last long
    little great good same away back down through because without within against during until
    among another however whom whose whether either neither nothing something anything"""
STOP = frozenset(_STOP_WORDS.split())


def page_terms(text: str) -> Counter:
    return Counter(
        w for t in _WORD.findall(text) if (w := t.lower()) not in STOP and len(set(w)) > 2
    )


@dataclass
class Library:
    """How many pages use each word, over a whole library."""

    pages: int
    df: Counter

    @classmethod
    def of(cls, texts) -> Library:
        df: Counter = Counter()
        n = 0
        for t in texts:
            df.update(page_terms(t).keys())
            n += 1
        return cls(n, df)


def weigh(counts: list[Counter], library: Library | None = None) -> list[dict[str, float]]:
    """tf-idf vectors, unit length, for pages whose words were counted by page_terms. A word on
    only one page, or on more than half of them, carries no weight. Without `library`, these
    pages are the library."""
    if library is None:
        n, df = len(counts), Counter(w for c in counts for w in c)
    else:
        n, df = library.pages, library.df
    out = []
    for c in counts:
        v = {
            w: (1 + math.log(k)) * math.log(n / df[w])
            for w, k in c.items()
            if 1 < df[w] <= max(2, n // 2)
        }
        norm = math.sqrt(sum(x * x for x in v.values())) or 1.0
        out.append({w: x / norm for w, x in v.items()})
    return out


def overlap(a: dict[str, float], b: dict[str, float]) -> tuple[float, list[str]]:
    """Cosine similarity of two weighed pages, and the words that count most in it."""
    if len(a) > len(b):
        a, b = b, a
    shared = {w: x * b[w] for w, x in a.items() if w in b}
    top = sorted(shared, key=shared.get, reverse=True)[:3]
    return sum(shared.values()), top
