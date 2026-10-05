"""Which way of comparing what pages are about best finds a page's own document?

python scripts/bench_meaning.py --real lindley.db
python scripts/bench_meaning.py --real lindley.db --models embeddinggemma

For each page of a real answer key (lindley.assembler.bench.real_answers), the page most like it
among the others is found, and the report says how often that page is from its own document:
by rare words (lindley.assembler.terms), and by each embedding model named, called through the
local connector's `embed` (an Ollama on this computer by default).
Also, how far apart the scores are: the average of each page's best score from its own
document less its best from any other (higher separates better; scores differ in scale from
one way to another, so compare the counts first).
"""

from __future__ import annotations

import argparse
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

from lindley.assembler.bench import arrange, load_real, real_answers
from lindley.assembler.model import weigh_terms
from lindley.assembler.run import library_terms, load_inbox
from lindley.assembler.terms import overlap
from lindley.config import ProviderConfig
from lindley.db.database import connect, init_db
from lindley.providers.registry import build_provider


def cosine(a: list[float], b: list[float]) -> float:
    na, nb = sum(x * x for x in a) ** 0.5, sum(x * x for x in b) ** 0.5
    return sum(x * y for x, y in zip(a, b, strict=True)) / (na * nb) if na and nb else 0.0


def judge(label: str, n: int, doc: list[str], alike: Callable[[int, int], float], took: float):
    found, gap = 0, 0.0
    for i in range(n):
        own = max((alike(i, j) for j in range(n) if j != i and doc[j] == doc[i]), default=None)
        other = max(alike(i, j) for j in range(n) if doc[j] != doc[i])
        best = max(range(n), key=lambda j: -1e9 if j == i else alike(i, j))
        found += doc[best] == doc[i]
        gap += (own - other) if own is not None else 0.0
    print(f"{label:>24}: {found}/{n} find their own document; gap {gap / n:+.3f}; {took:.2f} s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", type=Path, required=True)
    ap.add_argument("--models", default="nomic-embed-text,embeddinggemma")
    ap.add_argument("--url", default="http://localhost:11434/v1")
    a = ap.parse_args()

    src = connect(a.real)
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        truth = load_real(conn, src, arrange(real_answers(src), "in_order", 0))
        started = time.monotonic()
        pages = load_inbox(conn)
        weigh_terms(pages, library_terms(conn))
        took = time.monotonic() - started
        conn.close()
    src.close()
    pages = [p for p in pages if p.text.strip()]
    doc = [truth[p.id].doc for p in pages]
    n = len(pages)
    print(f"{len(set(doc))} documents, {n} pages")
    judge("rare words", n, doc, lambda i, j: overlap(pages[i].terms, pages[j].terms)[0], took)
    for model in a.models.split(","):
        embedder = build_provider(ProviderConfig(type="local", base_url=a.url), "embed", model)
        embedder.embed([pages[0].text])  # load it
        started = time.monotonic()
        vectors = embedder.embed([p.text for p in pages])
        took = time.monotonic() - started
        judge(model, n, doc, lambda i, j, v=vectors: cosine(v[i], v[j]), took)


if __name__ == "__main__":
    main()
