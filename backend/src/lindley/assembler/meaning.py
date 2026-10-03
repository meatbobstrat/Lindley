"""What a page is about, as a small vector, for pages that share a subject but few rare words.

It uses a static embedding model (Model2Vec, minishlab/potion-base-8M: 8 MB, numpy only, about
a quarter of a millisecond a page on a laptop's CPU) when it's installed, with
`pip install lindley[embed]`. Without it, pages are compared by their rare words alone
(lindley.assembler.terms). The model is downloaded once from Hugging Face the first time it's
used; no page text ever leaves the computer. Vectors are made afresh each run, which is quicker
than keeping them up to date as pages are read again.
"""

from __future__ import annotations

import importlib.util
import logging
import os
from functools import cache

log = logging.getLogger(__name__)

MODEL = "minishlab/potion-base-8M"
enabled = True  # tests turn it off, so they never download anything

Vector = tuple[float, ...]


def available() -> bool:
    return enabled and importlib.util.find_spec("model2vec") is not None


@cache
def _model():
    os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    from model2vec import StaticModel

    return StaticModel.from_pretrained(MODEL)


def encode(texts: list[str]) -> list[Vector | None]:
    """A unit vector for each text, or None for each when there's no model (or no text)."""
    if not texts or not available():
        return [None] * len(texts)
    try:
        rows = _model().encode(texts)
    except Exception:  # noqa: BLE001 - no model (offline at first use, say): rare words alone
        log.warning("The model for comparing what pages are about couldn't be loaded")
        return [None] * len(texts)
    out: list[Vector | None] = []
    for row in rows:
        norm = float((row * row).sum()) ** 0.5
        out.append(tuple((row / norm).tolist()) if norm > 0 else None)
    return out


def alike(a: Vector | None, b: Vector | None) -> float | None:
    """Cosine similarity of two pages' vectors, if both have one."""
    if a is None or b is None:
        return None
    return sum(x * y for x, y in zip(a, b, strict=True))
