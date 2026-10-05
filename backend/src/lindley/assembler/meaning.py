"""What a page is about, as a vector, for pages that share a subject but few rare words.

Pages have no vector yet: the `embed` job (EmbeddingGemma, through a local AI, for the Light
tier and up) is to fill `Page.topic`. Until then `topic_alike` is never measured, and pages are
compared by their rare words alone (lindley.assembler.terms).

Lindley never downloads a model on its own. A small static model (Model2Vec,
potion-base-8M) was tried here, but it was fetched from Hugging Face, which was asked for its
latest version each time Lindley started, and it found a page's own document less often than
rare words (design/database.md), so it was taken out.
"""

from __future__ import annotations

Vector = tuple[float, ...]


def alike(a: Vector | None, b: Vector | None) -> float | None:
    """Cosine similarity of two pages' unit vectors, if both have one."""
    if a is None or b is None:
        return None
    return sum(x * y for x, y in zip(a, b, strict=True))
