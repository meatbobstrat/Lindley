"""Fit the evidence weights to pages whose right answer is known.

The model is a logistic regression over the features in lindley.assembler.evidence: small,
quick to run on any laptop, and easy to explain, since each piece of evidence has one weight.
It's fitted with a few Newton steps in plain Python; there are only a couple of dozen weights.

Training pairs come from batches of pages fed in the way the assembler sees them: each page
and the next one scanned, and a sample of pages further apart, which is what joining parts
scanned apart looks at. The assembler asks whether one page comes next after another, so pages
of one document count as together only when they're neighbours (or two apart: a page swapped or
missing); pages of different documents count as apart, and the rest are left out.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from lindley.assembler.evidence import FEATURES, pair, score
from lindley.assembler.model import Page, weigh_terms
from lindley.assembler.segment import scan_order

L2 = 1.0  # pulls every weight but the bias towards 0, so rare evidence can't run away with it
FAR_PAIRS = 3  # pages further apart sampled per page
NEAR = 2  # pages of one document this close count as neighbours


@dataclass
class Example:
    x: list[float]
    y: int  # 1: same document
    w: float = 1.0  # how much it counts
    pages: tuple[int, int] = (0, 0)  # the pair it came from


def examples(
    pages: list[Page], truth: dict[int, tuple[str, int]], seed: int = 0, weight: float = 1.0
) -> list[Example]:
    """Training pairs from one batch of pages. `truth` gives each page's true document and its
    position in it."""
    r = random.Random(seed)
    weigh_terms(pages)
    ordered = scan_order(pages)
    picked = list(zip(ordered, ordered[1:], strict=False))
    for a in ordered:
        for b in r.sample(ordered, min(FAR_PAIRS, len(ordered))):
            if b is not a:
                picked.append((a, b))
    out = []
    for a, b in picked:
        p = pair(a, b)
        if not p.features:  # copies of one page: a hard rule, not evidence to weigh
            continue
        if "a blank page or a separate note" in p.breaks:
            continue
        (da, ia), (db, ib) = truth[a.id], truth[b.id]
        if da == db and abs(ib - ia) > NEAR:
            continue  # the same document, but nowhere near: not what pair() is asked
        out.append(Example([p.features[k] for k in FEATURES], int(da == db), weight, (a.id, b.id)))
    return out


def fit(
    data: list[Example],
    l2: float = L2,
    steps: int = 25,
    fixed: dict[str, float] | None = None,
    names: tuple[str, ...] = FEATURES,
) -> dict[str, float]:
    """Weights that best predict the labels (Newton's method on the penalised log-loss).
    Weights in `fixed` are kept as they are; only the others are fitted around them. `names`
    are the features in each example's order; the first is the bias."""
    n = len(names)
    fixed = fixed or {}
    beta = [fixed.get(k, 0.0) for k in names]
    held = [k in fixed for k in names]
    for _ in range(steps):
        grad = [0.0] * n
        hess = [[0.0] * n for _ in range(n)]
        for e in data:
            z = sum(b * x for b, x in zip(beta, e.x, strict=True))
            p = 1 / (1 + math.exp(-max(-30.0, min(30.0, z))))
            g, h = e.w * (p - e.y), e.w * p * (1 - p)
            for i, xi in enumerate(e.x):
                if xi:
                    grad[i] += g * xi
                    row = hess[i]
                    for j, xj in enumerate(e.x):
                        if xj:
                            row[j] += h * xi * xj
        for i in range(1, n):  # names[0] is the bias, never pulled towards 0
            grad[i] += l2 * beta[i]
            hess[i][i] += l2
        hess[0][0] += 1e-6
        for i in range(n):
            if held[i]:
                grad[i] = 0.0
                hess[i] = [0.0] * n
                for row in hess:
                    row[i] = 0.0
                hess[i][i] = 1.0
        step = _solve(hess, grad)
        beta = [b - s for b, s in zip(beta, step, strict=True)]
        if max(abs(s) for s in step) < 1e-6:
            break
    return {k: round(b, 3) for k, b in zip(names, beta, strict=True)}


def _solve(a: list[list[float]], b: list[float]) -> list[float]:
    """Solve a x = b by Gaussian elimination with partial pivoting."""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for c in range(n):
        piv = max(range(c, n), key=lambda r: abs(m[r][c]))
        m[c], m[piv] = m[piv], m[c]
        if abs(m[c][c]) < 1e-12:
            continue
        for r in range(n):
            if r != c and m[r][c]:
                f = m[r][c] / m[c][c]
                for k in range(c, n + 1):
                    m[r][k] -= f * m[c][k]
    return [m[i][n] / m[i][i] if abs(m[i][i]) >= 1e-12 else 0.0 for i in range(n)]


def log_loss(
    data: list[Example], weights: dict[str, float], names: tuple[str, ...] = FEATURES
) -> float:
    total = sum(e.w for e in data) or 1.0
    loss = 0.0
    for e in data:
        p = min(max(score(dict(zip(names, e.x, strict=True)), weights), 1e-9), 1 - 1e-9)
        loss -= e.w * (math.log(p) if e.y else math.log(1 - p))
    return loss / total


def accuracy(data: list[Example], weights: dict[str, float]) -> float:
    total = sum(e.w for e in data) or 1.0
    right = sum(
        e.w
        for e in data
        if (score(dict(zip(FEATURES, e.x, strict=True)), weights) >= 0.5) == bool(e.y)
    )
    return right / total
