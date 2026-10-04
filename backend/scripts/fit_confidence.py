"""Fit a group's confidence: the chance that the pages the rules put together, and only those,
are one document.

python scripts/fit_confidence.py --real lindley.db            # fit, and test it
python scripts/fit_confidence.py --real lindley.db --write    # save it to weights.py

The groups come from made-up batches and from real scans, filed by each scanning habit
(HABITS in lindley.assembler.bench, or --habits) (assembled PDFs or folders a person
sorted are the answer key, as for scripts/fit_assembler.py). Each real document is left out in
turn and its groups scored by a fit made without it, and the report says what the hand-made
rule and the fitted one would each make into documents at group_at. --write saves GROUP_WEIGHTS
to weights.py: only the weights, no text from any scan.
"""

from __future__ import annotations

import argparse
import math
import re
from pathlib import Path

from fit_assembler import MADE_UP, OUT, REAL_SEEDS, batch

from lindley.assembler.bench import (
    HABITS,
    ORDERS,
    arrange,
    load,
    load_real,
    make_batch,
    real_answers,
)
from lindley.assembler.evidence import score
from lindley.assembler.learn import L2, Example, fit
from lindley.assembler.segment import GROUP_FEATURES, segment
from lindley.config import AssemblerSettings
from lindley.db.database import connect

# The group, its confidence by the rule, its true documents, and how its scans were filed
Tagged = tuple[Example, int, set[str], str]


def groups(pages, truth) -> list[tuple[Example, int, set[str]]]:
    """Each group the rules propose: right when it holds every page of one true document in
    the batch, and no others."""
    size: dict[str, int] = {}
    for d, _ in truth.values():
        size[d] = size.get(d, 0) + 1
    out = []
    for g in segment(pages)[0]:
        if g.set_aside:
            continue
        docs = {truth[p.id][0] for p in g.pages}
        right = len(docs) == 1 and size[next(iter(docs))] == len(g.pages)
        x = [g.features[k] for k in GROUP_FEATURES]
        out.append((Example(x, int(right)), g.confidence, docs))
    return out


def fitted(e: Example, weights: dict[str, float]) -> float:
    return score(dict(zip(GROUP_FEATURES, e.x, strict=True)), weights)


def log_loss(rows: list[tuple[float, int]]) -> float:
    return -sum(
        math.log(min(max(p, 1e-6), 1 - 1e-6)) if y else math.log(1 - min(p, 1 - 1e-6))
        for p, y in rows
    ) / max(1, len(rows))


BINS = (0.0, 0.45, 0.75, 1.01)  # below hint_at, from hint_at to group_at, and above


def calibration(rows: list[tuple[float, int]]) -> str:
    """For each band of confidence, how sure it said it was against how often it was right.
    People and the AI read a confidence as a chance, so the two should be close."""
    out = []
    for lo, hi in zip(BINS, BINS[1:], strict=False):
        band = [(p, y) for p, y in rows if lo <= p < hi]
        if band:
            said = sum(p for p, _ in band) / len(band)
            right = sum(y for _, y in band) / len(band)
            out.append(f"{lo:.0%}+: said {said:.0%}, right {right:.0%} of {len(band)}")
    return "; ".join(out)


def report(label: str, rows: list[tuple[float, int]], at: float) -> None:
    made = [y for p, y in rows if p >= at]
    brier = sum((p - y) ** 2 for p, y in rows) / max(1, len(rows))
    print(
        f"  {label:20} log-loss {log_loss(rows):.3f}  Brier {brier:.3f}"
        f"  documents made {len(made):3d}, right {sum(made):3d}, wrong {len(made) - sum(made):2d}"
        f"  (of {len(rows)} groups, {sum(y for _, y in rows)} right)"
    )
    print(f"  {'':20} {calibration(rows)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", type=Path, action="append", default=[])
    ap.add_argument("--l2", type=float, default=L2)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--habits", default=",".join(HABITS))
    a = ap.parse_args()
    habits = a.habits.split(",")

    synthetic: list[Tagged] = []
    for habit in habits:
        for seed in MADE_UP:
            pages, truth = batch(
                lambda conn, s=seed, h=habit: load(conn, make_batch(s).pages, habit=h, seed=s)
            )
            synthetic += [(e, c, ds, habit) for e, c, ds in groups(pages, truth)]
    real: list[Tagged] = []
    for db in a.real:
        src = connect(db)
        docs = real_answers(src)
        for habit in habits:
            for order in ORDERS:
                for seed in REAL_SEEDS:
                    pages, truth = batch(
                        lambda conn, o=order, s=seed, h=habit, src=src, docs=docs: load_real(
                            conn, src, arrange(docs, o, s), h, s
                        )
                    )
                    real += [
                        (e, c, {f"{db}:{d}" for d in ds}, habit)
                        for e, c, ds in groups(pages, truth)
                    ]
        src.close()
    print(f"groups: {len(synthetic)} made-up, {len(real)} real")
    if real:  # the real groups count as much as all the made-up ones together
        share = sum(e.w for e, *_ in synthetic) / len(real)
        for e, *_ in real:
            e.w = share
    weights = fit([e for e, *_ in synthetic + real], a.l2, names=GROUP_FEATURES)
    print(f"\n{'evidence':14} {'fitted':>8}")
    for k in GROUP_FEATURES:
        print(f"{k:14} {weights[k]:8.2f}")

    at = AssemblerSettings().group_at / 100
    print(f"\nmade-up groups, in the fit (a document is made at {at:.0%}):")
    for habit in habits:
        mine = [(e, c) for e, c, _, h in synthetic if h == habit]
        print(f" {habit}")
        report("hand-made rule", [(c / 100, e.y) for e, c in mine], at)
        report("fitted", [(fitted(e, weights), e.y) for e, _ in mine], at)
    if real:
        docs = sorted({d for _, _, ds, _ in real for d in ds})
        old: dict[str, list] = {h: [] for h in habits}
        new: dict[str, list] = {h: [] for h in habits}
        for d in docs:
            fold = fit(
                [e for e, *_ in synthetic] + [e for e, _, ds, _ in real if d not in ds],
                a.l2,
                names=GROUP_FEATURES,
            )
            for e, c, ds, h in real:
                if min(ds) == d:  # each group once, left out with the first document in it
                    old[h].append((c / 100, e.y))
                    new[h].append((fitted(e, fold), e.y))
        print(f"real groups, each of {len(docs)} documents left out of the fit in turn:")
        for habit in habits:
            print(f" {habit}")
            report("hand-made rule", old[habit], at)
            report("fitted without it", new[habit], at)

    if a.write:
        body = ", ".join(f'"{k}": {weights[k]}' for k in GROUP_FEATURES)
        text = re.sub(
            r"GROUP_WEIGHTS: dict\[str, float\] = \{.*?\}",
            f"GROUP_WEIGHTS: dict[str, float] = {{{body}}}",
            OUT.read_text(encoding="utf-8"),
            flags=re.S,
        )
        OUT.write_text(text, encoding="utf-8")
        print(f"\nwrote GROUP_WEIGHTS to {OUT}")


if __name__ == "__main__":
    main()
