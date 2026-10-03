"""Fit the assembler's evidence weights to pages whose right answer is known.

python scripts/fit_assembler.py                       # made-up batches only
python scripts/fit_assembler.py --real lindley.db     # plus real scans (assembled PDFs)
python scripts/fit_assembler.py --real lindley.db --write

Real scans count as much as all the made-up batches together. Each real document is also left
out in turn and its pairs predicted by weights fitted without it, which says how well the
weights should do on documents they've never seen. --write saves them to
src/lindley/assembler/weights.py. Only the weights are saved: no text from any scan.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from lindley.assembler.bench import ORDERS, arrange, load, load_real, make_batch, pdf_answers
from lindley.assembler.evidence import FEATURES
from lindley.assembler.learn import L2, Example, accuracy, examples, fit, log_loss
from lindley.assembler.run import load_inbox
from lindley.assembler.weights import WEIGHTS
from lindley.db.database import connect, init_db

OUT = Path(__file__).resolve().parents[1] / "src" / "lindley" / "assembler" / "weights.py"
MADE_UP = range(1000, 1060)  # not the bench's seeds, so the bench stays a fair test
REAL_SEEDS = range(5)


def batch(fill) -> tuple[list, dict[int, tuple[str, int]]]:
    """Pages loaded into a scratch database by fill(conn), and each page's true document and
    position in it."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "fit.db"
        init_db(db)
        conn = connect(db)
        truth = fill(conn)
        pages = load_inbox(conn)
        conn.close()
    return pages, {
        pid: (tp.doc if tp.kind not in ("blank", "notes") else f"single{pid}", tp.index)
        for pid, tp in truth.items()
    }


def made_up() -> list[Example]:
    out = []
    for seed in MADE_UP:
        pages, truth = batch(lambda conn, s=seed: load(conn, make_batch(s).pages))
        out += examples(pages, truth, seed)
    return out


def real(db: Path) -> list[tuple[set[str], Example]]:
    """Examples from real scans, each with the documents its two pages came from."""
    src = connect(db)
    docs = pdf_answers(src)
    out = []
    for order in ORDERS:
        for seed in REAL_SEEDS:
            pages, truth = batch(
                lambda conn, o=order, s=seed: load_real(conn, src, arrange(docs, o, s))
            )
            for e in examples(pages, truth, seed):
                out.append(({f"{db}:{truth[i][0]}" for i in e.pages}, e))
    src.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", type=Path, action="append", default=[])
    ap.add_argument("--l2", type=float, default=L2)
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args()

    synthetic = made_up()
    tagged = [x for db in a.real for x in real(db)]
    print(f"pairs: {len(synthetic)} made-up, {len(tagged)} real")
    if tagged:
        share = sum(e.w for e in synthetic) / len(tagged)
        for _, e in tagged:
            e.w = share
    weights = fit(synthetic + [e for _, e in tagged], a.l2)

    print(f"\n{'evidence':16} {'fitted':>8} {'before':>8}")
    for k in FEATURES:
        print(f"{k:16} {weights[k]:8.2f} {WEIGHTS.get(k, 0.0):8.2f}")
    print(
        f"\nmade-up pairs: log-loss {log_loss(synthetic, weights):.3f}"
        f" (before {log_loss(synthetic, WEIGHTS):.3f}), right {accuracy(synthetic, weights):.1%}"
        f" (before {accuracy(synthetic, WEIGHTS):.1%})"
    )
    if tagged:
        docs = sorted({d for ds, _ in tagged for d in ds})
        loss = right = before_loss = before_right = n = 0.0
        for d in docs:
            train = synthetic + [e for ds, e in tagged if d not in ds]
            test = [e for ds, e in tagged if d in ds]
            fold = fit(train, a.l2)
            k = len(test)
            loss += k * log_loss(test, fold)
            right += k * accuracy(test, fold)
            before_loss += k * log_loss(test, WEIGHTS)
            before_right += k * accuracy(test, WEIGHTS)
            n += k
        print(
            f"real pairs, each of {len(docs)} documents left out in turn:"
            f" log-loss {loss / n:.3f} (before {before_loss / n:.3f}),"
            f" right {right / n:.1%} (before {before_right / n:.1%})"
        )
    if a.write:
        lines = "\n".join(f'    "{k}": {weights[k]},' for k in FEATURES)
        sources = "made-up batches" + (
            f" and {len(a.real)} database(s) of real scans" if a.real else ""
        )
        OUT.write_text(
            '"""Weights for the evidence in lindley.assembler.evidence, in log-odds.\n\n'
            f"Fitted by scripts/fit_assembler.py to {sources}, L2 {a.l2}.\n"
            'Run it again to refit; edit by hand only to try something out.\n"""\n\n'
            f"WEIGHTS = {{\n{lines}\n}}\n",
            encoding="utf-8",
        )
        print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
