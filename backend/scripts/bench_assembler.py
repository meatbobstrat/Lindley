"""Score the assembler on batches whose right answers are known.

python scripts/bench_assembler.py                 # rules only, 30 made-up batches
python scripts/bench_assembler.py --ai oracle     # plus a stand-in AI that is always right
python scripts/bench_assembler.py --ai settings   # plus the chat AI in your settings.json

python scripts/bench_assembler.py --real lindley.db [--orders in_order,shuffled] [--seeds 10]

--real scores it on real scans instead: assembled PDFs, read into a Lindley database with
scripts/intake.py, are the answer key. Each PDF's pages are fed in as loose scans, all the
documents together, in each order (see ORDERS in lindley.assembler.bench), once per seed.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from lindley.assembler import assemble
from lindley.assembler.bench import (
    ORDERS,
    OracleChat,
    Score,
    arrange,
    load,
    load_real,
    make_batch,
    pdf_answers,
    score,
    score_groups,
)
from lindley.assembler.run import load_inbox
from lindley.assembler.segment import segment
from lindley.config import load_settings
from lindley.db.database import connect, init_db
from lindley.providers.registry import get_provider


def show(conn, truth, s: Score, label: str, calls: int) -> None:
    print(f"{label}: {s.row()}  (AI calls {calls})")
    for r in conn.execute(
        "SELECT d.name, d.grouping_confidence, GROUP_CONCAT(p.id) ids FROM documents d"
        " JOIN pages p ON p.document_id = d.id GROUP BY d.id"
    ):
        docs = {truth[int(i)].doc for i in r["ids"].split(",")}
        print(
            f"      {'OK ' if len(docs) == 1 else 'BAD'} {r['grouping_confidence']:.0f}"
            f" {r['name']} {sorted(docs)}"
        )


def run(seed: int, ai: str, verbose: bool, real=None, order: str = "") -> tuple[Score, int, tuple]:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        settings = load_settings()  # asking for --ai is the OK to call it, whatever its "allow"
        if real:
            src, docs = real
            truth = load_real(conn, src, arrange(docs, order, seed))
            batch = None
        else:
            batch = make_batch(seed)
            truth = load(conn, batch.pages)
        proposal = score_groups(segment(load_inbox(conn))[0], truth)
        chat = None
        if ai == "oracle":
            chat = OracleChat(truth)
        elif ai == "settings":
            chat = get_provider(settings.ai, "assemble")
        calls = assemble(conn, settings.assembler, chat).ai_calls
        if batch and batch.late:
            late = load(conn, batch.late, start_seq=len(batch.pages) + 50)
            truth |= late
            if isinstance(chat, OracleChat):
                chat.truth = truth
            calls += assemble(conn, settings.assembler, chat).ai_calls
        s = score(conn, truth)
        if verbose:
            show(conn, truth, s, f"{order or 'seed'} {seed:3d}", calls)
        conn.close()
        return s, calls, proposal


def report(label: str, scores: list[Score], calls: list[int], proposals: list[tuple]) -> None:
    n = len(scores)
    mean = lambda f: sum(getattr(s, f) for s in scores) / n  # noqa: E731
    made, wrong = sum(s.documents_made for s in scores), sum(s.wrong_documents for s in scores)
    print(f"{label}: {n} batches")
    print(
        f"  pairs    precision {mean('precision'):.3f}  recall {mean('recall'):.3f}"
        f"  F1 {mean('f1'):.3f}"
    )
    print(f"  documents made {made}, wrong {wrong} ({wrong / max(1, made):.1%})")
    print(
        f"  rebuilt exactly {mean('exact'):.1%}, of those in the right order {mean('ordered'):.1%}"
    )
    print(f"  pages left in the Inbox {mean('inbox_left'):.1%}   AI calls {sum(calls)}")
    pr, rc, f1, ex, od = (sum(x[i] for x in proposals) / n for i in range(5))
    print(
        f"  proposed (before confidence): precision {pr:.3f}  recall {rc:.3f}  F1 {f1:.3f}"
        f"  exact {ex:.1%}  in order {od:.1%}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=None, help="default 30, or 10 with --real")
    ap.add_argument("--ai", choices=["none", "oracle", "settings"], default="none")
    ap.add_argument("--real", type=Path, help="a Lindley database holding assembled PDFs")
    ap.add_argument("--orders", default=",".join(ORDERS))
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    if a.real:
        src = connect(a.real)
        docs = pdf_answers(src)
        print(f"{len(docs)} documents, {sum(map(len, docs))} pages, from {a.real}  AI: {a.ai}")
        for order in a.orders.split(","):
            scores, calls, proposals = zip(
                *(run(s, a.ai, a.verbose, (src, docs), order) for s in range(a.seeds or 10)),
                strict=True,
            )
            report(order, list(scores), list(calls), list(proposals))
        return
    scores, calls, proposals = zip(
        *(run(s, a.ai, a.verbose) for s in range(a.seeds or 30)), strict=True
    )
    report(f"made-up batches, AI: {a.ai}", list(scores), list(calls), list(proposals))


if __name__ == "__main__":
    main()
