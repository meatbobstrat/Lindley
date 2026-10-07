"""Score the assembler on batches whose right answers are known.

python scripts/bench_assembler.py                 # rules only, 30 made-up batches
python scripts/bench_assembler.py --ai oracle     # plus a stand-in AI that is always right
python scripts/bench_assembler.py --ai settings   # plus the chat AI in your settings.json
python scripts/bench_assembler.py --ai gemma-4-e4b --device none  # plus Lindley's own AI
python scripts/bench_assembler.py --real lindley.db --judge qwen3.5-4b --answers judge.db

python scripts/bench_assembler.py --real lindley.db [--orders in_order,shuffled] [--seeds 10]
python scripts/bench_assembler.py --habits one_folder,per_document

--sweep also reports every group Lindley proposed, by its confidence: documents it made, and
"Do these go together?" hints (the rules', and the AI's), with how many were pure (one document's
pages) and exact (all of them). That's the check for assembler.group_at, offer_at and hint_at.

--habits says how the scans are filed: all in one folder, a folder per document, or mixed (see
HABITS in lindley.assembler.bench). Each is reported apart; the default is all of them.

--ai with a model's id (lindley.localai.catalog) sorts with Lindley's own AI, as the builtin
connector does, from the folder in settings; --device none keeps it on the processor.

--judge asks a model of Lindley's own AI whether the pages the rules are unsure of carry on from
one another, as the continues job does (lindley.assembler.continues). Its answers are kept in
--answers across runs, so each pair is asked once.

--real scores it on real scans instead, read into a Lindley database with scripts/intake.py.
The answer key is its assembled PDFs, or if it has none, the folders a person sorted the scans
into, one document each (lindley.assembler.bench.real_answers). Each document's pages are fed
in again, all the documents together, in each order (see ORDERS in lindley.assembler.bench) and
filed by each habit, once per seed.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from lindley.assembler import assemble
from lindley.assembler.bench import (
    BANDS,
    HABITS,
    ORDERS,
    OracleChat,
    Proposed,
    Score,
    arrange,
    kept_answers,
    load,
    load_real,
    make_batch,
    proposed_groups,
    real_answers,
    score,
    score_groups,
)
from lindley.assembler.run import load_inbox
from lindley.assembler.segment import segment
from lindley.config import ProviderConfig, load_settings
from lindley.db.database import connect, init_db
from lindley.localai import server
from lindley.localai.catalog import MODELS
from lindley.providers.registry import build_provider, get_provider


def show(conn, truth, s: Score, label: str, calls: int, asks: tuple[int, int]) -> None:
    print(f"{label}: {s.row()}  (AI calls {calls}, left for it {asks[0]} windows {asks[1]} pages)")
    for r in conn.execute(
        "SELECT d.name, d.grouping_confidence, GROUP_CONCAT(p.id) ids FROM documents d"
        " JOIN pages p ON p.document_id = d.id GROUP BY d.id"
    ):
        docs = {truth[int(i)].doc for i in r["ids"].split(",")}
        print(
            f"      {'OK ' if len(docs) == 1 else 'BAD'} {r['grouping_confidence']:.0f}"
            f" {r['name']} {sorted(docs)}"
        )


DEVICE: str | None = None  # --device, for Lindley's own AI
JUDGE: str | None = None  # --judge: the model asked whether pages carry on
ANSWERS: Path | None = None  # --answers: where its answers are kept across runs


def run(
    seed: int, ai: str, verbose: bool, real=None, order: str = "", habit: str = "one_folder"
) -> tuple[Score, int, tuple, tuple[int, int], list[Proposed]]:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        settings = load_settings()  # asking for --ai is the OK to call it, whatever its "allow"
        # and to call it at once, without waiting for a person to answer first
        settings.assembler.ask_ai_after_days = 0
        if real:
            src, docs = real
            truth = load_real(conn, src, arrange(docs, order, seed), habit, seed)
            batch = None
        else:
            batch = make_batch(seed)
            truth = load(conn, batch.pages, habit=habit, seed=seed)
        proposal = score_groups(segment(load_inbox(conn))[0], truth)
        chat = None
        if ai == "oracle":
            chat = OracleChat(truth)
        elif ai == "settings":
            chat = get_provider(settings.ai, "assemble")
        elif ai in MODELS:
            server.use(settings.ai.local.model_copy(update={"device": DEVICE}))
            chat = build_provider(ProviderConfig(type="builtin"), "assemble", ai)
        judge = None
        if JUDGE:
            server.use(settings.ai.local.model_copy(update={"device": DEVICE}))
            judge = build_provider(ProviderConfig(type="builtin"), "continues", JUDGE)
        with kept_answers(conn, ANSWERS):
            r = assemble(conn, settings.assembler, chat, judge=judge, max_judged=None)
        calls, asks = r.ai_calls, (r.ai_windows, r.ai_pages)
        if batch and batch.late:
            late = load(conn, batch.late, len(batch.pages) + 50, habit=habit, seed=seed)
            truth |= late
            if isinstance(chat, OracleChat):
                chat.truth = truth
            with kept_answers(conn, ANSWERS):
                r = assemble(conn, settings.assembler, chat, judge=judge, max_judged=None)
            calls += r.ai_calls
            asks = (asks[0] + r.ai_windows, asks[1] + r.ai_pages)
        s = score(conn, truth)
        if verbose:
            show(conn, truth, s, f"{habit} {order or 'seed'} {seed:3d}", calls, asks)
        groups = proposed_groups(conn, truth)
        conn.close()
        return s, calls, proposal, asks, groups


def report(
    label: str,
    scores: list[Score],
    calls: list[int],
    proposals: list[tuple],
    asks: list[tuple[int, int]],
) -> None:
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
    print(f"  left for the AI: {sum(a[0] for a in asks)} windows, {sum(a[1] for a in asks)} pages")
    pr, rc, f1, ex, od = (sum(x[i] for x in proposals) / n for i in range(5))
    print(
        f"  proposed (before confidence): precision {pr:.3f}  recall {rc:.3f}  F1 {f1:.3f}"
        f"  exact {ex:.1%}  in order {od:.1%}"
    )


def sweep(groups: list[Proposed]) -> None:
    """Every group proposed, by confidence band: how many, and how many pure and exact."""
    kinds = (
        ("made", lambda g: g.made),
        ("hints", lambda g: not g.made and not g.by_ai),
        ("AI hints", lambda g: not g.made and g.by_ai),
    )
    print("  by confidence: groups (pure, exact)")
    for i, lo in enumerate(BANDS):
        hi = BANDS[i - 1] if i else 101
        band = [g for g in groups if lo <= g.confidence < hi]
        cells = []
        for name, want in kinds:
            gs = [g for g in band if want(g)]
            n = max(1, len(gs))
            pure, exact = sum(g.pure for g in gs) / n, sum(g.exact for g in gs) / n
            cells.append(f"{name} {len(gs)}" + (f" ({pure:.0%}, {exact:.0%})" if gs else ""))
        print(f"    {f'{lo}-{hi - 1}':>7}: " + " | ".join(cells))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=None, help="default 30, or 10 with --real")
    ap.add_argument(
        "--ai",
        choices=["none", "oracle", "settings", *MODELS],
        default="none",
        help="no AI, one always right, settings.json's, or a model of Lindley's own AI",
    )
    ap.add_argument("--device", help="for Lindley's own AI: none, the processor alone")
    ap.add_argument("--judge", choices=list(MODELS), help="asked whether pages carry on")
    ap.add_argument("--answers", type=Path, help="a database to keep the judge's answers in")
    ap.add_argument(
        "--real", type=Path, help="a Lindley database of assembled PDFs or sorted folders"
    )
    ap.add_argument("--orders", default=",".join(ORDERS))
    ap.add_argument("--habits", default=",".join(HABITS))
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--sweep", action="store_true", help="groups by confidence band")
    a = ap.parse_args()
    global DEVICE, JUDGE, ANSWERS
    DEVICE, JUDGE, ANSWERS = a.device, a.judge, a.answers
    try:
        bench(a)
    finally:
        running = server.current()
        if peak := running.peak_memory():
            print(f"Lindley's own AI took {peak / 1e9:.1f} GB at most")
        running.stop()


def bench(a: argparse.Namespace) -> None:
    if a.real:
        src = connect(a.real)
        docs = real_answers(src)
        print(f"{len(docs)} documents, {sum(map(len, docs))} pages, from {a.real}  AI: {a.ai}")
        for habit in a.habits.split(","):
            for order in a.orders.split(","):
                scores, calls, proposals, asks, groups = zip(
                    *(
                        run(s, a.ai, a.verbose, (src, docs), order, habit)
                        for s in range(a.seeds or 10)
                    ),
                    strict=True,
                )
                report(f"{habit}, {order}", *map(list, (scores, calls, proposals, asks)))
                if a.sweep:
                    sweep([g for gs in groups for g in gs])
        src.close()
        return
    for habit in a.habits.split(","):
        scores, calls, proposals, asks, groups = zip(
            *(run(s, a.ai, a.verbose, habit=habit) for s in range(a.seeds or 30)), strict=True
        )
        report(
            f"made-up batches, {habit}, AI: {a.ai}", *map(list, (scores, calls, proposals, asks))
        )
        if a.sweep:
            sweep([g for gs in groups for g in gs])


if __name__ == "__main__":
    main()
