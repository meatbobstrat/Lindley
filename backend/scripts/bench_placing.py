"""Score where Lindley says pages that turn up later belong.

python scripts/bench_placing.py                    # 30 made-up batches
python scripts/bench_placing.py --real lindley.db  # real scans (PDFs or sorted folders)
python scripts/bench_placing.py --habits per_document --seeds 5
python scripts/bench_placing.py --ai oracle        # with a stand-in AI that is always right
python scripts/bench_placing.py --ai settings      # with the sorting AI in your settings.json

Each batch's documents are made first, as if already in the library, all but a page here and
there: about half of those of two or more pages hold back their first or last page. Those
pages are then scanned in as a later batch, filed the same way (see HABITS in
lindley.assembler.bench), and the rules sort them. For each group of them, the report says
whether the document it truly belongs to is Lindley's first candidate, or among the first few
(lindley.assembler.place), against how many documents a person or the AI would otherwise have
to look through. Then Lindley sorts them, as the watcher would, and the report says how many
pages it added to a document, how many wrongly, and with --ai, how many questions the AI was
asked and how many documents each showed it.
"""

from __future__ import annotations

import argparse
import json
import random
import tempfile
from collections import Counter
from pathlib import Path

from lindley.assembler import assemble
from lindley.assembler.bench import (
    HABITS,
    OracleChat,
    TruePage,
    load,
    load_real,
    make_batch,
    real_answers,
)
from lindley.assembler.model import weigh_terms
from lindley.assembler.place import TOP, candidates
from lindley.assembler.run import library_terms, load_inbox, load_open_documents
from lindley.assembler.segment import segment
from lindley.config import AssemblerSettings, load_settings
from lindley.db.database import connect, init_db
from lindley.providers.registry import get_provider


def hold_back(docs: list[list], seed: int) -> tuple[list, list]:
    """(pages scanned first, pages scanned later): about half the documents of two or more
    pages hold back their first or last page."""
    r = random.Random(seed)
    first, later = [], []
    for d in docs:
        if len(d) >= 2 and r.random() < 0.5:
            i = r.choice((0, len(d) - 1))
            later.append(d[i])
            d = d[:i] + d[i + 1 :]
        first += d
    return first, later


def make_documents(conn, truth: dict[int, TruePage]) -> dict[int, str]:
    """A document for each true document's pages: document id -> true document."""
    by_doc: dict[str, list[int]] = {}
    for pid, tp in sorted(truth.items(), key=lambda t: t[1].index):
        if tp.kind not in ("blank", "notes"):
            by_doc.setdefault(tp.doc, []).append(pid)
    out = {}
    with conn:
        for doc, ids in by_doc.items():
            d = conn.execute(
                "INSERT INTO documents (name, name_source, origin, status)"
                " VALUES (?, 'lindley', 'lindley', 'progress')",
                (doc,),
            ).lastrowid
            conn.executemany(
                "UPDATE pages SET document_id = ?, position = ? WHERE id = ?",
                [(d, i + 1, pid) for i, pid in enumerate(ids)],
            )
            out[d] = doc
    return out


class CountingOracle(OracleChat):
    """The stand-in AI, counting the documents each "which of these?" question shows it."""

    shown = 0

    def chat(self, messages):
        req = json.loads(messages[-1].content)
        if "documents" in req and "pages" in req:
            self.shown += len(req["documents"])
        return super().chat(messages)


def run(seed: int, habit: str, real=None, ai: str = "none") -> Counter:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        if real:
            src, docs = real
            r = random.Random(seed)
            order = list(enumerate(docs))
            r.shuffle(order)
            first, later = hold_back(
                [[(pid, d, i) for i, pid in enumerate(ids)] for d, ids in order], seed
            )
            truth = load_real(conn, src, first, habit, seed)
            names = make_documents(conn, truth)
            late = load_real(conn, src, later, habit, seed, start=len(first) + 50)
        else:
            batch = make_batch(seed)
            docs: dict[str, list[TruePage]] = {}
            for tp in sorted(batch.pages + batch.late, key=lambda t: t.index):
                docs.setdefault(tp.doc, []).append(tp)
            first, later = hold_back(list(docs.values()), seed)
            truth = load(conn, first, habit=habit, seed=seed)
            names = make_documents(conn, truth)
            late = load(conn, later, len(first) + 50, habit=habit, seed=seed)
        out = Counter(documents=len(names))
        pages, open_docs = load_inbox(conn), load_open_documents(conn)
        weigh_terms(
            pages + list({id(p): p for d in open_docs for p in (d.first, d.last)}.values()),
            library_terms(conn),
        )
        groups = [g for g in segment(pages)[0] if not g.set_aside]
        for g in groups:
            belongs = Counter(late[p.id].doc for p in g.pages).most_common(1)[0][0]
            if belongs not in names.values():
                continue  # its document has nothing in the library: nothing to find

            def right(c, belongs=belongs) -> bool:
                if c.document:
                    return names[c.document.id] == belongs
                return any(late[p.id].doc == belongs for p in c.group.pages)

            ranked = candidates(g, open_docs, groups)
            old = candidates(g, open_docs, [], top=1)  # documents only, as before
            out["groups"] += 1
            out["top1"] += bool(ranked) and right(ranked[0])
            out["topk"] += any(right(c) for c in ranked)
            out["old_top1"] += bool(old) and right(old[0])
            out["listed"] += len(ranked)
            out["first_sure"] += bool(ranked) and ranked[0].score >= 75
            out["first_sure_right"] += bool(ranked) and ranked[0].score >= 75 and right(ranked[0])
        truth = truth | late
        chat = None
        if ai == "oracle":
            chat = CountingOracle(truth)
        elif ai == "settings":  # asking for --ai is the OK to call it, whatever its "allow"
            chat = get_provider(load_settings().ai, "assemble")
        r = assemble(conn, AssemblerSettings(ask_ai_after_days=0), chat)
        for pid, doc in conn.execute(
            "SELECT id, document_id FROM pages WHERE document_id IS NOT NULL"
        ):
            if pid in late and doc in names:
                out["added"] += 1
                out["added_wrong"] += names[doc] != late[pid].doc
            elif pid in late:
                out["new_doc"] += 1
        out["late_pages"] += sum(1 for tp in late.values() if tp.kind not in ("blank", "notes"))
        out["ai_calls"] += r.ai_calls
        out["ai_placed"] += r.ai_placed
        out["shown"] += getattr(chat, "shown", 0)
        conn.close()
        return out


def report(label: str, c: Counter) -> None:
    n = max(1, c["groups"])
    print(f"{label}: {c['groups']} groups of pages scanned later, {c['documents']} documents")
    print(
        f"  right document first {c['top1'] / n:.0%} (ranking open documents alone:"
        f" {c['old_top1'] / n:.0%}); among the first {TOP} {c['topk'] / n:.0%};"
        f" not listed {1 - c['topk'] / n:.0%}"
    )
    print(
        f"  candidates shown {c['listed'] / n:.1f} on average, instead of"
        f" {c['documents'] / max(1, c['runs']):.0f} documents;"
        f" first at 75 or more {c['first_sure']}, of them right {c['first_sure_right']}"
    )
    calls = max(1, c["ai_calls"])
    print(
        f"  sorted: {c['added']} of {c['late_pages']} pages added to a document,"
        f" {c['added_wrong']} wrongly, {c['new_doc']} made a document of their own;"
        f" AI calls {c['ai_calls']}, pages it placed"
        f" {c['ai_placed']}, documents shown per call {c['shown'] / calls:.1f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", type=Path)
    ap.add_argument("--seeds", type=int, default=None, help="default 30, or 10 with --real")
    ap.add_argument("--habits", default=",".join(HABITS))
    ap.add_argument("--ai", choices=["none", "oracle", "settings"], default="none")
    a = ap.parse_args()
    real = None
    if a.real:
        src = connect(a.real)
        real = (src, real_answers(src))
        print(f"{len(real[1])} documents from {a.real}")
    for habit in a.habits.split(","):
        total = Counter()
        for seed in range(a.seeds or (10 if real else 30)):
            total += run(seed, habit, real, a.ai) + Counter(runs=1)
        report(habit, total)
    if real:
        real[0].close()


if __name__ == "__main__":
    main()
