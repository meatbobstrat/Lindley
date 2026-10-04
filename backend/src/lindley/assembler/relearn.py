"""Learn the evidence weights from documents people vouched for, when that does better.

The shipped weights (weights.py) were set on a handful of real documents. A person's answers are
free labels: every document a person made, accepted or finished says which pages go together and
in what order, and every "Do these go together?" turned down says two pages don't. As they
build up, the weights are fitted again (lindley.assembler.learn) and tried out.

Fitting alone isn't enough: fitted weights have predicted single pairs better yet built worse
documents. So the answer documents are split in two; weights fitted to one half rebuild the
other half's documents (fed in as loose scans, as scripts/bench_assembler.py does), against the
weights in use. They're adopted only if, both ways round, they make no more wrong documents and
rebuild no fewer exactly, and do better somewhere. Every try is kept in `learned_weights`.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from lindley.assembler import bench
from lindley.assembler.evidence import FEATURES, pair
from lindley.assembler.learn import Example, examples, fit
from lindley.assembler.model import weigh_terms
from lindley.assembler.weights import WEIGHTS
from lindley.config import AssemblerSettings

MIN_DOCUMENTS = 10  # answer documents needed before learning is tried
MORE_DOCUMENTS = 5  # and this many more than at the last try
MADE_UP = range(2000, 2030)  # made-up batches learned from as well, so rare evidence stays sane
TRIES = (("in_order", 0), ("in_order", 1), ("swapped", 0), ("swapped", 1))


@dataclass
class Relearned:
    adopted: bool
    documents: int
    report: dict


def learned(conn: sqlite3.Connection) -> dict[str, float] | None:
    """The weights in use, if any were learned and adopted."""
    r = conn.execute(
        "SELECT weights FROM learned_weights WHERE adopted = 1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return json.loads(r[0]) if r else None


def answer_key(conn: sqlite3.Connection) -> list[list[int]]:
    """Documents whose pages a person vouched for, each as its page ids in reading order: made
    or accepted by a person, finished, or worked on; and assembled PDFs read in."""
    docs: list[list[int]] = []
    taken: set[int] = set()
    rows = conn.execute(
        """SELECT d.id FROM documents d WHERE d.origin = 'user' OR d.status = 'complete'
           OR EXISTS (SELECT 1 FROM history h WHERE h.actor = 'user'
                      AND h.target_type = 'document' AND h.target_id = d.id)
           ORDER BY d.id"""
    ).fetchall()
    for (doc,) in rows:
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT p.id FROM pages p JOIN transcriptions t ON t.page_id = p.id"
                " AND t.is_current = 1 WHERE p.document_id = ? ORDER BY p.position",
                (doc,),
            )
        ]
        if len(ids) >= 2:
            docs.append(ids)
            taken |= set(ids)
    docs += [d for d in bench.pdf_answers(conn) if not taken & set(d)]
    return docs


def _pages(conn: sqlite3.Connection, ids: list[int]):
    from lindley.assembler.run import _PAGE_SQL, _page, folder_sizes, library_terms

    marks, sizes = ",".join("?" * len(ids)), folder_sizes(conn)
    pages = [
        _page(r, None, sizes) for r in conn.execute(_PAGE_SQL + f" WHERE p.id IN ({marks})", ids)
    ]
    weigh_terms(pages, library_terms(conn))
    return pages


def library_examples(conn: sqlite3.Connection, docs: list[list[int]]) -> list[Example]:
    """Pairs from the answer documents, in the order they were really scanned, and the pairs of
    pages a person said don't go together."""
    if not docs:
        return []
    truth = {pid: (f"d{n}", i) for n, ids in enumerate(docs) for i, pid in enumerate(ids)}
    out = examples(_pages(conn, list(truth)), truth)
    for (payload,) in conn.execute(
        "SELECT payload FROM suggestions WHERE kind = 'group_pages' AND status = 'dismissed'"
    ):
        ids = json.loads(payload or "{}").get("pages", [])
        if len(ids) == 2 and len(ps := _pages(conn, ids)) == 2:
            a, b = sorted(ps, key=lambda p: ids.index(p.id))
            if (p := pair(a, b)).features:
                out.append(Example([p.features[k] for k in FEATURES], 0, 1.0, (a.id, b.id)))
    return out


@cache
def _made_up() -> tuple[Example, ...]:
    out: list[Example] = []
    for seed in MADE_UP:
        with tempfile.TemporaryDirectory() as tmp:
            conn = _scratch(Path(tmp))
            truth = bench.load(conn, bench.make_batch(seed).pages)
            pages = _inbox(conn)
            conn.close()
        out += examples(pages, {i: (t.doc, t.index) for i, t in truth.items()}, seed)
    return tuple(out)


def _scratch(folder: Path) -> sqlite3.Connection:
    from lindley.db.database import connect, init_db

    init_db(folder / "try.db")
    return connect(folder / "try.db")


def _inbox(conn: sqlite3.Connection):
    from lindley.assembler.run import load_inbox

    return load_inbox(conn)


def _fit(conn: sqlite3.Connection, docs: list[list[int]]) -> dict[str, float]:
    synthetic = list(_made_up())
    real = library_examples(conn, docs)
    share = sum(e.w for e in synthetic) / max(1, len(real))  # real counts as much as made-up
    for e in real:
        e.w = share
    return fit(synthetic + real)


def rebuild(
    conn: sqlite3.Connection, docs: list[list[int]], weights: dict[str, float]
) -> tuple[int, int]:
    """(documents rebuilt exactly, wrong documents made) when the answer documents' pages are
    fed in again as loose scans and assembled with `weights`."""
    from lindley.assembler.run import assemble

    exact = wrong = 0
    for order, seed in TRIES:
        with tempfile.TemporaryDirectory() as tmp:
            scratch = _scratch(Path(tmp))
            truth = bench.load_real(scratch, conn, bench.arrange(docs, order, seed))
            assemble(scratch, AssemblerSettings(), weights=weights)
            s = bench.score(scratch, truth)
            scratch.close()
        exact += round(s.exact * len(docs))
        wrong += s.wrong_documents
    return exact, wrong


def relearn(conn: sqlite3.Connection, force: bool = False) -> Relearned | None:
    """Try learning the weights again, if there are enough new answers (or `force`). Adopts them
    only if they rebuild documents at least as well. None: not tried."""
    docs = answer_key(conn)
    last = conn.execute("SELECT documents FROM learned_weights ORDER BY id DESC LIMIT 1").fetchone()
    if len(docs) < MIN_DOCUMENTS or (not force and last and len(docs) < last[0] + MORE_DOCUMENTS):
        return None
    current = learned(conn) or dict(WEIGHTS)
    halves = (docs[0::2], docs[1::2])
    folds = []
    for test, train in (halves, halves[::-1]):
        candidate = _fit(conn, train)
        (old_exact, old_wrong), (new_exact, new_wrong) = (
            rebuild(conn, test, current),
            rebuild(conn, test, candidate),
        )
        folds.append(
            {
                "documents": len(test),
                "before": {"exact": old_exact, "wrong": old_wrong},
                "after": {"exact": new_exact, "wrong": new_wrong},
            }
        )
    no_worse = all(
        f["after"]["wrong"] <= f["before"]["wrong"] and f["after"]["exact"] >= f["before"]["exact"]
        for f in folds
    )
    better = any(
        f["after"]["wrong"] < f["before"]["wrong"] or f["after"]["exact"] > f["before"]["exact"]
        for f in folds
    )
    adopted = no_worse and better
    weights = _fit(conn, docs)
    report = {"folds": folds, "tries": [list(t) for t in TRIES]}
    with conn:
        conn.execute(
            "INSERT INTO learned_weights (weights, documents, adopted, report) VALUES (?, ?, ?, ?)",
            (json.dumps(weights), len(docs), int(adopted), json.dumps(report)),
        )
    return Relearned(adopted, len(docs), report)
