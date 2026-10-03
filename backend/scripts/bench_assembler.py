"""Score the assembler on made-up archive batches whose right answers are known.

python scripts/bench_assembler.py                 # rules only, 30 batches
python scripts/bench_assembler.py --ai oracle     # plus a stand-in AI that is always right
python scripts/bench_assembler.py --ai settings   # plus the chat AI in your settings.json
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

from lindley.assembler import assemble
from lindley.assembler.bench import OracleChat, Score, load, make_batch, score
from lindley.config import load_settings
from lindley.db.database import connect, init_db
from lindley.providers.registry import get_provider


def run(seed: int, ai: str, verbose: bool) -> tuple[Score, int]:
    batch = make_batch(seed)
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        truth = load(conn, batch.pages)
        settings = load_settings()
        # Asking for --ai is the OK to call it, whatever settings.json says.
        settings.assembler.use_ai = ai != "none"
        chat = None
        if ai == "oracle":
            chat = OracleChat(truth)
        elif ai == "settings":
            chat = get_provider(settings.ai, settings.ai.chat_provider)
        calls = assemble(conn, settings.assembler, chat).ai_calls
        if batch.late:
            late = load(conn, batch.late, start_seq=len(batch.pages) + 50)
            truth |= late
            if isinstance(chat, OracleChat):
                chat.truth = truth
            calls += assemble(conn, settings.assembler, chat).ai_calls
        s = score(conn, truth)
        if verbose:
            print(f"seed {seed:3d}: {s.row()}  (AI calls {calls})")
            for r in conn.execute(
                "SELECT d.name, d.grouping_confidence, GROUP_CONCAT(p.id) ids FROM documents d"
                " JOIN pages p ON p.document_id = d.id GROUP BY d.id"
            ):
                docs = {truth[int(i)].doc for i in r["ids"].split(",")}
                print(
                    f"      {'OK ' if len(docs) == 1 else 'BAD'} {r['grouping_confidence']:.0f}"
                    f" {r['name']} {sorted(docs)}"
                )
        conn.close()
        return s, calls


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=30)
    ap.add_argument("--ai", choices=["none", "oracle", "settings"], default="none")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    scores, calls = zip(*(run(seed, a.ai, a.verbose) for seed in range(a.seeds)), strict=True)
    n = len(scores)
    mean = lambda f: sum(getattr(s, f) for s in scores) / n  # noqa: E731
    made, wrong = sum(s.documents_made for s in scores), sum(s.wrong_documents for s in scores)
    print(f"{n} batches, AI: {a.ai}")
    print(
        f"  pairs    precision {mean('precision'):.3f}  recall {mean('recall'):.3f}"
        f"  F1 {mean('f1'):.3f}"
    )
    print(f"  documents made {made}, wrong {wrong} ({wrong / max(1, made):.1%})")
    print(
        f"  rebuilt exactly {mean('exact'):.1%}, of those in the right order {mean('ordered'):.1%}"
    )
    print(f"  pages left in the Inbox {mean('inbox_left'):.1%}   AI calls {sum(calls)}")


if __name__ == "__main__":
    main()
