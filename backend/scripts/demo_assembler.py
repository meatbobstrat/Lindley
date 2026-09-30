"""Watch the Inbox shrink: drop a batch of made-up scans in, let them "finish reading" a few at
a time, and run the assembler after each round, as the worker will.

    python scripts/demo_assembler.py [--seed 7] [--per-round 6]
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from lindley.assembler import assemble
from lindley.assembler.bench import load, make_batch
from lindley.db.database import connect, init_db


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--per-round", type=int, default=6)
    a = ap.parse_args()
    batch = make_batch(a.seed)
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "demo.db"
        init_db(db)
        conn = connect(db)
        load(conn, batch.pages + batch.late)
        with conn:
            conn.execute("UPDATE scans SET status = 'reading'")
        total = len(batch.pages) + len(batch.late)
        print(f"Dropped {total} scans into the watched folder. Inbox: {total}\n")
        shown: set[int] = set()
        rnd = 0
        while True:
            waiting = [
                r[0]
                for r in conn.execute(
                    "SELECT id FROM scans WHERE status = 'reading' ORDER BY id LIMIT ?",
                    (a.per_round,),
                )
            ]
            if not waiting:
                break
            rnd += 1
            with conn:
                conn.executemany(
                    "UPDATE scans SET status = 'read' WHERE id = ?", [(i,) for i in waiting]
                )
            r = assemble(conn)
            inbox = conn.execute(
                "SELECT COUNT(*) FROM pages WHERE document_id IS NULL AND set_aside_at IS NULL"
            ).fetchone()[0]
            reading = conn.execute(
                "SELECT COUNT(*) FROM scans WHERE status = 'reading'"
            ).fetchone()[0]
            print(
                f"Round {rnd}: {len(waiting)} more read. Inbox {inbox} ({reading} still reading)"
                f"  +{r.documents_created} documents, +{r.pages_added} pages added to one"
            )
            for d in conn.execute(
                "SELECT d.id, d.name, d.grouping_confidence, d.reasons,"
                " COUNT(p.id) n FROM documents d JOIN pages p ON p.document_id = d.id"
                " GROUP BY d.id"
            ):
                if d["id"] not in shown:
                    shown.add(d["id"])
                    why = json.loads(d["reasons"] or "[]")
                    print(
                        f"    In progress: {d['name']}"
                        f"  ({d['n']} page{'s' if d['n'] != 1 else ''},"
                        f" {d['grouping_confidence']:.0f}%)"
                        f"  because: {why[0] if why else '-'}"
                    )
        hints = conn.execute(
            "SELECT kind, COUNT(*) FROM suggestions WHERE status = 'open' GROUP BY kind"
        ).fetchall()
        print(
            "\nLeft in the Inbox with a hint: "
            + (", ".join(f"{n} {k.replace('_', ' ')}" for k, n in hints) or "none")
        )
        conn.close()


if __name__ == "__main__":
    main()
