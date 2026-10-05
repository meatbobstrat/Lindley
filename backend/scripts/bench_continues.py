"""Can a small local model tell whether one page carries straight on from another?

python scripts/bench_continues.py --real lindley.db --models qwen3.5:4b,gemma4:e2b
python scripts/bench_continues.py --real lindley.db --models qwen3.5:4b --cpu --lines 3

An experiment for a possible piece of evidence (see design/database.md, the assembler): the
rules' `runs_on` only sees that page A stops mid-sentence and page B starts mid-sentence, which
in a typescript is nearly every page. Here a model is shown A's last lines and B's first lines
and asked whether B is the very next page; the chance it gives "yes", read from its token
probabilities, is the score.

Pairs come from a real answer key (lindley.assembler.bench.real_answers): every page and the
next page of its document (they go together), and pages of different documents (they don't):
each document's last page with another's first page, and pages where `runs_on` fires though
they're from different documents, the cases the rules can't tell apart. The report gives, for
each model, how well the score ranks the pairs that go together above those that don't (AUC:
0.5 is a coin toss, 1.0 perfect), beside the rules' own scores, and the seconds a pair takes.

It calls Ollama's own API (/api/chat), not the OpenAI-compatible one Lindley's local connector
uses, because only Ollama's own returns token probabilities. --cpu keeps the model off the
graphics card, as on a laptop. Nothing is written to the database.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import tempfile
import time
from pathlib import Path

import httpx

from lindley.assembler.bench import arrange, load_real, real_answers
from lindley.assembler.clues import parse_marker, text_lines, trim_noise
from lindley.assembler.evidence import pair
from lindley.assembler.model import Page, weigh_terms
from lindley.assembler.run import library_terms, load_inbox
from lindley.db.database import connect, init_db

SYSTEM = (
    "You check scanned pages of old typed and handwritten documents. You are shown the last "
    "lines of page A and the first lines of page B. Answer yes if page B is the very next page "
    "of the same text as page A, so the writing carries straight on from A to B. Answer no if B "
    "starts something else or belongs somewhere else. Answer with one word: yes or no."
)


def body(p: Page) -> list[str]:
    """The page's lines, without specks at the edges or a page number."""
    lines = [ln.text for ln in trim_noise(text_lines(p.text, p.words))]
    for i in (0, -1):
        if lines and parse_marker(lines[i]):
            lines.pop(i)
    return [ln for ln in lines if ln.strip()]


def question(a: Page, b: Page, n: int) -> str:
    end, start = body(a)[-n:], body(b)[:n]
    return "Page A ends:\n" + "\n".join(end) + "\n\nPage B starts:\n" + "\n".join(start)


def p_yes(client: httpx.Client, model: str, text: str, cpu: bool) -> float:
    """The model's chance of "yes" against "no", from the first token's probabilities."""
    options = {"temperature": 0, "num_predict": 1} | ({"num_gpu": 0} if cpu else {})
    r = client.post(
        "/api/chat",
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM},
                {"role": "user", "content": text},
            ],
            "stream": False,
            "think": False,
            "logprobs": True,
            "top_logprobs": 10,
            "options": options,
        },
    )
    r.raise_for_status()
    first = (r.json().get("logprobs") or [{}])[0]
    yes = no = 0.0
    for t in first.get("top_logprobs") or [first]:
        word = t.get("token", "").strip().lower()
        if word in ("yes", "y"):
            yes += math.exp(t["logprob"])
        elif word in ("no", "n"):
            no += math.exp(t["logprob"])
    return yes / (yes + no) if yes + no else 0.5


def auc(scores: list[tuple[float, bool]]) -> float:
    """Chance a pair that goes together scores above one that doesn't (ties count half)."""
    pos = [s for s, y in scores if y]
    neg = [s for s, y in scores if not y]
    if not pos or not neg:
        return float("nan")
    wins = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


def pairs(pages: list[Page], truth: dict, negatives: int, seed: int) -> list[tuple]:
    """(a, b, goes together, kind): the next page of each document, and pages of different
    documents, at most `negatives` of each kind of those."""
    by_doc: dict[str, list[Page]] = {}
    for p in sorted(pages, key=lambda p: (truth[p.id].doc, truth[p.id].index)):
        by_doc.setdefault(truth[p.id].doc, []).append(p)
    out = [
        (a, b, True, "next page") for d in by_doc.values() for a, b in zip(d, d[1:], strict=False)
    ]
    docs = list(by_doc.values())
    ends = [(x[-1], y[0]) for x in docs for y in docs if x is not y]
    runs = [
        (a, b)
        for x in docs
        for y in docs
        if x is not y
        for a in x
        for b in y
        if a.clues.ends_mid and b.clues.starts_mid
    ]
    r = random.Random(seed)
    for kind, found in (("last, then another's first", ends), ("runs on, other document", runs)):
        out += [(a, b, False, kind) for a, b in r.sample(found, min(negatives, len(found)))]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real", type=Path, required=True)
    ap.add_argument("--models", default="qwen3.5:4b")
    ap.add_argument("--lines", type=int, default=2, help="lines shown from each page")
    ap.add_argument("--negatives", type=int, default=80, help="most pairs of each kind")
    ap.add_argument("--cpu", action="store_true", help="keep the model off the graphics card")
    ap.add_argument("--url", default="http://localhost:11434")
    ap.add_argument("--out", type=Path, help="write each pair's scores here, as JSON")
    a = ap.parse_args()

    src = connect(a.real)
    docs = real_answers(src)
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bench.db"
        init_db(db)
        conn = connect(db)
        truth = load_real(conn, src, arrange(docs, "in_order", 0))
        pages = load_inbox(conn)
        weigh_terms(pages, library_terms(conn))
        conn.close()
    src.close()
    todo = pairs(pages, truth, a.negatives, 0)
    kinds: dict[str, int] = {}
    for *_, kind in todo:
        kinds[kind] = kinds.get(kind, 0) + 1
    print(f"{len(docs)} documents, {len(pages)} pages; pairs: {kinds}; {a.lines} lines each")

    rows = [
        {
            "a": x.id,
            "b": y.id,
            "together": ok,
            "kind": kind,
            "runs_on": float(x.clues.ends_mid and y.clues.starts_mid),
            "rules": pair(x, y, is_adjacent=False).score,
        }
        for x, y, ok, kind in todo
    ]
    runs = [r for r in rows if r["runs_on"]]
    print(
        f"rules alone: AUC runs_on {auc([(r['runs_on'], r['together']) for r in rows]):.3f},"
        f" rules' score {auc([(r['rules'], r['together']) for r in rows]):.3f};"
        f" where runs_on fires ({len(runs)} pairs):"
        f" rules' score {auc([(r['rules'], r['together']) for r in runs]):.3f}"
    )
    with httpx.Client(base_url=a.url, timeout=900) as client:
        for model in a.models.split(","):
            p_yes(client, model, question(todo[0][0], todo[0][1], a.lines), a.cpu)  # load it
            started = time.monotonic()
            for (x, y, *_), r in zip(todo, rows, strict=True):
                r[model] = p_yes(client, model, question(x, y, a.lines), a.cpu)
            each = (time.monotonic() - started) / len(rows)

            def mixed(r: dict, model: str = model) -> float:
                """The rules' and the model's log-odds added, as a fitted weight might."""
                clip = lambda p: min(max(p, 1e-4), 1 - 1e-4)  # noqa: E731
                return sum(math.log(clip(p) / (1 - clip(p))) for p in (r["rules"], r[model]))

            right = sum((r[model] >= 0.5) == r["together"] for r in rows)
            print(
                f"{model:>16}: AUC {auc([(r[model], r['together']) for r in rows]):.3f},"
                f" where runs_on fires {auc([(r[model], r['together']) for r in runs]):.3f},"
                f" with the rules {auc([(mixed(r), r['together']) for r in rows]):.3f};"
                f" right at 0.5 {right}/{len(rows)}; {each:.2f} s a pair"
                + (" on the CPU" if a.cpu else "")
            )
    if a.out:
        a.out.write_text(json.dumps(rows, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
