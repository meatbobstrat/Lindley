"""How well, and how fast, vision models read the pages Tesseract finds hard: small local ones,
and a cloud AI such as Claude, which can also stand in for the right answer.

python scripts/bench_reading.py --db lindley.db --models glm-ocr,gemma4:e2b,gemma4:e4b
python scripts/bench_reading.py --db lindley.db --pages 17,45 --models glm-ocr --cpu
python scripts/bench_reading.py --db lindley.db --reference gemma4:12b --out readings/
python scripts/bench_reading.py --db lindley.db --reference anthropic --models gemma4:e4b --out r/
python scripts/bench_reading.py --db lindley.db --connections anthropic --effort low --models ""

Pages are those whose text a person checked or corrected, if any (but not with --hard): their
text is the right answer. Otherwise --pages, or else the pages Tesseract read below the
confidence threshold (70), handwritten ones first; with no checked text, each model is scored
against the reading of --reference instead, which is a stand-in for the right answer, not the
right answer. The score is the character error rate (edits needed, as a share of the answer's
characters; lower is better), on text with spacing and line breaks evened out. --out keeps
every reading, to be judged by eye.

Each page is turned upright as Lindley reads it and sent as Lindley's vision job sends it
(lindley.worker.ocr.vision.image_bytes, with Lindley's prompt). --models are called through
Ollama's own API (/api/chat), not the OpenAI-compatible one Lindley's local connector uses, so
that --cpu can keep the model off the graphics card, as on a laptop. --connections are AI
connections in settings.json (e.g. "anthropic", for Claude), called just as Lindley calls them,
with --effort if they take it; --reference may be one. What a paid AI used and cost is printed.
With --out, every reading is kept there, and one already there is used again, not paid for
twice. Nothing is written to the database.
"""

from __future__ import annotations

import argparse
import base64
import re
import sqlite3
import tempfile
import time
from pathlib import Path

import httpx

from lindley.config import load_settings
from lindley.providers.base import Usage
from lindley.providers.prices import cost
from lindley.providers.prompts import transcribe_prompt
from lindley.providers.registry import build_provider
from lindley.worker.image import upright_copy
from lindley.worker.ocr.vision import image_bytes

THRESHOLD = 70
MOST_TOKENS = 2048  # a full typed page is about 600; a model repeating itself is stopped


def even(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def cer(reading: str, answer: str) -> float:
    """Character error rate: Levenshtein distance over the answer's length."""
    a, b = even(reading), even(answer)
    if not b:
        return float(bool(a))
    row = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        prev, row[0] = row[0], i
        for j, cb in enumerate(b, 1):
            prev, row[j] = row[j], min(row[j] + 1, row[j - 1] + 1, prev + (ca != cb))
    return row[-1] / len(b)


def choose(
    db: sqlite3.Connection, pages: str | None, hard: bool = False
) -> tuple[list[sqlite3.Row], bool]:
    """(pages, whether their text was checked by a person). `hard`: the pages Tesseract read
    poorly, even when a person checked some."""
    sql = (
        "SELECT p.id, p.image_path, p.dpi, p.script, (p.detected_rotation + p.user_rotation)"
        " % 360 AS rotation, c.text, c.confidence, c.reviewed FROM pages p"
        " JOIN v_current_text c ON c.page_id = p.id"
    )
    checked = db.execute(sql + " WHERE c.reviewed ORDER BY p.id").fetchall()
    if checked and not pages and not hard:
        return checked, True
    if pages:
        ids = [int(i) for i in pages.split(",")]
        rows = db.execute(sql + f" WHERE p.id IN ({','.join('?' * len(ids))})", ids).fetchall()
    else:
        rows = db.execute(
            sql + " WHERE c.source = 'tesseract' AND coalesce(c.confidence, 0) < ?"
            " ORDER BY p.script != 'handwritten', p.id",
            (THRESHOLD,),
        ).fetchall()
    return rows, False


def read(client: httpx.Client, model: str, image: bytes, cpu: bool) -> str:
    """The model's reading, marked [cut off] if it ran on to MOST_TOKENS."""
    r = client.post(
        "/api/chat",
        json={
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": transcribe_prompt(),
                    "images": [base64.b64encode(image).decode()],
                }
            ],
            "stream": False,
            "think": False,
            "options": {"temperature": 0, "num_ctx": 8192, "num_predict": MOST_TOKENS}
            | ({"num_gpu": 0} if cpu else {}),
        },
    )
    r.raise_for_status()
    text = r.json()["message"]["content"].strip()
    return text + " [cut off]" if r.json().get("done_reason") == "length" else text


def connection(settings_path: Path | None, name: str, effort: str | None, used: list[Usage]):
    """The vision provider of a connection in settings.json, noting what each call used."""
    ai = load_settings(settings_path).ai
    if name not in ai.providers:
        raise SystemExit(f"No AI connection named {name!r} in settings.json")
    provider = build_provider(ai.providers[name], "vision", name=name)
    if hasattr(provider, "on_usage"):
        provider.on_usage = used.append
    if effort and hasattr(provider, "effort"):
        provider.effort = effort
    return provider


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--models", default="glm-ocr,gemma4:e2b,gemma4:e4b", help="Ollama models")
    ap.add_argument("--connections", default="", help="AI connections in settings.json")
    ap.add_argument("--settings", type=Path, help="settings.json (else Lindley's own)")
    ap.add_argument("--effort", help="for connections that take it: low, medium, high...")
    ap.add_argument("--reference", default="gemma4:12b", help="when no text was checked")
    ap.add_argument("--pages", help="page ids, e.g. 17,45")
    ap.add_argument("--hard", action="store_true", help="hard pages, even with some checked")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--max-side", type=int, default=2000)
    ap.add_argument("--cpu", action="store_true", help="keep the models off the graphics card")
    ap.add_argument("--url", default="http://localhost:11434")
    ap.add_argument("--out", type=Path, help="a folder for every reading, kept to use again")
    a = ap.parse_args()

    db = sqlite3.connect(a.db)
    db.row_factory = sqlite3.Row
    rows, checked = choose(db, a.pages, a.hard)
    rows = rows[: a.limit]
    db.close()
    models = [m for m in a.models.split(",") if m]
    conns = [c for c in a.connections.split(",") if c]
    if not checked and a.reference not in models + conns:
        is_conn = a.reference in load_settings(a.settings).ai.providers
        (conns if is_conn else models).append(a.reference)
    # A connection's readings are known by its name, and the effort asked for
    label = {m: m for m in models} | {c: f"{c}@{a.effort}" if a.effort else c for c in conns}
    reference = label[a.reference] if not checked else None
    print(
        f"{len(rows)} pages ({', '.join(str(r['id']) for r in rows)}); answer: "
        + ("text a person checked" if checked else f"{reference}'s reading (a stand-in)")
    )
    readings: dict[str, dict[int, str]] = {"tesseract": {r["id"]: r["text"] or "" for r in rows}}
    seconds: dict[str, float] = {}
    used: dict[str, list[Usage]] = {}
    kept = 0
    if a.out:
        a.out.mkdir(parents=True, exist_ok=True)

    def kept_file(name: str, page_id: int) -> Path | None:
        return a.out / f"page-{page_id}.{re.sub(r'[^\w.@-]', '-', name)}.txt" if a.out else None

    with tempfile.TemporaryDirectory() as tmp, httpx.Client(base_url=a.url, timeout=1800) as c:
        images = {}
        for r in rows:
            path = Path(r["image_path"])
            if r["rotation"]:
                path = upright_copy(path, Path(tmp) / f"{r['id']}.png", r["rotation"], r["dpi"])
            images[r["id"]] = image_bytes(path, a.max_side)
        for model in models + conns:
            name, local = label[model], model in models
            cpu = a.cpu and local and model != a.reference
            if not local:
                used[name] = []
                provider = connection(a.settings, model, a.effort, used[name])
            readings[name] = {}
            took, loaded = [], False
            for r in rows:
                f = kept_file(name, r["id"])
                if f and f.exists():
                    readings[name][r["id"]] = f.read_text(encoding="utf-8")
                    kept += 1
                    continue
                if local and not loaded:
                    read(c, model, images[r["id"]], cpu)  # load it
                    loaded = True
                began = time.monotonic()
                if local:
                    text = read(c, model, images[r["id"]], cpu)
                else:
                    text = provider.transcribe(images[r["id"]]).text.strip()
                readings[name][r["id"]] = text
                took.append(time.monotonic() - began)
                if f:
                    f.write_text(text, encoding="utf-8")
                cut = " (cut off)" if text.endswith("[cut off]") else ""
                print(f"  {name} page {r['id']}: {took[-1]:.0f} s{cut}", flush=True)
            if took:
                seconds[name] = sum(took) / len(took)
    if kept:
        print(f"{kept} readings used again from {a.out}")
    answer = {r["id"]: r["text"] or "" for r in rows} if checked else readings[reference]
    for name, texts in readings.items():
        if name == reference:
            continue
        rates = [cer(texts[r["id"]], answer[r["id"]]) for r in rows]
        each = " ".join(f"{x:.2f}" for x in rates)
        took = f"; {seconds[name]:.0f} s a page" if name in seconds else ""
        on = " on the CPU" if a.cpu and name in seconds and name in models else ""
        print(f"{name:>14}: CER {sum(rates) / len(rates):.2f} (pages: {each}){took}{on}")
    for name, calls in used.items():
        if calls:
            sent = sum(u.input_tokens + u.cache_read_tokens + u.cache_write_tokens for u in calls)
            written = sum(u.output_tokens for u in calls)
            costs = [cost(u) for u in calls]
            spent = f", ${sum(costs):.2f} (estimated)" if None not in costs else ""
            print(
                f"{name:>14}: {len(calls)} calls, {sent:,} tokens sent, {written:,} written"
                f" ({written // len(calls):,} a page, thinking included){spent}"
            )
    if a.out:
        for r in rows:
            parts = [f"===== {name}\n{texts[r['id']]}\n" for name, texts in readings.items()]
            (a.out / f"page-{r['id']}.txt").write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    main()
