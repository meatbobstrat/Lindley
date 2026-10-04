"""How well, and how fast, small local vision models read the pages Tesseract finds hard.

python scripts/bench_reading.py --db lindley.db --models glm-ocr,gemma4:e2b,gemma4:e4b
python scripts/bench_reading.py --db lindley.db --pages 17,45 --models glm-ocr --cpu
python scripts/bench_reading.py --db lindley.db --reference gemma4:12b --out readings/

Pages are those whose text a person checked or corrected, if any: their text is the right
answer. Otherwise --pages, or else the pages Tesseract read below the confidence threshold
(70), handwritten ones first; with no checked text, each model is scored against the reading
of --reference instead, which is a stand-in for the right answer, not the right answer. The
score is the character error rate (edits needed, as a share of the answer's characters; lower
is better), on text with spacing and line breaks evened out. --out keeps every reading, to be
judged by eye.

Each page is turned upright as Lindley reads it and sent as Lindley's vision job sends it
(lindley.worker.ocr.vision.image_bytes, with Lindley's prompt). It calls Ollama's own API
(/api/chat), not the OpenAI-compatible one Lindley's local connector uses, so that --cpu can
keep the model off the graphics card, as on a laptop. Nothing is written to the database.
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

from lindley.providers.prompts import transcribe_prompt
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


def choose(db: sqlite3.Connection, pages: str | None) -> tuple[list[sqlite3.Row], bool]:
    """(pages, whether their text was checked by a person)."""
    sql = (
        "SELECT p.id, p.image_path, p.dpi, p.script, (p.detected_rotation + p.user_rotation)"
        " % 360 AS rotation, c.text, c.confidence, c.reviewed FROM pages p"
        " JOIN v_current_text c ON c.page_id = p.id"
    )
    checked = db.execute(sql + " WHERE c.reviewed ORDER BY p.id").fetchall()
    if checked and not pages:
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--models", default="glm-ocr,gemma4:e2b,gemma4:e4b")
    ap.add_argument("--reference", default="gemma4:12b", help="when no text was checked")
    ap.add_argument("--pages", help="page ids, e.g. 17,45")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--max-side", type=int, default=2000)
    ap.add_argument("--cpu", action="store_true", help="keep the models off the graphics card")
    ap.add_argument("--url", default="http://localhost:11434")
    ap.add_argument("--out", type=Path, help="a folder for every reading")
    a = ap.parse_args()

    db = sqlite3.connect(a.db)
    db.row_factory = sqlite3.Row
    rows, checked = choose(db, a.pages)
    rows = rows[: a.limit]
    db.close()
    models = a.models.split(",")
    if not checked and a.reference not in models:
        models.append(a.reference)
    print(
        f"{len(rows)} pages ({', '.join(str(r['id']) for r in rows)}); answer: "
        + ("text a person checked" if checked else f"{a.reference}'s reading (a stand-in)")
    )
    readings: dict[str, dict[int, str]] = {"tesseract": {r["id"]: r["text"] or "" for r in rows}}
    seconds: dict[str, float] = {}
    with tempfile.TemporaryDirectory() as tmp, httpx.Client(base_url=a.url, timeout=1800) as c:
        images = {}
        for r in rows:
            path = Path(r["image_path"])
            if r["rotation"]:
                path = upright_copy(path, Path(tmp) / f"{r['id']}.png", r["rotation"], r["dpi"])
            images[r["id"]] = image_bytes(path, a.max_side)
        for model in models:
            cpu = a.cpu and model != a.reference
            read(c, model, images[rows[0]["id"]], cpu)  # load it
            started = time.monotonic()
            readings[model] = {}
            for r in rows:
                began = time.monotonic()
                readings[model][r["id"]] = text = read(c, model, images[r["id"]], cpu)
                cut = " (cut off)" if text.endswith("[cut off]") else ""
                print(
                    f"  {model} page {r['id']}: {time.monotonic() - began:.0f} s{cut}", flush=True
                )
            seconds[model] = (time.monotonic() - started) / len(rows)
    answer = {r["id"]: r["text"] or "" for r in rows} if checked else readings[a.reference]
    for name, texts in readings.items():
        if name == a.reference and not checked:
            continue
        rates = [cer(texts[r["id"]], answer[r["id"]]) for r in rows]
        each = " ".join(f"{x:.2f}" for x in rates)
        took = f"; {seconds[name]:.0f} s a page" if name in seconds else ""
        on = " on the CPU" if a.cpu and name in seconds else ""
        print(f"{name:>14}: CER {sum(rates) / len(rates):.2f} (pages: {each}){took}{on}")
    if a.out:
        a.out.mkdir(parents=True, exist_ok=True)
        for r in rows:
            parts = [f"===== {name}\n{texts[r['id']]}\n" for name, texts in readings.items()]
            (a.out / f"page-{r['id']}.txt").write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    main()
