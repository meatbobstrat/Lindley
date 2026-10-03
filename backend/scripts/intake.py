"""Read scan files into Lindley and group them into documents, the way the watcher will.

    python scripts/intake.py PATH... [--settings settings.json] [--no-assemble] [--no-ai]

    python scripts/intake.py [PATH...] --vision [--retry-failed]

PATH can be files or folders (searched recursively, in natural name order so scan_2 comes
before scan_10). Point the settings at a scratch folder: the database, the library copies and
the quarantine all go where settings.json says. Running it again is safe: files already read
are skipped, and scans that failed are tried again.

Pages Tesseract struggles with wait for the vision model, because it can cost money. --vision
is your OK to send them (and, with --retry-failed, the ones whose vision call failed before).
With ocr.vision_mode = "auto" in settings, they're sent without asking.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from lindley.assembler import assemble
from lindley.config import load_settings
from lindley.db.database import connect, init_db
from lindley.providers.base import ProviderError
from lindley.providers.registry import get_provider
from lindley.worker.image import BLANK_AT
from lindley.worker.intake import ingest, is_supported
from lindley.worker.pipeline import Pipeline, vision_failures, waiting_for_vision

REVIEW_BELOW = 90  # the review threshold the Settings screen will own
MODES = {
    "hybrid": "Tesseract, and the vision model for hard pages",
    "tesseract": "Tesseract only",
    "vision": "the vision model only",
}


def natural_key(p: Path) -> list:
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(p))]


def find_files(paths: list[Path]) -> list[Path]:
    found: list[Path] = []
    for p in paths:
        if p.is_dir():
            found += sorted((f for f in p.rglob("*") if f.is_file()), key=natural_key)
        else:
            found.append(p)
    return [f for f in found if is_supported(f)]


def where_vision_goes(settings) -> str:
    name = settings.ocr.vision_provider
    cfg = settings.ai.providers.get(name) if name else None
    if cfg is None:
        return "no vision model is set up"
    return f"{name}, {cfg.type} at {cfg.base_url}" if cfg.base_url else f"{name}, {cfg.type} cloud"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("paths", nargs="*", type=Path)
    ap.add_argument("--settings", type=Path, help="path to settings.json")
    ap.add_argument("--no-assemble", action="store_true", help="read only; don't group pages")
    ap.add_argument("--no-ai", action="store_true", help="rules and Tesseract only")
    ap.add_argument(
        "--vision", action="store_true", help="send the pages waiting for the vision model"
    )
    ap.add_argument(
        "--retry-failed", action="store_true", help="with --vision: retry failed vision calls"
    )
    a = ap.parse_args()
    if a.vision and a.no_ai:
        ap.error("--vision and --no-ai don't go together")
    if not (a.paths or a.vision):
        ap.error("give scan files or folders to read, or --vision")

    settings = load_settings(a.settings)
    pipe = Pipeline.from_settings(settings, use_ai=not a.no_ai)
    files = find_files(a.paths)
    if a.paths and not files:
        print("No scan files found (jpg, png, tif, bmp, webp or pdf).", file=sys.stderr)
        return 1
    if files and settings.ocr.engine != "vision" and not pipe.tesseract.is_available():
        print(pipe.tesseract.missing_help(), file=sys.stderr)
        return 1

    init_db(settings.db_path)
    conn = connect(settings.db_path)
    print(f"Database: {settings.db_path.resolve()}")
    if files:
        mode = settings.ocr.vision_mode if pipe.vision else "off"
        vision = "" if settings.ocr.engine == "tesseract" else f" (vision: {mode})"
        print(
            f"Reading {len(files)} file{'s' if len(files) != 1 else ''}"
            f" with {MODES[settings.ocr.engine]}{vision}\n"
        )
    counts = {"new": 0, "duplicate": 0, "failed": 0, "read": 0, "queued": 0}
    for f in files:
        r = ingest(conn, settings, pipe, f)
        counts[r.status] += 1
        line = f"  {f.name}: {r.status}"
        if r.error:
            line += f" ({r.error})"
        if r.reading:
            counts[r.reading if r.reading in ("read", "queued") else "failed"] += 1
            scan = conn.execute(
                "SELECT s.error, AVG(t.confidence) AS conf FROM scans s"
                " LEFT JOIN pages p ON p.scan_id = s.id"
                " LEFT JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1"
                " WHERE s.id = ?",
                (r.scan_id,),
            ).fetchone()
            line += f", {r.pages} page{'s' if r.pages != 1 else ''} {r.reading}"
            line += f" ({scan['conf']:.0f}%)" if scan["conf"] is not None else ""
            if r.reading == "failed":
                line += f": {scan['error']}"
        print(line)

    low = conn.execute(
        "SELECT COUNT(*) FROM v_current_text WHERE confidence < ? AND NOT reviewed", (REVIEW_BELOW,)
    ).fetchone()[0]
    if files:
        print(
            f"\n{counts['new']} new, {counts['duplicate']} already imported, {counts['read']} read,"
            f" {counts['queued']} waiting for the vision model, {counts['failed']} failed."
            f" Pages below {REVIEW_BELOW}% waiting for review: {low}"
        )

    if a.vision:
        if pipe.vision is None:
            print(f"\nCan't send pages to the vision model ({where_vision_goes(settings)}).")
        else:
            print(f"\nSending waiting pages to the vision model ({where_vision_goes(settings)})")
            run = pipe.read_waiting(conn, retry_failed=a.retry_failed)
            print(f"  {run.read} read, {run.failed} failed, {run.waiting} still waiting")
            if run.stopped:
                print(f"  {run.stopped}")
    waiting = waiting_for_vision(conn)
    if waiting:
        print(
            f"{waiting} page{'s are' if waiting != 1 else ' is'} waiting for the vision model"
            f" ({where_vision_goes(settings)}). Run again with --vision to send them."
        )
    failed, error = vision_failures(conn)
    if failed:
        print(
            f"The vision model failed on {failed} page(s): {error}."
            " Run again with --vision --retry-failed to try them again."
        )
    checked = conn.execute(
        "SELECT COUNT(*), SUM(blank_score >= ?), SUM(detected_rotation != 0) FROM pages"
        " WHERE blank_score IS NOT NULL",
        (BLANK_AT,),
    ).fetchone()
    if checked[0]:
        scripts = ", ".join(
            f"{r[1]} {r[0]}"
            for r in conn.execute(
                "SELECT coalesce(script, 'unsure') AS s, COUNT(*) FROM pages"
                " WHERE blank_score IS NOT NULL GROUP BY s ORDER BY COUNT(*) DESC"
            )
        )
        print(f"Page checks: {checked[1]} blank, {checked[2]} turned upright. Writing: {scripts}")

    if not a.no_assemble:
        chat = None
        if not a.no_ai and settings.assembler.use_ai:
            try:
                chat = get_provider(settings.ai, settings.ai.chat_provider)
            except ProviderError:
                chat = None
        report = assemble(conn, settings.assembler, chat)
        print(
            f"\nAssembler: {report.considered} Inbox pages, {report.documents_created} new"
            f" documents, {report.pages_added} pages added to one, {report.hints} hints."
            f" Inbox left: {report.inbox_left}"
        )
        if report.ai_rejected:
            print(f"  AI answers not used: {len(report.ai_rejected)} ({report.ai_rejected[0]})")
        print("\nLindley's documents so far:")
        docs = conn.execute(
            "SELECT d.id, d.name, d.grouping_confidence, d.reasons FROM documents d"
            " WHERE EXISTS (SELECT 1 FROM pages p WHERE p.document_id = d.id) ORDER BY d.id"
        ).fetchall()
        if not docs:
            print("  none yet")
        for d in docs:
            files = [
                f"{r['original_name']}"
                + (f" p{r['page_index'] + 1}" if r["page_count"] > 1 else "")
                for r in conn.execute(
                    "SELECT s.original_name, s.page_count, p.page_index FROM pages p"
                    " JOIN scans s ON s.id = p.scan_id WHERE p.document_id = ? ORDER BY p.position",
                    (d["id"],),
                )
            ]
            conf = d["grouping_confidence"] or 0
            n = f"{len(files)} page{'s' if len(files) != 1 else ''}"
            print(f"  {d['name']} ({n}, {conf:.0f}%): {', '.join(files)}")
            for reason in json.loads(d["reasons"] or "[]")[:3]:
                print(f"      - {reason}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
