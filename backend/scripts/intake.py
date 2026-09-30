"""Read scan files into Lindley and group them into documents, the way the watcher will.

    python scripts/intake.py PATH... [--settings settings.json] [--no-assemble] [--no-ai]

PATH can be files or folders (searched recursively, in natural name order so scan_2 comes
before scan_10). Point the settings at a scratch folder: the database, the library copies and
the quarantine all go where settings.json says. Running it again is safe: files already read
are skipped, and scans that failed are tried again.
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
from lindley.worker.intake import import_file, is_supported
from lindley.worker.pipeline import Pipeline

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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("paths", nargs="+", type=Path)
    ap.add_argument("--settings", type=Path, help="path to settings.json")
    ap.add_argument("--no-assemble", action="store_true", help="read only; don't group pages")
    ap.add_argument("--no-ai", action="store_true", help="rules and Tesseract only")
    a = ap.parse_args()

    settings = load_settings(a.settings)
    pipe = Pipeline.from_settings(settings, use_ai=not a.no_ai)
    if settings.ocr.engine != "vision" and not pipe.tesseract.is_available():
        print(pipe.tesseract.missing_help(), file=sys.stderr)
        return 1
    files = find_files(a.paths)
    if not files:
        print("No scan files found (jpg, png, tif, bmp, webp or pdf).", file=sys.stderr)
        return 1

    init_db(settings.db_path)
    conn = connect(settings.db_path)
    print(f"Database: {settings.db_path.resolve()}")
    vision = (
        "" if settings.ocr.engine == "tesseract" else f" (vision {'on' if pipe.vision else 'off'})"
    )
    print(
        f"Reading {len(files)} file{'s' if len(files) != 1 else ''}"
        f" with {MODES[settings.ocr.engine]}{vision}\n"
    )
    counts = {"new": 0, "duplicate": 0, "failed": 0, "read": 0}
    for f in files:
        r = import_file(conn, settings, f)
        counts[r.status] += 1
        line = f"  {f.name}: {r.status}"
        if r.error:
            line += f" ({r.error})"
        scan = (
            r.scan_id
            and conn.execute("SELECT status FROM scans WHERE id = ?", (r.scan_id,)).fetchone()
        )
        if scan and r.pages and scan["status"] != "read":
            state = pipe.process_scan(conn, r.scan_id)
            counts["read" if state == "read" else "failed"] += 1
            conf = conn.execute(
                "SELECT AVG(t.confidence) FROM transcriptions t JOIN pages p ON p.id = t.page_id"
                " WHERE p.scan_id = ? AND t.is_current = 1",
                (r.scan_id,),
            ).fetchone()[0]
            line += f", {r.pages} page{'s' if r.pages != 1 else ''} {state}"
            line += f" ({conf:.0f}%)" if conf is not None else ""
            if state == "failed":
                err = conn.execute("SELECT error FROM scans WHERE id = ?", (r.scan_id,)).fetchone()
                line += f": {err[0]}"
        print(line)

    low = conn.execute(
        "SELECT COUNT(*) FROM v_current_text WHERE confidence < ? AND NOT reviewed", (REVIEW_BELOW,)
    ).fetchone()[0]
    vision_failed = conn.execute(
        "SELECT COUNT(*), MAX(error) FROM intake_steps WHERE step = 'vision' AND status = 'failed'"
    ).fetchone()
    print(
        f"\n{counts['new']} new, {counts['duplicate']} already imported, {counts['read']} read,"
        f" {counts['failed']} failed. Pages below {REVIEW_BELOW}% waiting for review: {low}"
    )
    if vision_failed[0]:
        print(f"The vision model failed on {vision_failed[0]} page(s): {vision_failed[1]}")

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
