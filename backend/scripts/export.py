"""Export documents as searchable PDFs, into the library's Exports folder.

    python scripts/export.py [--settings settings.json]             # list documents
    python scripts/export.py --doc ID [ID...] [--settings ...]      # export these
    python scripts/export.py --ready [--settings ...]               # every document Lindley
                                                                    #   thinks is complete
    python scripts/export.py --all [--settings ...]                 # every document

Each PDF holds the document's pages in order, each scan with what Lindley read from it as
searchable text over the writing. Exporting marks a document Completed. Exporting it again
replaces its PDF.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lindley.config import load_settings
from lindley.db.database import connect, init_db
from lindley.db.progress import document_progress
from lindley.export import export_document


def main() -> int:
    # Document names hold whatever was read off the page: sent to a file or pipe on Windows, a
    # character the code page lacks would otherwise stop the run.
    sys.stdout.reconfigure(errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--settings", type=Path, help="path to settings.json")
    which = ap.add_mutually_exclusive_group()
    which.add_argument("--doc", type=int, nargs="+", metavar="ID", help="documents to export")
    which.add_argument("--ready", action="store_true", help="documents ready to export")
    which.add_argument("--all", action="store_true", help="every document")
    a = ap.parse_args()

    settings = load_settings(a.settings)
    init_db(settings.db_path)
    conn = connect(settings.db_path)
    print(f"Database: {settings.db_path.resolve()}")
    docs = conn.execute(
        "SELECT d.id, d.name, d.status, COUNT(p.id) AS pages FROM documents d"
        " LEFT JOIN pages p ON p.document_id = d.id GROUP BY d.id ORDER BY d.id"
    ).fetchall()
    progress = {d["id"]: document_progress(conn, d["id"], settings.ocr.review_below) for d in docs}

    if not (a.doc or a.ready or a.all):
        if not docs:
            print("No documents yet.")
        for d in docs:
            p = progress[d["id"]]
            state = "completed" if d["status"] == "complete" else "ready" if p.ready else ""
            todo = "; ".join(c.detail or c.label for c in p.checks if not c.done)
            print(
                f"  {d['id']:>4}  {d['name']}  ({d['pages']} pages{', ' + state if state else ''})"
            )
            if todo:
                print(f"        to do: {todo}")
        return 0

    known = {d["id"] for d in docs}
    if a.doc:
        if unknown := [i for i in a.doc if i not in known]:
            print(f"No document {', '.join(map(str, unknown))}", file=sys.stderr)
            return 1
        ids = a.doc
    else:
        ids = [d["id"] for d in docs if a.all or progress[d["id"]].ready]
    if not ids:
        print("Nothing to export.")
        return 0

    failed = 0
    for doc_id in ids:
        try:
            e = export_document(conn, settings.library_dir, doc_id, settings.ocr.review_below)
        except ValueError as err:
            failed += 1
            print(f"  {doc_id}: not exported: {err}")
            continue
        notes = [f"{len(e.pages)} pages"]
        if e.to_review:
            notes.append(f"{len(e.to_review)} not yet reviewed")
        if e.unplaced:
            notes.append(f"{len(e.unplaced)} with text not over the writing")
        print(f"  {doc_id}: {e.pdf_path} ({'; '.join(notes)})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
