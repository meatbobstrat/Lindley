"""Write the assembler's decisions to the database. Every change Lindley makes is logged."""

from __future__ import annotations

import json
import sqlite3

from lindley.assembler.evidence import SOURCE, Link
from lindley.assembler.model import Group, Page
from lindley.assembler.place import Candidate

HINT_KINDS = ("add_to_document", "set_aside", "group_pages")


def _log(conn: sqlite3.Connection, action: str, doc_id: int, after: dict) -> None:
    conn.execute(
        "INSERT INTO history (actor, action, target_type, target_id, after)"
        " VALUES ('lindley', ?, 'document', ?, ?)",
        (action, doc_id, json.dumps(after)),
    )


def save_clues(conn: sqlite3.Connection, pages: list[Page]) -> None:
    """Replace the rule-made facts for these pages with fresh ones."""
    ids = [p.id for p in pages]
    conn.executemany("DELETE FROM facts WHERE page_id = ? AND source = 'rule'", [(i,) for i in ids])
    conn.executemany(
        "INSERT INTO facts (page_id, kind, value, norm_value, confidence, source, source_detail)"
        " VALUES (?, ?, ?, ?, ?, 'rule', ?)",
        [(p.id, k, v, n, c, SOURCE) for p in pages for k, v, n, c in p.clues.facts()],
    )


def save_links(
    conn: sqlite3.Connection, page_ids: list[int], links: list[tuple[int, int, Link]]
) -> None:
    """Replace this assembler's evidence for these pages. One row per pair and relation."""
    conn.executemany(
        "DELETE FROM page_links WHERE source = ? AND (page_a = ? OR page_b = ?)",
        [(SOURCE, i, i) for i in page_ids],
    )
    merged: dict[tuple[int, int, str], tuple[float, list[str]]] = {}
    for a, b, link in links:
        key = (min(a, b), max(a, b), link.relation)
        score, notes = merged.get(key, (0.0, []))
        merged[key] = (
            max(score, link.score),
            notes + [link.note] if link.note not in notes else notes,
        )
    conn.executemany(
        "INSERT OR REPLACE INTO page_links (page_a, page_b, relation, score, evidence, source)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [(a, b, r, s, json.dumps(n), SOURCE) for (a, b, r), (s, n) in merged.items()],
    )


def create_document(conn: sqlite3.Connection, g: Group) -> int:
    doc_id = conn.execute(
        "INSERT INTO documents (name, name_source, origin, status, doc_type, doc_date, date_source,"
        " grouping_confidence, reasons)"
        " VALUES (?, 'lindley', 'lindley', 'progress', ?, ?, ?, ?, ?)",
        (
            g.name,
            g.kind,
            g.date,
            "lindley" if g.date else None,
            g.confidence,
            json.dumps(g.reasons),
        ),
    ).lastrowid
    conn.executemany(
        "UPDATE pages SET document_id = ?, position = ?, updated_at = datetime('now') WHERE id = ?",
        [(doc_id, i + 1, pid) for i, pid in enumerate(g.ids)],
    )
    _log(
        conn,
        "group_pages",
        doc_id,
        {
            "pages": g.ids,
            "confidence": g.confidence,
            "reasons": g.reasons,
            "checked_by_ai": g.by_ai,
        },
    )
    return doc_id


def attach(
    conn: sqlite3.Connection,
    doc_id: int,
    g: Group,
    at_end: bool,
    score: int,
    reasons: list[str],
    by_ai: bool = False,
) -> None:
    """Add a group's pages to the start or end of an untouched Lindley document. `by_ai`: the
    AI chose the document."""
    n = len(g.pages)
    if at_end:
        top = conn.execute(
            "SELECT COALESCE(MAX(position), 0) FROM pages WHERE document_id = ?", (doc_id,)
        ).fetchone()[0]
        rows = [(doc_id, top + i + 1, pid) for i, pid in enumerate(g.ids)]
    else:  # the new pages take the first places, whether a document counts from 0 or 1
        low = conn.execute(
            "SELECT COALESCE(MIN(position), 1) FROM pages WHERE document_id = ?", (doc_id,)
        ).fetchone()[0]
        conn.execute("UPDATE pages SET position = position + ? WHERE document_id = ?", (n, doc_id))
        rows = [(doc_id, low + i, pid) for i, pid in enumerate(g.ids)]
    conn.executemany(
        "UPDATE pages SET document_id = ?, position = ?, updated_at = datetime('now') WHERE id = ?",
        rows,
    )
    old = json.loads(
        conn.execute("SELECT reasons FROM documents WHERE id = ?", (doc_id,)).fetchone()[0] or "[]"
    )
    conn.execute(
        "UPDATE documents SET reasons = ?, updated_at = datetime('now') WHERE id = ?",
        (json.dumps(old + [r for r in reasons if r not in old]), doc_id),
    )
    _log(
        conn,
        "add_pages",
        doc_id,
        {
            "pages": g.ids,
            "at": "end" if at_end else "start",
            "confidence": score,
            "reasons": reasons,
            "checked_by_ai": by_ai,
        },
    )


def clear_hints(conn: sqlite3.Connection, page_ids: list[int]) -> None:
    """Drop Lindley's open hints for pages it is about to look at again."""
    conn.executemany(
        f"DELETE FROM suggestions WHERE status = 'open' AND page_id = ? AND kind IN {HINT_KINDS}",
        [(i,) for i in page_ids],
    )


def suggest(
    conn: sqlite3.Connection,
    kind: str,
    page_id: int,
    doc_id: int | None,
    confidence: int,
    reasons: list[str],
    payload: dict | None = None,
) -> bool:
    """Add a suggestion, unless a person already dismissed the same one. Pages proposed as a
    group are the same suggestion only with the same pages."""
    for (was,) in conn.execute(
        "SELECT payload FROM suggestions WHERE status = 'dismissed' AND kind = ? AND page_id = ?"
        " AND document_id IS ?",
        (kind, page_id, doc_id),
    ):
        pages = set((payload or {}).get("pages", []))
        if kind != "group_pages" or set(json.loads(was or "{}").get("pages", [])) == pages:
            return False
    conn.execute(
        "INSERT INTO suggestions (kind, page_id, document_id, payload, confidence, reasons)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (
            kind,
            page_id,
            doc_id,
            json.dumps(payload) if payload else None,
            confidence,
            json.dumps(reasons),
        ),
    )
    return True


def suggest_group(conn: sqlite3.Connection, g: Group, extra: dict | None = None) -> bool:
    """Ask a person whether these pages go together, as one document in this order. `extra`:
    more to keep with it, such as where else they may belong. `checked_by_ai`: the AI grouped
    them (api.suggestions offers those it was sure enough of for one-click accept). Its
    confidence is that the pages go together, whole document or not (Group.sure_together)."""
    payload = {"pages": g.ids, "name": g.name, "type": g.kind, "date": g.date}
    payload |= {"checked_by_ai": g.by_ai} | (extra or {})
    return suggest(conn, "group_pages", g.pages[0].id, None, g.sure_together, g.reasons, payload)


def save_needs_ai(
    conn: sqlite3.Connection,
    windows: list[list[Group]],
    placing: list[tuple[Group, list[Candidate]]] = (),
) -> None:
    """What's waiting for the sorting AI now: pages to sort into documents (`windows`), and
    pages to place in one of a few likely documents (`placing`, marked "question": "place",
    with the documents). Each guess carries the rules' reasons, for a person to read. A
    question still waiting keeps the time it started waiting; one no longer waiting (sorted,
    answered, or its pages gone) is dropped."""
    now = {
        json.dumps(sorted(p.id for g in w for p in g.pages)): json.dumps(
            [
                {
                    "pages": g.ids,
                    "name": g.name,
                    "confidence": g.confidence,
                    "reasons": g.reasons,
                }
                for g in w
            ]
        )
        for w in windows
    }
    for g, ranked in placing:
        now.setdefault(
            json.dumps(sorted(g.ids)),
            json.dumps(
                [
                    {
                        "pages": g.ids,
                        "name": g.name,
                        "confidence": g.confidence,
                        "reasons": g.reasons,
                        "question": "place",
                        "candidates": [c.payload() for c in ranked],
                    }
                ]
            ),
        )
    gone = [(k,) for (k,) in conn.execute("SELECT pages FROM needs_ai") if k not in now]
    conn.executemany("DELETE FROM needs_ai WHERE pages = ?", gone)
    conn.executemany(
        "INSERT INTO needs_ai (pages, proposal) VALUES (?, ?)"
        " ON CONFLICT (pages) DO UPDATE SET proposal = excluded.proposal",
        list(now.items()),
    )


def mark_matched(conn: sqlite3.Connection, scan_ids: set[int]) -> None:
    for sid in scan_ids:
        done = conn.execute(
            "UPDATE intake_steps"
            " SET status = 'done', engine_version = ?, finished_at = datetime('now')"
            " WHERE scan_id = ? AND page_id IS NULL AND step = 'match'",
            (SOURCE, sid),
        ).rowcount
        if not done:
            conn.execute(
                "INSERT INTO intake_steps"
                " (scan_id, step, status, engine_version, started_at, finished_at)"
                " VALUES (?, 'match', 'done', ?, datetime('now'), datetime('now'))",
                (sid, SOURCE),
            )
