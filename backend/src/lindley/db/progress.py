"""How close a document is to complete. Worked out from the data each time, never stored."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


@dataclass
class Check:
    key: str
    label: str
    done: bool
    detail: str = ""


@dataclass
class Progress:
    document_id: int
    checks: list[Check] = field(default_factory=list)

    @property
    def done(self) -> int:
        return sum(c.done for c in self.checks)

    @property
    def total(self) -> int:
        return len(self.checks)

    @property
    def ready(self) -> bool:
        """Everything but the export itself is done: Lindley can suggest exporting."""
        return all(c.done for c in self.checks if c.key != "exported")


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


def pages_to_review(conn: sqlite3.Connection, doc_id: int, review_below: float) -> list[int]:
    """A document's pages whose current reading waits for a person's review, in order."""
    return [
        r[0]
        for r in conn.execute(
            "SELECT p.id FROM pages p JOIN v_current_text c ON c.page_id = p.id"
            " WHERE p.document_id = ? AND NOT c.reviewed AND c.confidence < ?"
            " ORDER BY p.position",
            (doc_id, review_below),
        )
    ]


def document_progress(conn: sqlite3.Connection, doc_id: int, review_below: float) -> Progress:
    """Checklist for one document. `review_below` is the review threshold from settings."""
    doc = conn.execute(
        "SELECT name_source, doc_date, doc_type FROM documents WHERE id = ?", (doc_id,)
    ).fetchone()
    if doc is None:
        raise KeyError(f"No document {doc_id}")

    pages = conn.execute(
        """
        SELECT p.id, s.status AS scan_status, c.page_id IS NOT NULL AS has_text
        FROM pages p
        JOIN scans s ON s.id = p.scan_id
        LEFT JOIN v_current_text c ON c.page_id = p.id
        WHERE p.document_id = ?
        """,
        (doc_id,),
    ).fetchall()
    unread = sum(1 for p in pages if p["scan_status"] != "read" or not p["has_text"])
    to_review = len(pages_to_review(conn, doc_id, review_below))
    reorder_open = conn.execute(
        "SELECT COUNT(*) FROM suggestions"
        " WHERE document_id = ? AND kind = 'reorder' AND status = 'open'",
        (doc_id,),
    ).fetchone()[0]
    exported = conn.execute(
        "SELECT MAX(exported_at) FROM exports WHERE document_id = ?", (doc_id,)
    ).fetchone()[0]

    p = Progress(doc_id)
    p.checks = [
        Check("has_pages", "Has pages", bool(pages), _plural(len(pages), "page")),
        Check(
            "read",
            "Every page read",
            bool(pages) and not unread,
            f"{_plural(unread, 'page')} still being read" if unread else "",
        ),
        Check(
            "reviewed",
            "Nothing waiting for your review",
            not to_review,
            f"{_plural(to_review, 'page')} to review" if to_review else "",
        ),
        Check("named", "Named or name accepted by you", doc["name_source"] == "user"),
        Check("dated", "Date known", bool(doc["doc_date"]), doc["doc_date"] or ""),
        Check("typed", "Type known", bool(doc["doc_type"]), doc["doc_type"] or ""),
        Check(
            "ordered",
            "Page order settled",
            not reorder_open,
            "Lindley suggests a different order" if reorder_open else "",
        ),
        Check("exported", "Exported as a PDF", exported is not None, exported or ""),
    ]
    return p
