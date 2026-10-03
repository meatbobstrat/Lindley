"""One pass of the assembler: Inbox pages in, Lindley documents and hints out.

Safe to run as often as you like; the worker runs it whenever new scans have settled.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from lindley.assembler import ai, apply
from lindley.assembler.answers import Answers
from lindley.assembler.evidence import pair
from lindley.assembler.model import Group, Page, weigh_terms
from lindley.assembler.segment import segment
from lindley.config import AssemblerSettings
from lindley.providers.base import ChatProvider

MAX_AI_PAGES = 40  # pages per AI question; larger tangles are left to the rules


@dataclass
class RunReport:
    considered: int = 0
    documents_created: int = 0
    pages_grouped: int = 0
    pages_added: int = 0
    hints: int = 0
    set_aside_hints: int = 0
    ai_calls: int = 0
    ai_reused: int = 0  # questions answered from the AI's earlier replies, with no call
    # What the rules left for the AI, whether or not one was asked: windows and their pages
    ai_windows: int = 0
    ai_pages: int = 0
    ai_rejected: list[str] = field(default_factory=list)
    inbox_left: int = 0


@dataclass
class DocEnds:
    id: int
    name: str
    first: Page
    last: Page
    touched: bool  # a person has named, changed or reviewed it: Lindley only suggests
    page_ids: frozenset[int] = frozenset()


_PAGE_SQL = """
SELECT p.id, p.scan_id, p.page_index, p.width_px, p.height_px, p.blank_score, p.phash,
       p.paper_color, p.detected_rotation, p.user_rotation,
       s.original_name, s.scanned_at, s.imported_at, t.text, t.words
FROM pages p
JOIN scans s ON s.id = p.scan_id
JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1
"""


def _copies(conn: sqlite3.Connection) -> dict[int, frozenset[int]]:
    """For each page, the pages that look like it scanned again (open same-page duplicates)."""
    out: dict[int, set[int]] = {}
    for a, b in conn.execute(
        "SELECT page_a, page_b FROM duplicates WHERE status = 'open' AND kind = 'same_page'"
    ):
        out.setdefault(a, set()).add(b)
        out.setdefault(b, set()).add(a)
    return {k: frozenset(v) for k, v in out.items()}


def _page(r: sqlite3.Row, copies: dict[int, frozenset[int]] | None = None) -> Page:
    return Page(
        r["id"],
        r["scan_id"],
        r["original_name"],
        r["text"],
        r["page_index"],
        r["scanned_at"],
        r["imported_at"],
        json.loads(r["words"]) if r["words"] else None,
        _upright_height(r),
        r["blank_score"],
        r["phash"],
        r["paper_color"],
        (copies or {}).get(r["id"], frozenset()),
        _upright_width(r),
    )


def _sideways(r: sqlite3.Row) -> bool:
    """Word boxes are read from the page turned upright; a quarter turn swaps its sides."""
    return (r["detected_rotation"] + r["user_rotation"]) % 180 == 90


def _upright_height(r: sqlite3.Row) -> int | None:
    return r["width_px"] if _sideways(r) else r["height_px"]


def _upright_width(r: sqlite3.Row) -> int | None:
    return r["height_px"] if _sideways(r) else r["width_px"]


def load_inbox(conn: sqlite3.Connection) -> list[Page]:
    rows = conn.execute(
        _PAGE_SQL + " WHERE p.document_id IS NULL AND p.set_aside_at IS NULL AND s.status = 'read'"
    ).fetchall()
    copies = _copies(conn)
    return [_page(r, copies) for r in rows]


def load_open_documents(conn: sqlite3.Connection) -> list[DocEnds]:
    docs = []
    copies = _copies(conn)
    for d in conn.execute(
        """SELECT d.id, d.name, (d.origin = 'user' OR d.name_source = 'user' OR EXISTS (
               SELECT 1 FROM history h WHERE h.actor = 'user' AND h.target_type = 'document'
               AND h.target_id = d.id)) AS touched
           FROM documents d WHERE d.status = 'progress'"""
    ):
        rows = conn.execute(
            _PAGE_SQL + " WHERE p.document_id = ? ORDER BY p.position", (d["id"],)
        ).fetchall()
        if rows:
            docs.append(
                DocEnds(
                    d["id"],
                    d["name"],
                    _page(rows[0], copies),
                    _page(rows[-1], copies),
                    bool(d["touched"]),
                    frozenset(r["id"] for r in rows),
                )
            )
    return docs


def _ai_windows(
    groups: list[Group], pairs, ordered: list[Page], band: tuple[int, int]
) -> list[list[Group]]:
    """Sets of groups the AI should look at together: either side of an uncertain break, and
    groups whose order the rules couldn't settle."""
    where = {p.id: i for i, g in enumerate(groups) for p in g.pages}
    parent = list(range(len(groups)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    marked: set[int] = set()
    lo, hi = band
    for i, p in enumerate(pairs):
        a, b = where[ordered[i].id], where[ordered[i + 1].id]
        if lo <= p.score * 100 < hi and not (groups[a].set_aside or groups[b].set_aside):
            parent[find(a)] = find(b)
            marked |= {a, b}
    marked |= {i for i, g in enumerate(groups) if not g.order_settled}
    windows: dict[int, list[Group]] = {}
    for i in sorted(marked):
        windows.setdefault(find(i), []).append(groups[i])
    return [w for w in windows.values() if sum(len(g.pages) for g in w) <= MAX_AI_PAGES]


def _best_match(g: Group, docs: list[DocEnds]) -> tuple[DocEnds, bool, int, list[str]] | None:
    """The open document this group most likely continues or starts.

    Returns (document, whether the group goes at its end, score 0-100, reasons)."""
    best = None
    for d in docs:
        if any(p.copies & d.page_ids for p in g.pages):
            continue  # the document already holds a copy of one of these pages
        tries = []
        if not g.pages[0].clues.starts_doc and not d.last.clues.ends_doc:
            tries.append((True, pair(d.last, g.pages[0])))
        if not g.pages[-1].clues.ends_doc and not d.first.clues.starts_doc:
            tries.append((False, pair(g.pages[-1], d.first)))
        for at_end, p in tries:
            score = round(100 * p.score)
            if not best or score > best[2]:
                best = (
                    d,
                    at_end,
                    score,
                    [k.note for k in p.links if k.relation != "adjacent_file"]
                    or [k.note for k in p.links],
                )
    return best


def assemble(
    conn: sqlite3.Connection,
    cfg: AssemblerSettings | None = None,
    chat: ChatProvider | None = None,
    max_ai_calls: int | None = None,
) -> RunReport:
    """Sort the Inbox. With `chat`, the AI is asked about what the rules couldn't settle:
    passing it is the OK to call it (see lindley.providers.allowance), at most `max_ai_calls`
    times if given. Without it, the rules decide alone, helped only by what the AI already said
    about the same pages (lindley.assembler.answers), which costs nothing."""
    cfg = cfg or AssemblerSettings()
    answers = Answers(conn)

    def may_ask() -> ChatProvider | None:
        if chat is not None and (max_ai_calls is None or answers.calls < max_ai_calls):
            return chat
        return None

    report = RunReport()
    pages = load_inbox(conn)
    report.considered = len(pages)
    if not pages:
        return report

    docs = load_open_documents(conn)
    weigh_terms(pages + list({id(p): p for d in docs for p in (d.first, d.last)}.values()))
    groups, pairs, ordered = segment(pages)

    # The AI looks only at what the rules couldn't settle.
    windows = _ai_windows(groups, pairs, ordered, cfg.ai_band)
    report.ai_windows = len(windows)
    report.ai_pages = sum(len(g.pages) for w in windows for g in w)
    for window in windows:
        result = ai.refine(may_ask(), [p for g in window for p in g.pages], window, answers)
        if result.groups is None:
            if result.problem:
                report.ai_rejected.append(result.problem)
            continue
        gone = {id(g) for g in window}
        groups = [g for g in groups if id(g) not in gone] + result.groups

    links = [
        (a.id, b.id, k)
        for a, b, p in zip(ordered, ordered[1:], pairs, strict=False)
        for k in p.links
    ]

    with conn:
        apply.save_clues(conn, pages)
        apply.clear_hints(conn, [p.id for p in pages])

        def hint(d: DocEnds, score: int, why: list[str], g: Group) -> None:
            for p in g.pages:
                report.hints += apply.suggest(conn, "add_to_document", p.id, d.id, score, why)

        def place(g: Group, allow_new: bool) -> bool:
            """Add a group to an open document, or hint at one. False: still unplaced."""
            m = _best_match(g, docs)
            if m and m[2] >= cfg.group_at:
                d, at_end, score, why = m
                if d.touched:  # a person's document: suggest, never change it
                    hint(d, score, why, g)
                    return True
                apply.attach(conn, d.id, g, at_end, score, why)
                a, b = (d.last, g.pages[0]) if at_end else (g.pages[-1], d.first)
                links.extend((a.id, b.id, k) for k in pair(a, b).links)
                if at_end:
                    d.last = g.pages[-1]
                else:
                    d.first = g.pages[0]
                report.pages_added += len(g.pages)
                return True
            if allow_new and g.confidence >= cfg.group_at:
                return False
            if m and m[2] >= cfg.hint_at:
                hint(m[0], m[2], m[3], g)
                return True
            return False

        # 1. Pages that continue a document already open. 2. New documents. 3. Hints for the rest.
        waiting = []
        for g in groups:
            if g.set_aside:
                for p in g.pages:
                    report.set_aside_hints += apply.suggest(
                        conn, "set_aside", p.id, None, 70, g.reasons
                    )
            elif not place(g, allow_new=True):
                waiting.append(g)
        new = [g for g in waiting if g.confidence >= cfg.group_at]
        made = {id(g) for g in new}
        if guess := [g for g in new if g.name_is_guess]:
            for i, name in ai.suggest_names(may_ask(), guess, answers).items():
                guess[i].name, guess[i].name_is_guess = name, False
        for g in new:
            doc_id = apply.create_document(conn, g)
            docs.append(DocEnds(doc_id, g.name, g.pages[0], g.pages[-1], False))
            report.documents_created += 1
            report.pages_grouped += len(g.pages)
        for g in waiting:
            if id(g) not in made:
                place(g, allow_new=False)

        apply.save_links(conn, [p.id for p in pages], links)
        apply.mark_matched(conn, {p.scan_id for p in pages})
    report.inbox_left = report.considered - report.pages_grouped - report.pages_added
    report.ai_calls, report.ai_reused = answers.calls, answers.reused
    return report
