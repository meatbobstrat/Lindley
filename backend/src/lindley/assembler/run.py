"""One pass of the assembler: Inbox pages in, Lindley documents and hints out.

Safe to run as often as you like; the worker runs it whenever new scans have settled.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import PurePath

from lindley import history
from lindley.assembler import ai, apply, evidence, relearn
from lindley.assembler.answers import Answers
from lindley.assembler.evidence import pair
from lindley.assembler.model import Group, Page, weigh_terms
from lindley.assembler.place import Candidate, DocEnds, candidates
from lindley.assembler.segment import segment
from lindley.assembler.terms import Library
from lindley.config import AssemblerSettings
from lindley.duplicates.resolve import quoted
from lindley.providers.base import ChatProvider

MAX_AI_PAGES = 40  # pages per AI question; larger tangles are left to the rules
_ONE_AT_A_TIME = threading.Lock()  # the watcher and a person's request may both assemble


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
    ai_waiting: int = 0  # questions left waiting for the AI (needs_ai)
    ai_placed: int = 0  # pages added to a document the AI chose from a few likely ones
    ai_rejected: list[str] = field(default_factory=list)
    inbox_left: int = 0


_PAGE_SQL = """
SELECT p.id, p.scan_id, p.page_index, p.width_px, p.height_px, p.blank_score, p.phash,
       p.paper_color, p.detected_rotation, p.user_rotation, p.dpi, p.color_mode, p.script,
       p.created_at AS added_at,
       s.original_name, s.source_path, s.scanned_at, s.file_modified_at, s.imported_at,
       t.text, t.words, t.confidence
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


def _copy_scores(conn: sqlite3.Connection) -> dict[int, int]:
    """For each page that looks scanned again, how alike its likeliest copy is, 0-100."""
    out: dict[int, int] = {}
    for a, b, score in conn.execute(
        "SELECT page_a, page_b, score FROM duplicates WHERE status = 'open' AND kind = 'same_page'"
    ):
        for p in (a, b):
            out[p] = max(out.get(p, 0), round(score))
    return out


def _set_aside_confidence(p: Page, copy_scores: dict[int, int]) -> int:
    """How sure Lindley is that a page can be set aside: as sure as it is that the page is
    another scan of one (a same-page duplicate's score), or that it's blank. A stray note: 70."""
    if p.copies and p.id in copy_scores:
        return copy_scores[p.id]
    if p.clues.kind == "blank" and p.blank_score is not None:
        return round(100 * p.blank_score)
    return 70


def folder_sizes(conn: sqlite3.Connection) -> Counter[str]:
    """How many pages Lindley holds from each folder scans were found in."""
    out: Counter[str] = Counter()
    for path, n in conn.execute(
        "SELECT s.source_path, COUNT(*) FROM pages p JOIN scans s ON s.id = p.scan_id GROUP BY s.id"
    ):
        out[str(PurePath(path).parent)] += n
    return out


def _page(
    r: sqlite3.Row,
    copies: dict[int, frozenset[int]] | None = None,
    sizes: Counter[str] | None = None,
) -> Page:
    folder = str(PurePath(r["source_path"]).parent)
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
        r["dpi"],
        r["color_mode"],
        r["script"],
        r["confidence"],
        r["file_modified_at"],
        r["added_at"],
        folder,
        (sizes or {}).get(folder, 0),
        sizes.total() if sizes else 0,
    )


def _sideways(r: sqlite3.Row) -> bool:
    """Word boxes are read from the page turned upright; a quarter turn swaps its sides."""
    return (r["detected_rotation"] + r["user_rotation"]) % 180 == 90


def _upright_height(r: sqlite3.Row) -> int | None:
    return r["width_px"] if _sideways(r) else r["height_px"]


def _upright_width(r: sqlite3.Row) -> int | None:
    return r["height_px"] if _sideways(r) else r["width_px"]


def library_terms(conn: sqlite3.Connection) -> Library:
    """How many pages use each word, over every page read so far."""
    return Library.of(t for (t,) in conn.execute("SELECT text FROM v_current_text"))


def _filed(conn: sqlite3.Connection) -> dict[int, str]:
    """Every page in a document, with where it is in words: "page 2 of “Letter”"."""
    return {
        r[0]: f"page {r[1]} of {quoted(r[2])}"
        for r in conn.execute(
            "SELECT p.id, p.position, d.name FROM pages p JOIN documents d ON d.id = p.document_id"
        )
    }


def load_inbox(conn: sqlite3.Connection) -> list[Page]:
    rows = conn.execute(
        _PAGE_SQL + " WHERE p.document_id IS NULL AND p.set_aside_at IS NULL AND s.status = 'read'"
    ).fetchall()
    copies, sizes, filed = _copies(conn), folder_sizes(conn), _filed(conn)
    pages = [_page(r, copies, sizes) for r in rows]
    for p in pages:
        p.filed_copy = next((filed[i] for i in sorted(p.copies) if i in filed), None)
    return pages


def load_open_documents(conn: sqlite3.Connection) -> list[DocEnds]:
    docs = []
    copies, sizes = _copies(conn), folder_sizes(conn)
    for d in conn.execute(
        "SELECT d.id, d.name, (d.origin = 'user' OR d.name_source = 'user' OR "
        + history.WORKED_ON_SQL
        + ") AS touched FROM documents d WHERE d.status = 'progress'"
    ):
        rows = conn.execute(
            _PAGE_SQL + " WHERE p.document_id = ? ORDER BY p.position", (d["id"],)
        ).fetchall()
        if rows:
            docs.append(
                DocEnds(
                    d["id"],
                    d["name"],
                    _page(rows[0], copies, sizes),
                    _page(rows[-1], copies, sizes),
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
    best = candidates(g, docs, top=1, floor=0)
    return (best[0].document, best[0].at_end, best[0].score, best[0].reasons) if best else None


def _listed(ranked: list[Candidate]) -> dict:
    """Where else the pages may belong, best first, kept with a hint for a person."""
    return {"candidates": [c.payload() for c in ranked]} if ranked else {}


def _declined(conn: sqlite3.Connection) -> set[int]:
    """Pages a person said don't go together as Lindley proposed: the AI isn't asked about
    them on its own."""
    out: set[int] = set()
    for (payload,) in conn.execute(
        "SELECT payload FROM suggestions WHERE kind = 'group_pages' AND status = 'dismissed'"
    ):
        out |= set(json.loads(payload or "{}").get("pages", []))
    return out


def assemble(
    conn: sqlite3.Connection,
    cfg: AssemblerSettings | None = None,
    chat: ChatProvider | None = None,
    max_ai_calls: int | None = None,
    asked: set[int] | None = None,
    weights: dict[str, float] | None = None,
) -> RunReport:
    """Sort the Inbox. With `chat`, the AI may be asked about what the rules couldn't settle:
    passing it is the OK to call it (see lindley.providers.allowance), at most `max_ai_calls`
    times if given. A person is asked first, with hints, so on its own the AI is only asked
    about pages that have waited `ask_ai_after_days` and that no one turned down. With `asked`,
    a person asked about those pages: only they are sent, at once. Without `chat`, the rules
    decide alone, helped by what the AI already said about the same pages
    (lindley.assembler.answers), which costs nothing.

    The evidence is weighed with `weights` if given, else with weights learned from people's
    answers (lindley.assembler.relearn), else with the shipped ones."""
    with _ONE_AT_A_TIME:
        evidence.use_weights(weights if weights is not None else relearn.learned(conn))
        return _assemble(conn, cfg or AssemblerSettings(), chat, max_ai_calls, asked)


def _assemble(
    conn: sqlite3.Connection,
    cfg: AssemblerSettings,
    chat: ChatProvider | None,
    max_ai_calls: int | None,
    asked: set[int] | None,
) -> RunReport:
    answers = Answers(conn)
    waited = (datetime.now(UTC) - timedelta(days=cfg.ask_ai_after_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    declined = _declined(conn) if chat is not None and asked is None else set()

    def may_ask(ps: list[Page]) -> ChatProvider | None:
        """The AI, if it may be called about these pages now."""
        if chat is None or (max_ai_calls is not None and answers.calls >= max_ai_calls):
            return None
        ids = {p.id for p in ps}
        if asked is not None:
            return chat if ids & asked else None
        if ids & declined or any((p.added_at or "") > waited for p in ps):
            return None
        return chat

    report = RunReport()
    pages = load_inbox(conn)
    report.considered = len(pages)
    if not pages:
        with conn:
            apply.save_needs_ai(conn, [])
        return report

    docs = load_open_documents(conn)
    weigh_terms(
        pages + list({id(p): p for d in docs for p in (d.first, d.last)}.values()),
        library_terms(conn),
    )
    groups, pairs, ordered = segment(pages)

    # The AI looks only at what the rules couldn't settle.
    windows = _ai_windows(groups, pairs, ordered, cfg.ai_band)
    report.ai_windows = len(windows)
    report.ai_pages = sum(len(g.pages) for w in windows for g in w)
    if asked:  # pages a person asked about, though the rules didn't think them uncertain
        sent = {p.id for w in windows for g in w for p in g.pages}
        more = [g for g in groups if not g.set_aside and set(g.ids) & (asked - sent)]
        if more and sum(len(g.pages) for g in more) <= MAX_AI_PAGES:
            windows.append(more)
    unasked: list[list[Group]] = []  # not asked, and no earlier answer: they wait for the AI
    for window in windows:
        ps = [p for g in window for p in g.pages]
        result = ai.refine(may_ask(ps), ps, window, answers)
        if result.groups is None:
            if result.problem:
                report.ai_rejected.append(result.problem)
            else:
                unasked.append(window)
            continue
        gone = {id(g) for g in window}
        groups = [g for g in groups if id(g) not in gone] + result.groups

    links = [
        (a.id, b.id, k)
        for a, b, p in zip(ordered, ordered[1:], pairs, strict=False)
        for k in p.links
    ]
    # And what joins pages of one group that weren't scanned one after the other
    scanned_next = {(a.id, b.id) for a, b in zip(ordered, ordered[1:], strict=False)}
    links += [
        (a.id, b.id, k)
        for g in groups
        for a, b in zip(g.pages, g.pages[1:], strict=False)
        if (a.id, b.id) not in scanned_next
        for k in pair(a, b).links
    ]

    with conn:
        apply.save_clues(conn, pages)
        apply.clear_hints(conn, [p.id for p in pages])

        # Groups that stay in the Inbox: where else a group's pages may belong
        others = [g for g in groups if not g.set_aside and g.confidence < cfg.group_at]

        def hint(d: DocEnds, at_end: bool, score: int, why: list[str], g: Group) -> None:
            where = {"pages": g.ids, "at": "end" if at_end else "start"}
            where |= _listed(candidates(g, docs, others))
            for p in g.pages:
                report.hints += apply.suggest(
                    conn, "add_to_document", p.id, d.id, score, why, where
                )

        def attach(
            d: DocEnds, at_end: bool, score: int, why: list[str], g: Group, by_ai=False
        ) -> None:
            if d.touched:  # a person's document: suggest, never change it
                hint(d, at_end, score, why, g)
                return
            apply.attach(conn, d.id, g, at_end, score, why, by_ai)
            a, b = (d.last, g.pages[0]) if at_end else (g.pages[-1], d.first)
            links.extend((a.id, b.id, k) for k in pair(a, b).links)
            if at_end:
                d.last = g.pages[-1]
            else:
                d.first = g.pages[0]
            report.pages_added += len(g.pages)

        def place(g: Group, first: bool) -> bool:
            """Add a group to an open document, or hint at one. False: still unplaced. `first`:
            only add it; whatever else goes waits for the AI's say and new documents."""
            m = _best_match(g, docs)
            if m and m[2] >= cfg.group_at:
                attach(*m, g)
                return True
            if first:
                return False
            if m and m[2] >= cfg.hint_at:
                hint(*m, g)
                return True
            return False

        # 1. Pages that continue a document already open. 2. New documents. 3. Hints for the rest.
        waiting = []
        copy_scores = _copy_scores(conn)
        for g in groups:
            if g.set_aside:
                for p in g.pages:
                    sure = _set_aside_confidence(p, copy_scores)
                    report.set_aside_hints += apply.suggest(
                        conn, "set_aside", p.id, None, sure, g.reasons
                    )
            elif not place(g, first=True):
                waiting.append(g)
        placing: list[tuple[Group, list[Candidate]]] = []  # questions waiting for the AI

        def check(g: Group, confident: bool = False) -> str | None:
            """When the rules aren't sure which open document g belongs to, ask the AI about the
            few likeliest, not every one. Returns "added" or "hinted" from its answer, "none" if
            it said none of them, or None when there's no answer and the rules decide. A
            question the AI may not be asked now waits for it in Needs AI.

            `confident`: the rules would make g a document of its own. Then only an answer that
            adds it to a document counts, and nothing waits: the pages leave the Inbox."""
            ranked = [c for c in candidates(g, docs) if c.document]
            if not ranked or not cfg.ai_band[0] <= ranked[0].score < cfg.group_at:
                return None
            q = ai.place(may_ask(g.pages), g, ranked, answers)
            if q.problem:
                report.ai_rejected.append(q.problem)
            elif not q.answered and not confident:
                placing.append((g, ranked))
            if q.choice is None:
                return "none" if q.answered and not q.problem else None
            c, why = q.choice, q.reasons or q.choice.reasons
            if q.confidence >= cfg.group_at and not c.document.touched:
                attach(c.document, c.at_end, q.confidence, why, g, by_ai=True)
                report.ai_placed += len(g.pages)
                return "added"
            if confident:
                return None
            if q.confidence >= cfg.hint_at:
                hint(c.document, c.at_end, q.confidence, why, g)
                return "hinted"
            return "none"

        # Confident on their own, but they may be part of a document already made: if the AI
        # may be asked, it's asked first, about the few likeliest.
        confident = [g for g in waiting if g.confidence >= cfg.group_at]
        made = {id(g) for g in confident}  # made a document, or added to one by the AI
        new = [g for g in confident if check(g, confident=True) != "added"]
        if guess := [g for g in new if g.name_is_guess]:
            asking = may_ask([p for g in guess for p in g.pages])
            for i, name in ai.suggest_names(asking, guess, answers).items():
                guess[i].name, guess[i].name_is_guess = name, False
        for g in new:
            doc_id = apply.create_document(conn, g)
            docs.append(DocEnds(doc_id, g.name, g.pages[0], g.pages[-1], False))
            report.documents_created += 1
            report.pages_grouped += len(g.pages)

        # What's left goes to a person first: "Do these go together?"
        for g in waiting:
            if id(g) in made:
                continue
            said = check(g)
            if said == "hinted" or said == "added":
                continue
            if said is None and place(g, first=False):
                continue
            if len(g.pages) > 1 and g.confidence >= cfg.hint_at:
                report.hints += apply.suggest_group(conn, g, _listed(candidates(g, docs, others)))

        apply.save_links(conn, [p.id for p in pages], links)
        apply.mark_matched(conn, {p.scan_id for p in pages})
        apply.save_needs_ai(conn, unasked, placing)
        report.ai_waiting = len(unasked) + len(placing)
    report.inbox_left = report.considered - report.pages_grouped - report.pages_added
    report.ai_calls, report.ai_reused = answers.calls, answers.reused
    return report
