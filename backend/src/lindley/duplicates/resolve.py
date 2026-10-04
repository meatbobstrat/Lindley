"""Duplicates a person decides on: which copy to keep, or that the pages aren't copies at all.

Open pairs that share a page form a set (three scans of one page are one set). When every copy
in a set lies in a document, sets falling in the same two documents make a document pair: that
document was scanned twice.

Keeping a copy never deletes anything. The kept scan takes the best place any copy has (a
document position, else the Inbox) and the other copies are set aside, marked as duplicates.
Every change is written to `history` with its before and after, one batch per decision, so
lindley.history.undo can put it all back.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from lindley import history
from lindley.organise import close_gaps, move_page, remove_if_empty

KIND_ORDER = {"same_page": 0, "similar": 1}


@dataclass
class Copy:
    page_id: int
    file_name: str
    imported_at: str
    dpi: int | None
    width: int | None
    height: int | None
    color_mode: str | None
    file_size: int | None
    confidence: float | None
    text: str
    corrected: bool  # a person corrected or confirmed the text
    where: str  # 'inbox', 'document' or 'aside'
    document_id: int | None = None
    document_name: str | None = None
    position: int | None = None
    document_touched: bool = False  # a person named or edited that document

    @property
    def pixels(self) -> int:
        return (self.width or 0) * (self.height or 0)


@dataclass
class DuplicateSet:
    id: int  # the set's first pair: stable while the set is open
    kind: str  # 'same_page' if any pair is, else 'similar'
    score: int
    pair_ids: list[int]
    copies: list[Copy]
    reasons: list[str]  # why these look like copies
    suggested: int  # the page Lindley would keep
    why: list[str]  # why that one


@dataclass
class Decision:
    """What a decision changed. `batch` is what lindley.history.undo takes to undo it."""

    batch: int
    set_aside: list[int] = field(default_factory=list)
    sets: int = 1


@dataclass
class DocumentPair:
    documents: tuple[int, int]
    names: tuple[str, str]
    set_ids: list[int]
    extra: dict[int, list[int]] = field(default_factory=dict)  # pages only one document has


_COPY_SQL = """
SELECT p.id, s.original_name, s.imported_at, p.dpi, p.width_px, p.height_px, p.color_mode,
       s.file_size, t.confidence, t.text,
       (t.source = 'user' OR t.confirmed_at IS NOT NULL) AS corrected,
       p.document_id, p.position, p.set_aside_at, d.name AS document_name,
       (d.origin = 'user' OR d.name_source = 'user' OR EXISTS (
           SELECT 1 FROM history h WHERE h.actor = 'user' AND h.target_type = 'document'
           AND h.target_id = d.id)) AS touched
FROM pages p
JOIN scans s ON s.id = p.scan_id
LEFT JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1
LEFT JOIN documents d ON d.id = p.document_id
WHERE p.id = ?
"""


def _copy(conn: sqlite3.Connection, page_id: int) -> Copy:
    r = conn.execute(_COPY_SQL, (page_id,)).fetchone()
    where = "document" if r["document_id"] else "aside" if r["set_aside_at"] else "inbox"
    return Copy(
        r["id"],
        r["original_name"],
        r["imported_at"],
        r["dpi"],
        r["width_px"],
        r["height_px"],
        r["color_mode"],
        r["file_size"],
        r["confidence"],
        r["text"] or "",
        bool(r["corrected"]),
        where,
        r["document_id"],
        r["document_name"],
        r["position"],
        bool(r["touched"]),
    )


def open_sets(conn: sqlite3.Connection) -> list[DuplicateSet]:
    """Every open duplicate set, strongest first.

    Copies of one page chain together (three scans of a page are one set). A "similar" pair
    never does: a sheet and a piece of it aren't copies of each other's copies, so each similar
    pair is a decision of its own. A page can be in both kinds of set.
    """
    pairs = conn.execute(
        "SELECT id, page_a, page_b, kind, score, evidence FROM duplicates WHERE status = 'open'"
        " ORDER BY id"
    ).fetchall()
    same = [p for p in pairs if p["kind"] == "same_page"]
    sets = [_make_set(conn, [p]) for p in pairs if p["kind"] != "same_page"]
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for p in same:
        parent[find(p["page_a"])] = find(p["page_b"])
    groups: dict[int, list[sqlite3.Row]] = {}
    for p in same:
        groups.setdefault(find(p["page_a"]), []).append(p)
    sets += [_make_set(conn, ps) for ps in groups.values()]
    return sorted(sets, key=lambda s: (KIND_ORDER[s.kind], -s.score, s.id))


def _make_set(conn: sqlite3.Connection, pairs: list[sqlite3.Row]) -> DuplicateSet:
    page_ids = sorted({p["page_a"] for p in pairs} | {p["page_b"] for p in pairs})
    copies = [_copy(conn, i) for i in page_ids]
    strongest = max(pairs, key=lambda p: (KIND_ORDER[p["kind"]] == 0, p["score"]))
    kind = "same_page" if any(p["kind"] == "same_page" for p in pairs) else "similar"
    suggested, why = suggest_keep(copies)
    return DuplicateSet(
        pairs[0]["id"],
        kind,
        round(strongest["score"]),
        [p["id"] for p in pairs],
        copies,
        json.loads(strongest["evidence"] or "{}").get("reasons", []),
        suggested,
        why,
    )


def get_set(conn: sqlite3.Connection, set_id: int) -> DuplicateSet | None:
    return next((s for s in open_sets(conn) if s.id == set_id), None)


def document_pairs(sets: list[DuplicateSet], conn: sqlite3.Connection) -> list[DocumentPair]:
    """Sets whose copies all lie in the same two documents: that document was scanned twice."""
    found: dict[tuple[int, int], DocumentPair] = {}
    for s in sets:
        if s.kind != "same_page" or any(c.where != "document" for c in s.copies):
            continue
        docs = sorted({c.document_id for c in s.copies})
        if len(docs) != 2:
            continue
        key = (docs[0], docs[1])
        if key not in found:
            names = {c.document_id: c.document_name for c in s.copies}
            found[key] = DocumentPair(key, (names[key[0]], names[key[1]]), [])
        found[key].set_ids.append(s.id)
    for key, dp in found.items():
        matched = {c.page_id for s in sets if s.id in dp.set_ids for c in s.copies}
        for doc in key:
            ids = [
                r[0]
                for r in conn.execute(
                    "SELECT id FROM pages WHERE document_id = ? ORDER BY position", (doc,)
                )
            ]
            dp.extra[doc] = [i for i in ids if i not in matched]
    return list(found.values())


def suggest_keep(copies: list[Copy]) -> tuple[int, list[str]]:
    """The copy Lindley would keep, and why. A person decides.

    The kept copy takes the best place any copy has, so where a copy is matters only as a tie
    break: text a person checked, then the clearer reading (in steps of 5%), then the bigger scan
    (in whole megapixels), then colour.
    """

    def rank(c: Copy) -> tuple:
        return (
            c.corrected,
            round((c.confidence or 0) / 5),
            c.pixels // 1_000_000,
            c.color_mode == "rgb",
            c.where == "document",
            -_order(c, copies),
        )

    ranked = sorted(copies, key=rank, reverse=True)
    best, rest = ranked[0], ranked[1:]
    why: list[str] = []
    if best.where == "document" and all(c.where != "document" for c in rest):
        why.append(f"It's already in {quoted(best.document_name or '')}")
    if best.corrected and not any(c.corrected for c in rest):
        why.append("You've checked its text")
    others_conf = [c.confidence for c in rest if c.confidence is not None]
    if best.confidence is not None and others_conf and best.confidence >= max(others_conf) + 3:
        why.append(f"Read with {best.confidence:.0f}% confidence, against {max(others_conf):.0f}%")
    other_px = max((c.pixels for c in rest), default=0)
    if best.pixels and other_px and best.pixels >= other_px * 1.1:
        runner = max(rest, key=lambda c: c.pixels)
        if best.dpi and runner.dpi and best.dpi != runner.dpi:
            why.append(f"Sharper scan: {best.dpi} dpi, against {runner.dpi}")
        else:
            why.append(
                f"Bigger scan: {best.width}×{best.height}, against {runner.width}×{runner.height}"
            )
    if best.color_mode == "rgb" and all(c.color_mode != "rgb" for c in rest):
        why.append("Scanned in colour")
    if not why:
        why.append(
            "Imported first" if _order(best, copies) == 0 else "The copies look equally good"
        )
    why = why[:3]
    home = next((c for c in rest if c.where == "document"), None)
    if best.where != "document" and home:
        why.append(f"Keeping it puts it in its place in {quoted(home.document_name or '')}")
    return best.page_id, why


def quoted(name: str) -> str:
    """A name in quotes, unless it already has some (Lindley's guesses can)."""
    return name if "“" in name else f"“{name}”"


def _order(c: Copy, copies: list[Copy]) -> int:
    return sorted(copies, key=lambda x: (x.imported_at, x.page_id)).index(c)


# ---------------------------------------------------------------- Decisions (a person's)


def keep(conn: sqlite3.Connection, set_id: int, page_id: int, batch: int | None = None) -> Decision:
    """Keep one copy; set the others aside."""
    s = get_set(conn, set_id)
    if s is None:
        raise LookupError(f"No open duplicate set {set_id}")
    kept = next((c for c in s.copies if c.page_id == page_id), None)
    if kept is None:
        raise ValueError(f"Page {page_id} isn't one of the copies")
    others = [c for c in s.copies if c.page_id != page_id]
    # The kept scan takes the best place: its own document position, or another copy's.
    home = (
        kept
        if kept.where == "document"
        else next(
            (
                c
                for c in sorted(others, key=lambda c: not c.document_touched)
                if c.where == "document"
            ),
            None,
        )
    )
    with conn:
        batch = batch or history.new_batch(conn)
        emptied: set[int] = set()
        for c in others:
            move_page(conn, batch, c.page_id, None, None, aside=True, action="set_aside_duplicate")
            if c.document_id and c is not home:
                emptied.add(c.document_id)
        if home is not kept and home is not None:
            move_page(
                conn,
                batch,
                page_id,
                home.document_id,
                home.position,
                aside=False,
                action="replace_with_duplicate",
            )
        elif kept.where == "aside" and any(c.where == "inbox" for c in others):
            move_page(conn, batch, page_id, None, None, aside=False, action="return_duplicate")
        for doc in emptied:
            close_gaps(conn, batch, doc)
        _decide(conn, batch, "keep_duplicate", set_id, s.pair_ids, "resolved", page_id)
        for doc in emptied:
            remove_if_empty(conn, batch, doc)
    return Decision(batch, [c.page_id for c in others])


def keep_document(conn: sqlite3.Connection, keep_doc: int, other_doc: int) -> Decision:
    """Keep one document of a pair scanned twice: in every set they share, the copy in
    `keep_doc` is kept. Pages only the other document has stay where they are. It's one
    decision, undone as one."""
    sets = [
        s for s in open_sets(conn) if {c.document_id for c in s.copies} >= {keep_doc, other_doc}
    ]
    decision = Decision(history.new_batch(conn), [], len(sets))
    for s in sets:
        kept = next(c for c in s.copies if c.document_id == keep_doc)
        decision.set_aside += keep(conn, s.id, kept.page_id, decision.batch).set_aside
    return decision


def not_duplicates(conn: sqlite3.Connection, set_id: int) -> Decision:
    """The pages only look alike: keep them all, and never raise them again."""
    s = get_set(conn, set_id)
    if s is None:
        raise LookupError(f"No open duplicate set {set_id}")
    with conn:
        batch = history.new_batch(conn)
        _decide(conn, batch, "not_duplicates", set_id, s.pair_ids, "not_duplicate", None)
    return Decision(batch)


def duplicate_of(conn: sqlite3.Connection, page_id: int) -> int | None:
    """For a copy set aside as a duplicate: the page that was kept instead."""
    r = conn.execute(
        "SELECT kept_page FROM duplicates WHERE status = 'resolved' AND kept_page != ?"
        " AND (page_a = ? OR page_b = ?) ORDER BY resolved_at DESC LIMIT 1",
        (page_id, page_id, page_id),
    ).fetchone()
    return r[0] if r else None


def _decide(
    conn: sqlite3.Connection,
    batch: int,
    action: str,
    set_id: int,
    pair_ids: list[int],
    status: str,
    kept: int | None,
) -> None:
    """Close a set's pairs, recording each pair's state before and after."""

    def states() -> list[dict]:
        return [
            dict(
                conn.execute(
                    "SELECT id, status, kept_page, resolved_at FROM duplicates WHERE id = ?", (i,)
                ).fetchone()
            )
            for i in pair_ids
        ]

    before = states()
    conn.executemany(
        "UPDATE duplicates SET status = ?, kept_page = ?, resolved_at = datetime('now')"
        " WHERE id = ?",
        [(status, kept, i) for i in pair_ids],
    )
    history.log(conn, batch, action, "duplicate_set", set_id, before, states())
