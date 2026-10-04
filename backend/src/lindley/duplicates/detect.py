"""Find pages that are the same page scanned twice, or that have very similar text.

A second scan with other settings (dpi, colour or grey, brightness, cropping) changes every pixel
and the file's hash, but not the words on the page. So pages are compared by their text:

- Candidates: each page keeps its SKETCH smallest letter 8-gram hashes in `text_sketch`. Pages
  that share MIN_SHARED of them are compared in full, so there's no all-pairs comparison.
- Confirmation: how many letter 8-grams the two texts share (Jaccard J, and containment C: the
  shorter text's share), and how many words match in order (R). OCR errors on old paper keep
  J well below 1 even for the same page, so the thresholds are modest; unrelated pages share
  almost nothing.

Pages with too little text (a note, a drawing, unread handwriting) are compared by a small
picture of their contents instead (worker.image.image_signature). That's never certain, so a
match is only ever "similar".

Two texts can be much alike yet not the same page: a sheet and a piece of it scanned on its own,
or a page and a retyped version with a paragraph added. Then their lengths differ, so the same
page also needs texts about as long (SAME_LENGTH); otherwise the pair is only "similar".

On 178 real typewritten scans: re-scans had J 0.35 to 0.59, R 0.64 to 0.83 and length ratios
of 0.96 or more. A page and part of it, or a longer version, had length ratios of 0.48 to 0.77.
Drafts and pasted-up pages sharing paragraphs had C 0.30 to 0.45. Unrelated pages had C 0.21
at most.
"""

from __future__ import annotations

import heapq
import json
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from functools import lru_cache
from hashlib import blake2b
from pathlib import Path

from lindley.worker.image import BLANK_AT, image_signature, signature_likeness

log = logging.getLogger(__name__)

SOURCE = "duplicates v1"
GRAM = 8  # letters per gram
SKETCH = 64  # hashes kept per page
MIN_SHARED = 4  # sketch hashes two pages must share to be compared in full
MIN_LETTERS = 200  # fewer than this and the text says too little; the image is compared
SAME_J, SAME_R = 0.30, 0.60  # either: the same page...
SAME_LENGTH = 0.85  # ...if the texts are about as long; otherwise one holds part of the other
SIMILAR_J, SIMILAR_C = 0.15, 0.30  # either: very similar text
IMAGE_SIMILAR = 0.90  # signature likeness for pages with little text


@dataclass(frozen=True)
class Match:
    kind: str  # 'same_page' or 'similar'
    score: int  # 0-100
    evidence: dict


@dataclass
class DuplicateReport:
    checked: int = 0  # pages checked this time
    found: list[tuple[int, int, Match]] = field(default_factory=list)  # new pairs


def letters(text: str) -> str:
    return re.sub(r"[^a-z]", "", text.lower())


def _words(text: str) -> list[str]:
    return [w for w in re.findall(r"[a-z]+", text.lower()) if len(w) >= 3]


@lru_cache(maxsize=4096)
def _grams(text: str) -> frozenset[str]:
    s = letters(text)
    return frozenset(s[i : i + GRAM] for i in range(len(s) - GRAM + 1))


def _hash(gram: str) -> int:
    return int.from_bytes(blake2b(gram.encode(), digest_size=8).digest(), "big") >> 1  # 63 bits


def sketch(text: str) -> list[int]:
    """The page's SKETCH smallest gram hashes: pages that share several have text in common."""
    return heapq.nsmallest(SKETCH, {_hash(g) for g in _grams(text)})


def likeness(a: str, b: str, least: int = MIN_LETTERS) -> float | None:
    """How alike two texts are, 0-1: the share of letter grams they have in common, or of
    words that match in order, whichever is more. None when either has fewer than `least`
    letters."""
    if min(len(letters(a)), len(letters(b))) < least:
        return None
    ga, gb = _grams(a), _grams(b)
    j = len(ga & gb) / len(ga | gb)
    return max(j, SequenceMatcher(None, _words(a), _words(b), autojunk=False).ratio())


def compare_text(a: str, b: str) -> Match | None:
    if min(len(letters(a)), len(letters(b))) < MIN_LETTERS:
        return None
    ga, gb = _grams(a), _grams(b)
    shared = len(ga & gb)
    j = shared / len(ga | gb)
    c = shared / min(len(ga), len(gb))
    r = SequenceMatcher(None, _words(a), _words(b), autojunk=False).ratio()
    length = min(len(letters(a)), len(letters(b))) / max(len(letters(a)), len(letters(b)))
    measures = {
        "letters_shared": round(j, 3),
        "contained": round(c, 3),
        "words_in_order": round(r, 3),
        "length_ratio": round(length, 3),
    }
    alike = j >= SAME_J or r >= SAME_R
    if alike and length >= SAME_LENGTH:
        reasons = [f"{round(r * 100)}% of the words match, in the same order"]
        if c >= 0.5:
            reasons.append(f"{round(c * 100)}% of the text appears on both")
        return Match("same_page", round(100 * max(r, c)), {**measures, "reasons": reasons})
    if alike:  # much the same words, but one page has a lot more
        reasons = [
            f"{round(c * 100)}% of the shorter page's text is also on the other, which has more",
            "The shorter page may be part of the other, or an earlier or later version",
        ]
        return Match("similar", round(100 * c), {**measures, "reasons": reasons})
    if j >= SIMILAR_J or c >= SIMILAR_C:
        reasons = [
            f"{round(c * 100)}% of the shorter page's text is also on the other",
            "The wording differs in places: perhaps another draft or a copy with changes",
        ]
        return Match("similar", round(100 * c), {**measures, "reasons": reasons})
    return None


def compare_images(a: bytes, b: bytes) -> Match | None:
    likeness = signature_likeness(a, b)
    if likeness < IMAGE_SIMILAR:
        return None
    reasons = [
        "The two scans look alike",
        "There's too little text to compare, so check them side by side",
    ]
    return Match(
        "similar", round(100 * likeness), {"image_likeness": round(likeness, 3), "reasons": reasons}
    )


_PAGES = """
SELECT p.id, p.image_path, p.blank_score, p.detected_rotation, p.user_rotation,
       t.id AS tid, t.text, c.transcription_id AS checked, c.image_sig
FROM pages p
JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1
LEFT JOIN duplicate_checks c ON c.page_id = p.id
WHERE NOT (p.set_aside_at IS NOT NULL AND EXISTS (
    SELECT 1 FROM duplicates d WHERE d.status = 'resolved' AND d.kept_page != p.id
    AND (d.page_a = p.id OR d.page_b = p.id)))
"""


def find_duplicates(conn: sqlite3.Connection) -> DuplicateReport:
    """Check every page whose current reading hasn't been checked yet against all the others.

    Copies already set aside as duplicates are left out. Blank pages are never compared. A pair
    a person has decided on is kept as it is (it's never raised again).
    """
    report = DuplicateReport()
    rows = {r["id"]: r for r in conn.execute(_PAGES)}
    pages = {i: r for i, r in rows.items() if (r["blank_score"] or 0) < BLANK_AT}
    fresh = [i for i, r in pages.items() if r["checked"] != r["tid"]]
    sigs = {i: r["image_sig"] for i, r in pages.items() if r["image_sig"]}
    for i in fresh:
        r = pages[i]
        sig = None
        if len(letters(r["text"])) < MIN_LETTERS and r["image_path"]:
            rotation = ((r["detected_rotation"] or 0) + (r["user_rotation"] or 0)) % 360
            try:
                sig = image_signature(Path(r["image_path"]), rotation)
            except OSError as e:  # the image has gone: compare what text there is
                log.warning("Page %d's image couldn't be opened: %s", i, e)
        if sig is not None:
            sigs[i] = sig
        else:
            sigs.pop(i, None)
        with conn:
            conn.execute("DELETE FROM text_sketch WHERE page_id = ?", (i,))
            if sig is None:
                conn.executemany(
                    "INSERT INTO text_sketch (page_id, h) VALUES (?, ?)",
                    [(i, h) for h in sketch(r["text"])],
                )
            conn.execute(
                "INSERT INTO duplicate_checks (page_id, transcription_id, image_sig, checked_at)"
                " VALUES (?, ?, ?, datetime('now')) ON CONFLICT (page_id) DO UPDATE SET"
                " transcription_id = excluded.transcription_id, image_sig = excluded.image_sig,"
                " checked_at = excluded.checked_at",
                (i, r["tid"], sig),
            )
        report.checked += 1

    compared: set[tuple[int, int]] = set()
    for i in fresh:
        if i in sigs:
            others = [(j, compare_images(sigs[i], sigs[j])) for j in sigs if j != i]
        else:
            others = [
                (j, compare_text(pages[i]["text"], pages[j]["text"]))
                for j in _candidates(conn, i, pages)
            ]
        for j, match in others:
            pair = (min(i, j), max(i, j))
            if pair in compared or match is None:
                continue
            compared.add(pair)
            added = conn.execute(
                "INSERT OR IGNORE INTO duplicates (page_a, page_b, kind, score, evidence)"
                " VALUES (?, ?, ?, ?, ?)",
                (*pair, match.kind, match.score, json.dumps(match.evidence)),
            ).rowcount
            if added:
                report.found.append((*pair, match))
        conn.commit()
    return report


def _candidates(conn: sqlite3.Connection, page_id: int, pages: dict) -> list[int]:
    rows = conn.execute(
        "SELECT o.page_id FROM text_sketch s JOIN text_sketch o ON o.h = s.h"
        " WHERE s.page_id = ? AND o.page_id != ? GROUP BY o.page_id HAVING COUNT(*) >= ?",
        (page_id, page_id, MIN_SHARED),
    )
    return [r[0] for r in rows if r[0] in pages]
