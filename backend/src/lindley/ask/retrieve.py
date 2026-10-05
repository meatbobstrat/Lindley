"""Finding the pages that answer a question, before the AI is asked it.

No connector gives the AI tools, so Lindley does the looking: the AI names the words to search
for (`search_words`), and `gather` brings the pages a person has open, the pages found with those
words and the pages the last answer cited, as much of each as the question's budget allows.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from lindley.browse import page_number
from lindley.providers.base import ChatMessage
from lindley.search.fts import search_any

MAX_WORDS = 12  # words searched for, the AI's and the question's own
MAX_HITS = 30  # pages found with them, before the budget
MAX_SOURCES = 20  # pages sent with a question
PAGE_MAX = 4000  # characters of one page, at most, however large the budget
PAGE_MIN = 800  # and at least, however small

WORDS_PROMPT = (
    "You help find pages in a person's archive of scanned letters and papers. Given their"
    " question, reply with only a JSON list of up to 8 names, places, dates and other words to"
    " search the pages' text for. Give a person's surname on its own as well as their full"
    ' name. No other text. For example: ["Branson", "John Branson", "Leeds", "1940"]'
)

_SMALL = """a about after all also an and any are as at be been before but by can could did do does
    for from had has have he her him his how i if in into is it its me my no not of on or our
    she so some than that the their them then there these they this those to up was we were what
    when where which who whom why will with would you your say says said tell show find page pages
    letter letters document documents anything something"""
STOPWORDS = frozenset(_SMALL.split())


@dataclass(frozen=True)
class Scope:
    """What a person has open: a page, a document, or neither."""

    document_id: int | None = None
    page_id: int | None = None


@dataclass(frozen=True)
class Source:
    """A page sent with a question, as [n] in the answer."""

    n: int
    page_id: int
    document_id: int | None
    page_number: int | None
    label: str  # "Letter to John, page 2", or "Inbox: scan 14.jpg"
    text: str
    unsure: bool  # read with low confidence, and not checked by a person yet

    def brief(self) -> dict:
        """What the pane and the stored answer keep: enough to name the page and open it."""
        return {
            "n": self.n,
            "page_id": self.page_id,
            "document_id": self.document_id,
            "page_number": self.page_number,
            "label": self.label,
            "unsure": self.unsure,
        }


def question_words(question: str) -> list[str]:
    """The question's own words worth searching for: not the small ones every question has."""
    out: list[str] = []
    for w in re.findall(r"\w+", question):
        if (len(w) > 2 or w.isdigit()) and w.lower() not in STOPWORDS and w not in out:
            out.append(w)
    return out


def _parse_words(reply: str) -> list[str]:
    m = re.search(r"\[.*?\]", reply, re.DOTALL)
    if not m:
        return []
    try:
        got = json.loads(m.group(0))
    except ValueError:
        return []
    if not isinstance(got, list):
        return []
    return [w.strip() for w in got if isinstance(w, str) and w.strip()]


def search_words(chat, question: str, earlier: str | None = None) -> list[str]:
    """What to search the pages for: the words the AI names, then the question's own. A reply
    that isn't a list of words is no loss: the question's words are searched for anyway.
    `earlier`: the question before, so "and his brother?" is searched for with its subject.
    Raises ProviderError when the call fails."""
    asked = f"Earlier question: {earlier}\nQuestion: {question}" if earlier else question
    reply = chat.chat([ChatMessage("system", WORDS_PROMPT), ChatMessage("user", asked)])
    words: list[str] = []
    seen: set[str] = set()
    for w in _parse_words(reply) + question_words(question):
        if w.lower() not in seen:
            seen.add(w.lower())
            words.append(w)
    return words[:MAX_WORDS]


def _clip(text: str, words: Iterable[str], cap: int) -> str:
    """A page's text, at most `cap` characters: around the first word found, if it's long."""
    text = re.sub(r"\n\s*\n\s*", "\n\n", text.strip())
    if len(text) <= cap:
        return text
    width = cap - 2  # room for a "…" at either end
    low = text.lower()
    found = [i for w in words if (i := low.find(w.lower())) >= 0]
    start = max(0, min(found) - width // 3) if found else 0
    start = min(start, len(text) - width)
    out = text[start : start + width]
    return ("…" if start else "") + out + ("…" if start + width < len(text) else "")


_PAGE = """
SELECT p.id, p.document_id, p.position, p.set_aside_at, s.original_name AS file,
       d.name AS document_name, c.text, c.confidence, c.reviewed
FROM pages p
JOIN scans s ON s.id = p.scan_id
JOIN v_current_text c ON c.page_id = p.id
LEFT JOIN documents d ON d.id = p.document_id
WHERE p.id = ?
"""


def _label(conn: sqlite3.Connection, r: sqlite3.Row) -> tuple[str, int | None]:
    if r["document_id"] is not None:
        n = page_number(conn, r["document_id"], r["position"])
        return f"{r['document_name']}, page {n}", n
    return f"{'Set aside' if r['set_aside_at'] else 'Inbox'}: {r['file']}", None


def _document_pages(conn: sqlite3.Connection, doc_id: int | None) -> list[int]:
    if doc_id is None:
        return []
    return [
        r[0]
        for r in conn.execute(
            "SELECT id FROM pages WHERE document_id = ? ORDER BY position", (doc_id,)
        )
    ]


def gather(
    conn: sqlite3.Connection,
    words: list[str],
    scope: Scope,
    budget: int,
    review_below: float,
    cited: Iterable[int] = (),
) -> list[Source]:
    """The pages to send with a question, in this order, until the budget is spent:
    - the page a person has open, and the pages either side of it in its document,
    - pages of the open document that have the words, and pages the last answer cited,
    - the rest of the open document, from its start, with at most half the budget,
    - pages anywhere that have the words, best match first."""
    doc_id = scope.document_id
    focus: list[int] = []
    if scope.page_id is not None:
        r = conn.execute("SELECT document_id FROM pages WHERE id = ?", (scope.page_id,)).fetchone()
        if r is not None:
            focus.append(scope.page_id)
            doc_id = doc_id or r[0]
    in_doc = _document_pages(conn, doc_id)
    if focus and focus[0] in in_doc:
        i = in_doc.index(focus[0])
        focus += in_doc[max(0, i - 1) : i] + in_doc[i + 1 : i + 2]
    hits = search_any(conn, words, MAX_HITS)
    doc_set = set(in_doc)
    order = [
        *((p, "focus") for p in focus),
        *((p, "focus") for p in hits if p in doc_set),
        *((p, "focus") for p in cited),
        *((p, "document") for p in in_doc),
        *((p, "hit") for p in hits if p not in doc_set),
    ]

    cap = min(PAGE_MAX, max(PAGE_MIN, budget // 4))
    left, document_left = budget, budget // 2
    out: list[Source] = []
    seen: set[int] = set()
    for page_id, kind in order:
        if page_id in seen or len(out) >= MAX_SOURCES or left < PAGE_MIN // 2:
            continue
        seen.add(page_id)
        r = conn.execute(_PAGE, (page_id,)).fetchone()
        if r is None or not r["text"].strip():
            continue
        room = min(cap, left, document_left if kind == "document" else left)
        if room < PAGE_MIN // 2:
            continue
        text = _clip(r["text"], words, room)
        left -= len(text)
        if kind == "document":
            document_left -= len(text)
        label, number = _label(conn, r)
        unsure = (
            not r["reviewed"] and r["confidence"] is not None and r["confidence"] < review_below
        )
        out.append(Source(len(out) + 1, page_id, r["document_id"], number, label, text, unsure))
    return out
