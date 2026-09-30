"""Clues about a single page, found with old-fashioned text rules. No AI.

Each clue says something about where a page sits in a document: a page number, a greeting
that starts a letter, a signature that ends one, a sentence cut off at the bottom of the page.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import PurePath

# ---------------------------------------------------------------- Page numbers

_ROMAN = {
    "i": 1,
    "ii": 2,
    "iii": 3,
    "iv": 4,
    "v": 5,
    "vi": 6,
    "vii": 7,
    "viii": 8,
    "ix": 9,
    "x": 10,
    "xi": 11,
    "xii": 12,
}
_MARKERS = [
    re.compile(r"^[-–—~(\[\s]*(\d{1,3})[-–—~)\]\s.]*$"),  # 2, - 2 -, (2)
    re.compile(
        r"^(?:page|pg\.?|p\.)\s*(\d{1,3})(?:\s*(?:of|/)\s*(\d{1,3}))?\.?$", re.I
    ),  # Page 2 of 3
    re.compile(r"^(\d{1,3})\s*(?:of|/)\s*(\d{1,3})$"),  # 2 of 3, 2/3
]
_MARKER_ROMAN = re.compile(r"^[-–—(\s]*([ivx]{1,4}|[IVX]{2,4})[-–—).\s]*$")  # not "I", a pronoun


def parse_marker(line: str) -> tuple[int, int | None] | None:
    """A page number standing alone on a line: returns (number, total or None)."""
    s = line.strip()
    for pat in _MARKERS:
        if m := pat.match(s):
            n = int(m.group(1))
            total = int(m.group(2)) if m.lastindex and m.lastindex > 1 and m.group(2) else None
            if 0 < n <= 300 and (total is None or n <= total):
                return n, total
            return None
    if (m := _MARKER_ROMAN.match(s)) and m.group(1).lower() in _ROMAN:
        return _ROMAN[m.group(1).lower()], None
    return None


def _marker_from_words(words: list[dict], height: int) -> tuple[int, int | None] | None:
    """A page number alone in the top or bottom 12% of the page, using word positions."""
    band = 0.12 * height
    rows: dict[int, list[dict]] = {}
    for w in words:
        x, y, _, h = w["bbox"]
        cy = y + h / 2
        if cy < band or cy > height - band:
            rows.setdefault(int(cy // max(h, 1)), []).append(w)
    for row in rows.values():
        marker = parse_marker(" ".join(w["text"] for w in sorted(row, key=lambda w: w["bbox"][0])))
        if marker:
            return marker
    return None


# ---------------------------------------------------------------- Dates

MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
MONTH_NAMES = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
_MON = (
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?"
    r"|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?"
)
_YR = r"(1[5-9]\d\d)"
_DATE_MDY = re.compile(rf"\b{_MON}\s+(\d{{1,2}})(?:st|nd|rd|th|d)?,?\s+{_YR}\b", re.I)
_DATE_DMY = re.compile(rf"\b(\d{{1,2}})(?:st|nd|rd|th|d)?\s+(?:of\s+)?{_MON},?\s+{_YR}\b", re.I)
_DATE_MY = re.compile(rf"\b{_MON},?\s+{_YR}\b", re.I)
_DATE_NUM = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4}|\d{2})\b")
_YEAR = re.compile(r"\b(1[6-9]\d\d)\b")


def _mon(s: str) -> int:
    return MONTHS[s[:3].lower()]


def find_dates(text: str) -> list[tuple[str, str, int]]:
    """Dates as (as written, partial ISO date, confidence), fullest first."""
    out: list[tuple[str, str, int]] = []
    taken: list[tuple[int, int]] = []

    def add(m: re.Match, iso: str, conf: int) -> None:
        if any(a < m.end() and m.start() < b for a, b in taken):
            return
        taken.append((m.start(), m.end()))
        out.append((m.group(0), iso, conf))

    for m in _DATE_MDY.finditer(text):
        if 1 <= int(m.group(2)) <= 31:
            add(m, f"{m.group(3)}-{_mon(m.group(1)):02d}-{int(m.group(2)):02d}", 90)
    for m in _DATE_DMY.finditer(text):
        if 1 <= int(m.group(1)) <= 31:
            add(m, f"{m.group(3)}-{_mon(m.group(2)):02d}-{int(m.group(1)):02d}", 90)
    for m in _DATE_MY.finditer(text):
        add(m, f"{m.group(2)}-{_mon(m.group(1)):02d}", 80)
    for m in _DATE_NUM.finditer(text):
        mo, d, y = int(m.group(1)), int(m.group(2)), m.group(3)
        if 1 <= mo <= 12 and 1 <= d <= 31:
            # Two-digit years are read as the 1800s, the most common case in these archives.
            add(m, f"{y if len(y) == 4 else '18' + y}-{mo:02d}-{d:02d}", 60 if len(y) == 4 else 40)
    if not out:
        for m in _YEAR.finditer(text):
            add(m, m.group(1), 40)
    return out


# ---------------------------------------------------------------- Letters, forms, people

_SALUTE = re.compile(
    r"^(my\s+dear(?:est)?|dear(?:est)?|friend|to\s+whom\s+it\s+may\s+concern|sirs?|madam|gentlemen)\b",
    re.I,
)
_KIN = {
    "sister",
    "brother",
    "mother",
    "father",
    "cousin",
    "aunt",
    "uncle",
    "friend",
    "sir",
    "sirs",
    "madam",
    "son",
    "daughter",
    "wife",
    "husband",
    "child",
    "children",
    "folks",
    "all",
    "old",
}
_CLOSE = re.compile(
    r"^(yours|ever\s+yours|as\s+ever|affectionately|sincerely|respectfully|lovingly|faithfully"
    r"|with\s+(?:much\s+)?love|truly\s+yours|good\s*-?\s*bye"
    r"|your\s+(?:\w+\s+)?(?:brother|sister|son|daughter|mother|father|friend|cousin|niece|nephew"
    r"|wife|husband|servant|child))\b",
    re.I,
)
_HEADINGS = re.compile(
    r"^(know\s+all\s+men|this\s+indenture|receipt|invoice|inventory|last\s+will|certificate|deed\b)",
    re.I,
)
_DEED_END = re.compile(r"in\s+witness\s+whereof|notary\s+public|signed,?\s+sealed", re.I)
_DEED_WORDS = re.compile(
    r"\b(grantor|grantee|hereby|premises|acres|conveyed?|indenture|heirs\s+and\s+assigns|deed)\b",
    re.I,
)
_RECEIPT_LINE = re.compile(
    r"(\.{3,}|\s{2,})\s*\$?\d+\.\d\d\s*$|^total\b|^sold\s+to\b|^received\s+of\b", re.I
)
_WEEKDAY = re.compile(r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", re.I)
_PLACE = re.compile(
    r"^([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),\s+(?:[A-Z][a-z]{0,4}\.|Ohio|Indiana|Kentucky)"
)
_TITLED = re.compile(
    r"\b(?:Mr|Mrs|Miss|Dr|Rev|Capt|Aunt|Uncle|Cousin|Grandma|Grandpa)\.?\s+"
    r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)"
)
_SOLD_TO = re.compile(
    r"\b(?:sold\s+to|received\s+of|bought\s+of)\s+([A-Z][\w.]*(?:\s[A-Z][\w.]*){0,2})", re.I
)
_AMOUNT = re.compile(r"\$?\b\d+\.\d\d\b")
_REFNO = re.compile(r"\b(?:Book|Vol\.?|Lot|No\.)\s+\d+\b")
_FILE_SEQ = re.compile(r"(\d+)(?!.*\d)")
_MID_START = re.compile(r"^[a-z]")


@dataclass
class PageClues:
    marker: tuple[int, int | None] | None = None  # (page number, total)
    salutation: str | None = None
    letterhead: str | None = None
    heading: str | None = None
    closing: str | None = None
    signature: str | None = None
    ends_form: str | None = None  # "Paid", "TOTAL", "Notary Public"...
    starts_mid: bool = False
    ends_mid: bool = False
    first_line: str = ""
    last_line: str = ""
    kind: str | None = None  # letter, receipt, deed, diary, notes, blank
    dates: list[tuple[str, str, int]] = field(default_factory=list)
    people: set[str] = field(default_factory=set)
    places: set[str] = field(default_factory=set)
    amounts: list[str] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)
    file_prefix: str = ""
    file_seq: int | None = None

    @property
    def starts_doc(self) -> str | None:
        """Why this page looks like the first page of a document, if it does."""
        if self.salutation:
            return f"starts with “{self.salutation}”"
        if self.heading:
            return f"starts with the heading “{self.heading}”"
        if self.letterhead:
            return f"has the letterhead “{self.letterhead}”"
        if self.marker and self.marker[0] == 1:
            return "is numbered page 1"
        return None

    @property
    def ends_doc(self) -> str | None:
        """Why this page looks like the last page of a document, if it does."""
        if self.signature:
            return f"ends with the signature “{self.signature}”"
        if self.closing:
            return f"ends with “{self.closing}”"
        if self.ends_form:
            return f"ends with “{self.ends_form}”"
        if self.marker and self.marker[1] and self.marker[0] == self.marker[1]:
            return f"is numbered page {self.marker[0]} of {self.marker[1]}"
        return None

    def facts(self) -> list[tuple[str, str, str | None, int]]:
        """(kind, value, normalised value, confidence) rows for the facts table."""
        out: list[tuple[str, str, str | None, int]] = []
        if self.marker:
            n, total = self.marker
            out.append(("page_marker", f"{n}" + (f" of {total}" if total else ""), str(n), 85))
        for kind, val in (
            ("salutation", self.salutation),
            ("letterhead", self.letterhead),
            ("heading", self.heading),
            ("closing", self.closing),
            ("signature_name", self.signature),
        ):
            if val:
                out.append((kind, val, None, 80))
        out += [("date", v, iso, conf) for v, iso, conf in self.dates]
        out += [("person", p, p.lower(), 60) for p in sorted(self.people)]
        out += [("place", p, p.lower(), 60) for p in sorted(self.places)]
        out += [("amount", a, a.lstrip("$"), 70) for a in self.amounts[:10]]
        out += [("reference_number", r, None, 60) for r in self.refs]
        if self.first_line:
            out.append(("first_line", self.first_line, None, 100))
        if self.last_line:
            out.append(("last_line", self.last_line, None, 100))
        if self.kind:
            out.append(("document_type_hint", self.kind, self.kind, 60))
        return out


def _is_caps(line: str) -> bool:
    letters = [c for c in line if c.isalpha()]
    return (
        len(letters) >= 6
        and sum(c.isupper() for c in letters) / len(letters) >= 0.8
        and len(line) <= 60
    )


def _names_after_salutation(line: str) -> set[str]:
    words = re.findall(r"[A-Z][a-z]+", line)
    return {
        w for w in words if w.lower() not in _KIN and w.lower() not in {"dear", "my", "dearest"}
    }


def page_clues(
    text: str,
    file_name: str = "",
    words: list[dict] | None = None,
    height: int | None = None,
    blank_score: float | None = None,
) -> PageClues:
    c = PageClues()
    stem = PurePath(file_name).stem
    if m := _FILE_SEQ.search(stem):
        c.file_seq = int(m.group(1))
        c.file_prefix = (stem[: m.start()] + stem[m.end() :]).lower()
    else:
        c.file_prefix = stem.lower()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    stripped = re.sub(r"\s+", "", text)
    if (blank_score is not None and blank_score >= 0.97) or len(stripped) < 15:
        c.kind = "blank"
        return c

    # A page number is a number with nothing else on its line, at the very top or bottom.
    if words and height:
        c.marker = _marker_from_words(words, height)
    body = list(lines)
    for i in (0, -1):
        if body and (m := parse_marker(body[i])):
            c.marker = c.marker or m
            body.pop(i)
    if not body:
        c.kind = "blank"
        return c

    c.first_line, c.last_line = body[0][:200], body[-1][:200]
    for ln in body[:5]:
        if _SALUTE.match(ln) and (ln.rstrip().endswith((",", ":")) or len(ln) <= 25):
            c.salutation = ln.rstrip(",:").strip()
            c.people |= _names_after_salutation(ln)
            break
    for ln in body[:2]:
        if _HEADINGS.match(ln):
            c.heading = ln.rstrip(".,:")
            break
        if _is_caps(ln) and not _WEEKDAY.match(ln):
            c.letterhead = ln.rstrip(".,:")
            break
    for i in range(max(0, len(body) - 4), len(body)):
        if _CLOSE.match(body[i]):
            c.closing = body[i].rstrip(",.")
            sig = [s for s in body[i + 1 : i + 3] if len(s) <= 30 and s[:1].isupper()]
            if sig:
                c.signature = sig[-1].rstrip(".,")
                c.people.add(c.signature)
            break
    tail = " ".join(body[-3:])
    if not c.closing:
        if re.search(r"\bpaid\b", tail, re.I):
            c.ends_form = "Paid"
        elif m := _DEED_END.search(" ".join(body[-6:])):
            c.ends_form = m.group(0)

    c.starts_mid = bool(_MID_START.match(body[0])) and not c.salutation
    last = body[-1]
    c.ends_mid = not (c.closing or c.ends_form) and (
        last.endswith("-") or (last[-1:].isalpha() or last[-1:] == ",")
    )

    c.dates = find_dates(text)
    for ln in body[:3]:
        if m := _PLACE.match(ln):
            c.places.add(m.group(1))
    c.people |= {m.group(1) for m in _TITLED.finditer(text)}
    c.people |= {m.group(1).rstrip(".") for m in _SOLD_TO.finditer(text)}
    c.amounts = _AMOUNT.findall(text)
    c.refs = _REFNO.findall(text)

    receipt_lines = sum(1 for ln in body if _RECEIPT_LINE.search(ln))
    if receipt_lines >= 2 or (receipt_lines and c.ends_form == "Paid"):
        c.kind = "receipt"
    elif len(_DEED_WORDS.findall(text)) >= 2 or (c.heading and "know all men" in c.heading.lower()):
        c.kind = "deed"
    elif _WEEKDAY.match(body[0]):
        c.kind = "diary"
    elif c.salutation or c.closing:
        c.kind = "letter"
    elif len(stripped) < 80 and not c.marker and not c.starts_mid and not c.ends_mid:
        c.kind = "notes"
    return c
