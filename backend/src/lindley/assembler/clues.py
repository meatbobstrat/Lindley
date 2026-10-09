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


# OCR slips inside a page number: "1l" is 11, "2O" is 20. Only read this way next to a real
# digit, so a lone "I" or "O" is never taken for a number.
_SLIPS = str.maketrans(
    {"l": "1", "I": "1", "i": "1", "|": "1", "!": "1", "O": "0", "o": "0", "Q": "0", "D": "0"}
    | {"S": "5", "s": "5", "Z": "2", "z": "2", "B": "8"}
)
_NUMBER_EDGES = "-–—~()[]{}.,:;'\"‘’“”/\\*_«»#"
MARKER_EDGE = 0.12  # page numbers sit in the top or bottom 12% of the page
MARKER_MIN_CONF = 20  # an OCR guess below this is a speck, not a number


def read_number(token: str) -> tuple[int, bool] | None:
    """A page number in one OCR word: (number, whether an OCR slip was corrected)."""
    t = token.strip(_NUMBER_EDGES)
    if not t or len(t) > 3:
        return None
    if t.isdigit():
        n, slipped = int(t), False
    elif any(ch.isdigit() for ch in t) and (fixed := t.translate(_SLIPS)).isdigit():
        n, slipped = int(fixed), True
    else:
        return None
    return (n, slipped) if 0 < n <= 300 else None


def is_junk(w: dict) -> bool:
    """An OCR word that's a speck, a smudge or the edge of the paper rather than writing."""
    text, conf = w["text"], w.get("conf", 100)
    letters = sum(ch.isalpha() for ch in text)
    if not letters and not any(ch.isdigit() for ch in text):
        return True  # only marks: | — ~ =
    if conf < 40:
        return True
    # An uncertain word counts as writing if it has a few letters and is mostly letters, like
    # "L.C.Branson". Uncertain "oa", "re" and "(Gi" are usually smudges read as letters.
    return conf < 70 and (letters <= 2 or letters < len(text) / 2)


def rows(words: list[dict]) -> list[list[dict]]:
    """Words grouped into rows by their height on the page, top to bottom, each left to right."""
    out: list[tuple[float, float, list[dict]]] = []  # (centre, height, words)
    for w in sorted(words, key=lambda w: w["bbox"][1] + w["bbox"][3] / 2):
        _, y, _, h = w["bbox"]
        cy = y + h / 2
        if out and abs(cy - out[-1][0]) <= 0.6 * max(out[-1][1], h, 1):
            c, hh, ws = out[-1]
            ws.append(w)
            out[-1] = ((c * (len(ws) - 1) + cy) / len(ws), max(hh, h), ws)
        else:
            out.append((cy, h, [w]))
    return [sorted(ws, key=lambda w: w["bbox"][0]) for _, _, ws in out]


@dataclass
class Marker:
    number: int
    total: int | None
    sure: bool  # read cleanly, not guessed past OCR slips or a low-confidence reading
    words: list[dict] = field(default_factory=list)  # the words it was read from


def _marker_from_words(words: list[dict], height: int) -> Marker | None:
    """A page number on a row of its own near the top or bottom of the page, using word
    positions. Specks and smudges on the same row don't count against it."""
    edge = MARKER_EDGE * height
    rs = rows(words)
    near = [r for r in rs if _centre(r[0]) < edge] + [
        r for r in reversed(rs) if _centre(r[0]) > height - edge
    ]
    for row in near:
        writing = [w for w in row if not is_junk(w)]
        text = " ".join(w["text"] for w in row)
        if m := parse_marker(text):
            return Marker(m[0], m[1], min(w.get("conf", 100) for w in row) >= 60, row)
        numbers = [
            (n, w)
            for w in row
            if (n := read_number(w["text"])) and w.get("conf", 100) >= MARKER_MIN_CONF
        ]
        # The same number twice, typed and copied in pencil beside it: the one read best
        if len(numbers) > 1 and len({n for (n, _), _ in numbers}) == 1:
            best = max(numbers, key=lambda x: x[1].get("conf", 100))
            writing = [w for w in writing if read_number(w["text"]) is None or w is best[1]]
            numbers = [best]
        # A clear number beside doubtful ones: "95" read at 94% next to a "25" at 51%
        clear = [x for x in numbers if x[1].get("conf", 100) >= 80]
        if (
            len(numbers) > 1
            and len(clear) == 1
            and all(x[1].get("conf", 100) < 60 for x in numbers if x is not clear[0])
        ):
            numbers = clear
            writing = [w for w in writing if read_number(w["text"]) is None or w is clear[0][1]]
        if len(numbers) == 1 and all(w is numbers[0][1] for w in writing):
            (n, slipped), w = numbers[0]
            return Marker(n, None, not slipped and w.get("conf", 100) >= 60, row)
    return None


def _centre(w: dict) -> float:
    return w["bbox"][1] + w["bbox"][3] / 2


# ---------------------------------------------------------------- Lines and noise

NOISE_LINES = 3  # at most this many noise lines are dropped from each end of a page


@dataclass
class Line:
    text: str
    words: list[dict] = field(default_factory=list)  # empty when the reading has no word boxes


def text_lines(text: str, words: list[dict] | None = None) -> list[Line]:
    """The reading's lines, each with its OCR words when the words match the text (Tesseract
    writes each line as its words joined by spaces; a person's correction won't match)."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not words or sum(len(ln.split()) for ln in lines) != len(words):
        return [Line(ln) for ln in lines]
    out, i = [], 0
    for ln in lines:
        n = len(ln.split())
        out.append(Line(ln, words[i : i + n]))
        i += n
    return _on_the_page(out)


def _span(ws: list[dict]) -> tuple[float, float]:
    return min(w["bbox"][1] for w in ws), max(w["bbox"][1] + w["bbox"][3] for w in ws)


def _on_the_page(lines: list[Line]) -> list[Line]:
    """Lines top to bottom as they sit on the page. Tesseract sometimes reads a word or two of
    a line as a line of its own, out of order ("stage" before "Fellow passengers were"); a
    scrap like that is put back into the line it sits beside."""
    out = [Line(ln.text, list(ln.words)) for ln in lines]
    for ln in [o for o in out if len(o.words) <= 3]:
        lo, hi = _span(ln.words)
        mid = (lo + hi) / 2
        left, right = min(w["bbox"][0] for w in ln.words), max(_right(w) for w in ln.words)
        home = next(
            (
                o
                for o in out
                if len(o.words) > 3
                and _span(o.words)[0] <= mid <= _span(o.words)[1]
                and not any(w["bbox"][0] < right and left < _right(w) for w in o.words)
            ),
            None,
        )
        if home:
            home.words = sorted(home.words + ln.words, key=lambda w: w["bbox"][0])
            home.text = " ".join(w["text"] for w in home.words)
            out.remove(ln)
    return sorted(out, key=lambda ln: sum(_span(ln.words)) / 2)  # stable: ties keep their order


def _right(w: dict) -> float:
    return w["bbox"][0] + w["bbox"][2]


def is_noise(line: Line) -> bool:
    """A line of specks and smudges: the edge of the paper, a hole punch, show-through."""
    if not line.words:
        return not re.search(r"[A-Za-z]{3}|\d", line.text)
    real = [w for w in line.words if not is_junk(w)]
    if not real or (len(real) <= 2 and len(real) < len(line.words) / 2):
        return True
    # A scrap of a few letters read with doubt: the cut-off edge of a line ("id", "on", "Tale")
    letters = sum(ch.isalpha() for w in line.words for ch in w["text"])
    return letters < 6 and sum(w.get("conf", 100) for w in line.words) / len(line.words) < 75


def trim_noise(lines: list[Line]) -> list[Line]:
    """Drop noise lines from the top and bottom of a page, and specks before the first word
    and after the last, so the page's real first and last lines show."""
    out = list(lines)
    for _ in range(NOISE_LINES):
        if out and is_noise(out[0]):
            out.pop(0)
    for _ in range(NOISE_LINES):
        if out and is_noise(out[-1]):
            out.pop()
    if out and out[0].words:
        ws = out[0].words
        while len(ws) > 1 and is_junk(ws[0]):
            ws = ws[1:]
        out[0] = Line(" ".join(w["text"] for w in ws), ws)
    if out and out[-1].words:
        ws = out[-1].words
        while len(ws) > 1 and is_junk(ws[-1]):
            ws = ws[:-1]
        out[-1] = Line(" ".join(w["text"] for w in ws), ws)
    return out


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
_TO_YR = r"(?:,\s*|\s+)"  # typists often left no space after the comma: "June 24,1940"
_DATE_MDY = re.compile(rf"\b{_MON}\s+(\d{{1,2}})(?:st|nd|rd|th|d)?{_TO_YR}{_YR}\b", re.I)
_DATE_DMY = re.compile(rf"\b(\d{{1,2}})(?:st|nd|rd|th|d)?\s+(?:of\s+)?{_MON}{_TO_YR}{_YR}\b", re.I)
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
# Who a letter is to, under its date line: "Honorable Grey Mashburn,", "Mr. A.E. Johnson"
_ADDRESSEE = re.compile(
    r"^(?:hon(?:ou?rable\b|\.)|mrs?\b\.?|miss\b|messrs\b\.?|dr\b\.?|dear\b)", re.I
)
DATELINE_LEN = 60  # a date line is short: a place and a date, no more
SALUTE_DEEP = 15  # a greeting may sit this far down, under a letterhead
# "By Lindley C. Branson": an article, a story or a chapter starts here
_BYLINE = re.compile(r"^[Bb]y\s+(?:[A-Z][a-z]*\.?\s*){1,4}$")
# "Paid", "Paid. Thank you.", "Paid in full, T. Hale": a short line of its own, not "and paid no
# attention" in a story
_PAID = re.compile(r"^\W*paid\b", re.I)
PAID_LINE = 40
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
_FILE_COPY = re.compile(r"\s*\((\d+)\)$")  # Windows numbers files of one name: "Image (2)"
_MID_START = re.compile(r"^[a-z]")
# A word broken at the end of a line. Tesseract often reads a typewriter's hyphen as "=".
HYPHENS = ("-", "=", "¬")


@dataclass
class PageClues:
    marker: tuple[int, int | None] | None = None  # (page number, total)
    marker_sure: bool = False  # read cleanly: no OCR slips, good confidence
    salutation: str | None = None
    dateline: str | None = None  # "Ely, Nevada, June 24, 1940", over who the letter is to
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
        if self.dateline:
            return f"starts with the date line “{self.dateline}”"
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
            out.append(
                (
                    "page_marker",
                    f"{n}" + (f" of {total}" if total else ""),
                    str(n),
                    85 if self.marker_sure else 55,
                )
            )
        for kind, val in (
            ("salutation", self.salutation),
            ("dateline", self.dateline),
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


def _dateline(top: list[str]) -> str | None:
    """A letter's date line, among the page's top lines: a short line with a full date, with
    who it's to (or the greeting) just below. A diary's dated entry has no one under its date."""
    for i, ln in enumerate(top):
        if len(ln) > DATELINE_LEN or not any(conf >= 90 for _, _, conf in find_dates(ln)):
            continue
        for b in top[i + 1 : i + 4]:
            short_name = len(b) <= 40 and not b.rstrip().endswith((".", "!", "?"))
            if (_ADDRESSEE.match(b) and short_name) or (
                _SALUTE.match(b) and b.rstrip().endswith((",", ":"))
            ):
                return ln.strip()
    return None


def file_series(file_name: str) -> tuple[str, int | None]:
    """(series, number in it) from a scan's file name: scan_0042 is 42 of "scan_".

    Scanners and file managers often number only the files after the first: Image, Image (2),
    Image (3) on Windows; Image, Image 2 on a Mac. So a name without a number is number 1."""
    stem = PurePath(file_name).stem
    if m := _FILE_COPY.search(stem) or _FILE_SEQ.search(stem):
        return (stem[: m.start()] + stem[m.end() :]).rstrip().lower(), int(m.group(1))
    return stem.strip().lower(), 1 if stem.strip() else None


def page_clues(
    text: str,
    file_name: str = "",
    words: list[dict] | None = None,
    height: int | None = None,
    blank_score: float | None = None,
) -> PageClues:
    c = PageClues()
    c.file_prefix, c.file_seq = file_series(file_name)

    stripped = re.sub(r"\s+", "", text)
    if (blank_score is not None and blank_score >= 0.97) or len(stripped) < 15:
        c.kind = "blank"
        return c

    # A page number is a number with nothing else on its line, at the very top or bottom.
    lines = text_lines(text, words)
    if words and height and (found := _marker_from_words(words, height)):
        c.marker, c.marker_sure = (found.number, found.total), found.sure
        taken = {id(w) for w in found.words}
        lines = [ln for ln in lines if not (ln.words and all(id(w) in taken for w in ln.words))]
    # Tesseract may doubt every word of a typed date line and so call it noise: it's looked
    # for among the lines before noise is trimmed (specks are never a date)
    top = [ln.text for ln in lines[: NOISE_LINES + 4]]
    lines = trim_noise(lines)
    body = [ln.text for ln in lines]
    for i in (0, -1):
        if body and (m := parse_marker(body[i])):
            if not c.marker:
                c.marker, c.marker_sure = m, True
            body.pop(i)
    if not body:
        c.kind = "blank"
        return c

    c.first_line, c.last_line = body[0][:200], body[-1][:200]
    for i, ln in enumerate(body[:SALUTE_DEEP]):
        # Further down, under a letterhead, only a short line ending as a greeting does
        ends = ln.rstrip().endswith((",", ":"))
        if _SALUTE.match(ln) and ((ends or len(ln) <= 25) if i < 5 else (ends and len(ln) <= 30)):
            c.salutation = ln.rstrip(",:").strip()
            c.people |= _names_after_salutation(ln)
            break
    c.dateline = _dateline(top)
    for ln in body[:3]:
        if _BYLINE.match(ln.replace(".", ". ").strip()) and not c.heading:
            c.heading = ln.rstrip(".,:")
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
    if not c.closing:
        if any(_PAID.match(ln) and len(ln) <= PAID_LINE for ln in body[-3:]):
            c.ends_form = "Paid"
        elif m := _DEED_END.search(" ".join(body[-6:])):
            c.ends_form = m.group(0)

    c.starts_mid = bool(_MID_START.match(body[0])) and not c.salutation
    last = body[-1]
    c.ends_mid = not (c.closing or c.ends_form) and (
        last.endswith(HYPHENS) or (last[-1:].isalpha() or last[-1:] == ",")
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
    elif c.salutation or c.closing or c.dateline:
        c.kind = "letter"
    elif len(stripped) < 80 and not c.marker and not c.starts_mid and not c.ends_mid:
        c.kind = "notes"
    return c
