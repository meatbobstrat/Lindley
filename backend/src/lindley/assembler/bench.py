"""A test bench for the assembler: made-up archive batches whose right answers are known.

`make_batch(seed)` writes believable pages (letters, receipts, a deed, diary pages, notes, blanks)
in a believable scanning order, with the usual mistakes: pages swapped, a page scanned later.
`score(conn, truth)` compares what the assembler did with the right answer.
"""

from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass, field
from itertools import combinations

from lindley.providers.base import ChatMessage

PLACES = ["Xenia, O.", "Bellbrook, O.", "Dayton, O.", "Spring Valley, O.", "Cedarville, O."]
FIRSTS = ["John", "Will", "Clara", "Mary", "Samuel", "Ellen", "George", "Hattie"]
SURNAMES = ["Branson", "Hale", "Price", "Moore", "Tobias"]
MONTHS = ["Jan.", "Feb.", "March", "April", "May", "June", "Aug.", "Sept.", "Oct.", "Nov."]
SENTENCES = [
    "We are all well here and hope these few lines find you the same.",
    "The river came up over the low road again last week and Father could not get to town.",
    "Mother says to tell you the hens are laying well now that the weather has turned.",
    "I went to meeting on Sunday and saw {name}, who asked after you.",
    "The corn is in and the wheat looks better than it did last year.",
    "Uncle Samuel sold the bay mare at the sale in Xenia for a good price.",
    "We had a letter from {name} and she is teaching school again this winter.",
    "There is talk of a new bridge at the mill but nobody believes it will come to anything.",
    "Please write soon and tell us all about your new place and the people there.",
    "The children have had the measles but are over the worst of it now.",
    "{name} was here on Tuesday with the papers for the east forty acres.",
    "Father thinks we will sell the old place in the spring if the price holds.",
    "It has rained every day this week and the lane is nothing but mud.",
    "I have sent the book you asked for by the Tuesday stage.",
    "We are going to the county fair if the weather is fine and the wagon is mended.",
    "The new preacher is a young man from Cincinnati and the ladies think him very fine.",
    "Tell {name} that the quilt is nearly done and she shall have it at Christmas.",
    "The doctor came out twice for Grandma but she is sitting up again now.",
    "Prices are so low this year that it hardly pays to haul the hogs to Dayton.",
    "I found your old slate in the attic and have put it by for you.",
]
CLOSINGS = [
    "Your loving brother",
    "Your affectionate sister",
    "Yours truly",
    "Affectionately",
    "Your loving son",
    "Ever yours",
]
STORES = ["XENIA FEED & SEED CO.", "BELLBROOK MERCANTILE", "J. H. MOORE, DRY GOODS"]
ITEMS = [
    "2 bu. seed oats",
    "50 lb. timothy",
    "1 gal. coal oil",
    "10 yds. calico",
    "1 lb. coffee",
    "1 pr. boots",
    "25 lb. flour",
]
LEGAL = [
    "that the grantor, for and in consideration of the sum of eight hundred dollars, does hereby",
    "grant, bargain, sell and convey unto the grantee, his heirs and assigns forever, the premises",
    "situate in Greene County and being part of Section 12, containing forty acres more or less,",
    "together with all the privileges and appurtenances thereunto belonging, to have and to hold",
    "the same unto the said grantee, and the grantor does covenant that the premises are free",
    "and clear of all incumbrances whatsoever, and that he will warrant and defend the same.",
]
NOTES = [
    "Ask Clara about the Dayton letters.",
    "Box 4, top shelf. Check with Aunt Mary.",
    "These go with the Branson papers?",
    "Photo of the mill, about 1900.",
]
DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@dataclass
class TruePage:
    text: str
    doc: str  # which document it truly belongs to
    index: int  # its true position in that document
    kind: str


@dataclass
class Batch:
    pages: list[TruePage]  # in scanning order
    late: list[TruePage] = field(default_factory=list)  # scanned later, in a second drop


def _fill(r: random.Random, s: str) -> str:
    return s.replace("{name}", f"{r.choice(FIRSTS)} {r.choice(SURNAMES)}")


def _marker(r: random.Random, n: int, style: str) -> str:
    return {"dash": f"- {n} -", "bare": f"{n}", "page": f"Page {n}"}[style]


def letter(r: random.Random, key: str, n_pages: int) -> list[TruePage]:
    writer = r.choice(FIRSTS)
    year = r.randint(1885, 1905)
    head = f"{r.choice(PLACES)}, {r.choice(MONTHS)} {r.randint(1, 28)} {year}"
    greet = r.choice(
        [
            "Dear Sister,",
            "Dear Mother,",
            f"Dear {r.choice(FIRSTS)},",
            f"Friend {r.choice(SURNAMES)},",
            "My dear Cousin,",
        ]
    )
    sentences = [_fill(r, s) for s in r.sample(SENTENCES, min(len(SENTENCES), 4 * n_pages + 1))]
    per = max(2, len(sentences) // n_pages)
    marked, style = r.random() < 0.45, r.choice(["dash", "bare", "page"])
    pages, carry = [], ""
    for i in range(n_pages):
        chunk = sentences[i * per : (i + 1) * per] if i < n_pages - 1 else sentences[i * per :]
        body = ([head, greet] if i == 0 else []) + ([carry] if carry else [])
        carry = ""
        text_lines = list(chunk)
        if i < n_pages - 1 and r.random() < 0.6 and text_lines:
            words = text_lines[-1].split()
            k = r.randint(2, len(words) - 2)
            text_lines[-1], carry = " ".join(words[:k]), " ".join(words[k:])
        body += text_lines
        if i == n_pages - 1:
            body += [r.choice(CLOSINGS), writer]
        if marked and i > 0:
            m = _marker(r, i + 1, style)
            body = [m] + body if r.random() < 0.5 else body + [m]
        pages.append(TruePage("\n".join(body), key, i, "letter"))
    return pages


def receipt(r: random.Random, key: str) -> list[TruePage]:
    items = r.sample(ITEMS, r.randint(2, 4))
    prices = [r.randint(10, 400) / 100 for _ in items]
    lines = [
        r.choice(STORES),
        "Main St.",
        "",
        f"{r.choice(['Apr', 'May', 'Oct', 'Nov'])} {r.randint(1, 28)} {r.randint(1885, 1905)}",
        f"Sold to {r.choice('JWGS')}. {r.choice(SURNAMES)}",
        "",
    ]
    lines += [f"{it} {'.' * (20 - len(it))} {p:.2f}" for it, p in zip(items, prices, strict=True)]
    lines += ["", f"TOTAL .............. {sum(prices):.2f}", "", "Paid. Thank you."]
    return [TruePage("\n".join(lines), key, 0, "receipt")]


def deed(r: random.Random, key: str) -> list[TruePage]:
    year = r.randint(1860, 1900)
    grantor, grantee = (
        f"{r.choice(FIRSTS)} {r.choice(SURNAMES)}",
        f"{r.choice(FIRSTS)} {r.choice(SURNAMES)}",
    )
    p1 = ["KNOW ALL MEN BY THESE PRESENTS,", f"that {grantor} of Greene County, Ohio,"] + LEGAL[:3]
    p2 = LEGAL[3:]
    p3 = [
        f"unto {grantee}.",
        f"IN WITNESS WHEREOF the grantor has set his hand this {r.randint(2, 28)}th day of",
        f"{r.choice(MONTHS)} {year}.",
        f"{grantor}",
        "Notary Public",
    ]
    return [
        TruePage("\n".join(p + [f"Page {i + 1} of 3"]), key, i, "deed")
        for i, p in enumerate((p1, p2, p3))
    ]


def diary(r: random.Random, key: str, n_pages: int) -> list[TruePage]:
    day, pages = r.randint(1, 20), []
    for i in range(n_pages):
        lines = []
        for _ in range(2):
            lines += [f"{DAYS[day % 7]}, Jan. {day}.", _fill(r, r.choice(SENTENCES))]
            day += 1
        pages.append(TruePage("\n".join(lines), key, i, "diary"))
    return pages


def make_batch(seed: int) -> Batch:
    r = random.Random(seed)
    docs: list[list[TruePage]] = []
    for i in range(r.randint(4, 7)):
        docs.append(letter(r, f"letter{i}", r.choice([1, 2, 2, 3, 3, 4])))
    for i in range(r.randint(1, 2)):
        docs.append(receipt(r, f"receipt{i}"))
    if r.random() < 0.6:
        docs.append(deed(r, "deed"))
    if r.random() < 0.5:
        docs.append(diary(r, "diary", r.randint(2, 3)))
    for i in range(r.randint(0, 2)):
        docs.append([TruePage(r.choice(NOTES), f"note{i}", 0, "notes")])
    for i in range(r.randint(0, 2)):
        docs.append([TruePage("", f"blank{i}", 0, "blank")])
    r.shuffle(docs)

    late: list[TruePage] = []
    long_letters = [d for d in docs if d[0].kind == "letter" and len(d) >= 3]
    if long_letters and r.random() < 0.5:  # the last page turns up in a later drop
        late.append(long_letters[0].pop())
    pages = [p for d in docs for p in d]
    multi = [
        i
        for i in range(len(pages) - 1)
        if pages[i].doc == pages[i + 1].doc and pages[i].kind == "letter"
    ]
    if multi and r.random() < 0.3:  # two pages fed through the scanner swapped
        i = r.choice(multi)
        pages[i], pages[i + 1] = pages[i + 1], pages[i]
    return Batch(pages, late)


def load(
    conn: sqlite3.Connection, pages: list[TruePage], start_seq: int = 1, prefix: str = "scan_"
) -> dict[int, TruePage]:
    """Insert pages as read scans with current text. Returns page id -> the right answer."""
    truth = {}
    with conn:
        for i, tp in enumerate(pages):
            name = f"{prefix}{start_seq + i:04d}.jpg"
            scan = conn.execute(
                "INSERT INTO scans"
                " (sha256, original_name, source_path, origin, import_mode, status,"
                " imported_at) VALUES (?, ?, ?, 'watched', 'copy', 'read', datetime('now', ?))",
                (
                    f"{name}:{hash(tp.text)}:{tp.doc}:{start_seq + i}",
                    name,
                    f"D:/Scans/{name}",
                    f"+{start_seq + i} seconds",
                ),
            ).lastrowid
            page = conn.execute("INSERT INTO pages (scan_id) VALUES (?)", (scan,)).lastrowid
            conn.execute(
                "INSERT INTO transcriptions (page_id, source, text, confidence, is_current)"
                " VALUES (?, 'tesseract', ?, 91, 1)",
                (page, tp.text),
            )
            truth[page] = tp
    return truth


@dataclass
class Score:
    precision: float
    recall: float
    f1: float
    documents_made: int
    wrong_documents: int  # made by Lindley but mixing pages of different true documents
    exact: float  # share of true documents rebuilt exactly
    ordered: float  # of those, share in the right order
    inbox_left: float  # share of real document pages still in the Inbox

    def row(self) -> str:
        return (
            f"pairs P {self.precision:.2f} R {self.recall:.2f} F1 {self.f1:.2f} | "
            f"docs made {self.documents_made}, wrong {self.wrong_documents} | "
            f"exact {self.exact:.0%}, "
            f"in order {self.ordered:.0%} | left in Inbox {self.inbox_left:.0%}"
        )


def score(conn: sqlite3.Connection, truth: dict[int, TruePage]) -> Score:
    placed = {
        r["id"]: (r["document_id"], r["position"])
        for r in conn.execute("SELECT id, document_id, position FROM pages")
    }
    pred = {pid: (f"d{placed[pid][0]}" if placed[pid][0] else f"inbox{pid}") for pid in truth}
    real = {
        pid: (tp.doc if tp.kind not in ("blank", "notes") else f"single{pid}")
        for pid, tp in truth.items()
    }
    t_pairs = {(a, b) for a, b in combinations(sorted(truth), 2) if real[a] == real[b]}
    p_pairs = {(a, b) for a, b in combinations(sorted(truth), 2) if pred[a] == pred[b]}
    tp_ = len(t_pairs & p_pairs)
    precision = tp_ / len(p_pairs) if p_pairs else 1.0
    recall = tp_ / len(t_pairs) if t_pairs else 1.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    made: dict[str, list[int]] = {}
    for pid, d in pred.items():
        if d.startswith("d"):
            made.setdefault(d, []).append(pid)
    wrong = sum(1 for ids in made.values() if len({real[i] for i in ids}) > 1)
    true_docs: dict[str, list[int]] = {}
    for pid, d in real.items():
        if not d.startswith("single"):
            true_docs.setdefault(d, []).append(pid)
    exact = in_order = 0
    for ids in true_docs.values():
        d = pred[ids[0]]
        if d.startswith("d") and sorted(made[d]) == sorted(ids):
            exact += 1
            by_pos = sorted(ids, key=lambda i: placed[i][1])
            in_order += [truth[i].index for i in by_pos] == sorted(truth[i].index for i in ids)
    doc_pages = [pid for pid in truth if not real[pid].startswith("single")]
    left = sum(1 for pid in doc_pages if placed[pid][0] is None)
    return Score(
        precision,
        recall,
        f1,
        len(made),
        wrong,
        exact / max(1, len(true_docs)),
        in_order / max(1, exact),
        left / max(1, len(doc_pages)),
    )


class OracleChat:
    """A stand-in AI that always knows the right answer. It shows the best the AI step can add,
    and exercises the whole AI path; it is not a measure of any real model."""

    def __init__(self, truth: dict[int, TruePage]) -> None:
        self.truth = truth
        self.calls = 0

    def chat(self, messages: list[ChatMessage]) -> str:
        self.calls += 1
        req = json.loads(messages[-1].content)
        if "documents" in req:
            return json.dumps(
                {"names": {str(d["id"]): "Named by the AI" for d in req["documents"]}}
            )
        ids = [p["id"] for p in req["pages"]]
        by_doc: dict[str, list[int]] = {}
        unplaced = []
        for i in ids:
            tp = self.truth[i]
            if tp.kind in ("blank", "notes"):
                unplaced.append(i)
            else:
                by_doc.setdefault(tp.doc, []).append(i)
        docs = [
            {
                "pages": sorted(v, key=lambda i: self.truth[i].index),
                "name": "",
                "type": self.truth[v[0]].kind,
                "date": None,
                "confidence": 90,
                "reasons": ["The text reads on from page to page"],
            }
            for v in by_doc.values()
        ]
        return json.dumps({"documents": docs, "unplaced": unplaced})

    def chat_stream(self, messages: list[ChatMessage]):
        yield self.chat(messages)


# ---------------------------------------------------------------- Real scans

# How the pages of real documents are fed to the assembler. The documents always come in a
# random order; "in_order" keeps each one's pages in reading order, as if scanned one document
# at a time, "swapped" also feeds two neighbouring pages through the wrong way round here and
# there, and "shuffled" mixes every page, so only what's on the pages can put them together.
ORDERS = ("in_order", "swapped", "shuffled")


def pdf_answers(conn: sqlite3.Connection) -> list[list[int]]:
    """An answer key from assembled PDFs: each read PDF of two or more pages is one document,
    its pages in the PDF's order."""
    docs = []
    for (scan,) in conn.execute(
        "SELECT id FROM scans WHERE mime_type = 'application/pdf' AND status = 'read' ORDER BY id"
    ).fetchall():
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT p.id FROM pages p"
                " JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1"
                " WHERE p.scan_id = ? ORDER BY p.page_index",
                (scan,),
            )
        ]
        if len(ids) >= 2:
            docs.append(ids)
    return docs


def arrange(docs: list[list[int]], order: str, seed: int) -> list[tuple[int, int, int]]:
    """(page id, document number, position in it) in the order the pages are fed in."""
    r = random.Random(seed)
    numbered = list(enumerate(docs))
    r.shuffle(numbered)
    out = [(pid, d, i) for d, ids in numbered for i, pid in enumerate(ids)]
    if order == "swapped":
        i = 0
        while i < len(out) - 1:
            if out[i][1] == out[i + 1][1] and r.random() < 0.2:
                out[i], out[i + 1] = out[i + 1], out[i]
                i += 1
            i += 1
    elif order == "shuffled":
        r.shuffle(out)
    return out


def load_real(
    conn: sqlite3.Connection, src: sqlite3.Connection, arranged: list[tuple[int, int, int]]
) -> dict[int, TruePage]:
    """Copy real pages, with their readings and image checks, into a bench database as loose
    scans named scan_0001.jpg, scan_0002.jpg... in the arranged order."""
    truth = {}
    with conn:
        for n, (pid, doc, index) in enumerate(arranged, 1):
            p = src.execute(
                "SELECT p.*, t.text, t.words, t.confidence FROM pages p"
                " JOIN transcriptions t ON t.page_id = p.id AND t.is_current = 1 WHERE p.id = ?",
                (pid,),
            ).fetchone()
            name = f"scan_{n:04d}.jpg"
            scan = conn.execute(
                "INSERT INTO scans (sha256, original_name, source_path, origin, import_mode,"
                " status, page_count, imported_at)"
                " VALUES (?, ?, ?, 'watched', 'copy', 'read', 1, datetime('now', ?))",
                (f"real:{pid}:{n}", name, f"D:/Scans/{name}", f"+{n} seconds"),
            ).lastrowid
            page = conn.execute(
                "INSERT INTO pages (scan_id, width_px, height_px, dpi, color_mode, phash,"
                " paper_color, blank_score, detected_rotation, user_rotation, script)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    scan,
                    *(
                        p[k]
                        for k in (
                            "width_px",
                            "height_px",
                            "dpi",
                            "color_mode",
                            "phash",
                            "paper_color",
                            "blank_score",
                            "detected_rotation",
                            "user_rotation",
                            "script",
                        )
                    ),
                ),
            ).lastrowid
            conn.execute(
                "INSERT INTO transcriptions (page_id, source, text, confidence, words, is_current)"
                " VALUES (?, 'tesseract', ?, ?, ?, 1)",
                (page, p["text"], p["confidence"], p["words"]),
            )
            truth[page] = TruePage(p["text"], f"real{doc}", index, "page")
    return truth


def score_groups(groups, truth: dict[int, TruePage]) -> tuple[float, float, float, float, float]:
    """How good the rules' proposal is before any confidence threshold: pair precision, recall
    and F1, the share of true documents proposed exactly, and of those the share in order."""
    pred = {p.id: i for i, g in enumerate(groups) if not g.set_aside for p in g.pages}
    real = {pid: tp.doc for pid, tp in truth.items() if tp.kind not in ("blank", "notes")}
    ids = sorted(real)
    t_pairs = {(a, b) for a, b in combinations(ids, 2) if real[a] == real[b]}
    p_pairs = {
        (a, b) for a, b in combinations(ids, 2) if a in pred and b in pred and pred[a] == pred[b]
    }
    hit = len(t_pairs & p_pairs)
    precision = hit / len(p_pairs) if p_pairs else 1.0
    recall = hit / len(t_pairs) if t_pairs else 1.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    docs: dict[str, list[int]] = {}
    for pid in ids:
        docs.setdefault(real[pid], []).append(pid)
    exact = ordered = 0
    for members in docs.values():
        g = next((g for g in groups if members[0] in [p.id for p in g.pages]), None)
        if g and sorted(p.id for p in g.pages) == sorted(members):
            exact += 1
            ordered += [truth[p.id].index for p in g.pages] == sorted(
                truth[i].index for i in members
            )
    return precision, recall, f1, exact / max(1, len(docs)), ordered / max(1, exact)
