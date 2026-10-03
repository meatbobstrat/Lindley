"""What the assembler knows about a page, and the groups it proposes."""

from __future__ import annotations

from dataclasses import dataclass, field

from lindley.assembler.clues import PageClues, page_clues, text_lines
from lindley.assembler.layout import Layout, page_layout
from lindley.assembler.meaning import Vector, encode
from lindley.assembler.terms import Library, page_terms, weigh


@dataclass
class Page:
    id: int
    scan_id: int
    file_name: str
    text: str
    page_index: int = 0
    scanned_at: str | None = None
    imported_at: str = ""
    words: list[dict] | None = None
    height: int | None = None
    blank_score: float | None = None
    phash: str | None = None
    paper_color: str | None = None
    copies: frozenset[int] = frozenset()  # pages that look like this page scanned again
    width: int | None = None
    dpi: int | None = None
    color_mode: str | None = None  # rgb, gray, bilevel
    script: str | None = None  # handwritten, printed, typed, mixed, none
    ocr_conf: float | None = None  # the reading's confidence, 0-100
    modified_at: str | None = None  # the file's time: when it was scanned, if EXIF didn't say
    added_at: str | None = None  # when the page came into Lindley (UTC, as SQLite writes it)
    clues: PageClues = field(init=False)
    layout: Layout | None = field(init=False)
    # Rare words, weighed against the other pages being sorted (see weigh_terms)
    terms: dict[str, float] = field(init=False, default_factory=dict)
    topic: Vector | None = field(init=False, default=None)  # what it's about (meaning.py)

    def __post_init__(self) -> None:
        self.clues = page_clues(
            self.text, self.file_name, self.words, self.height, self.blank_score
        )
        self.layout = page_layout(text_lines(self.text, self.words), self.width)

    def scan_key(self) -> tuple:
        """Scanning order: the strongest single hint about which pages go together."""
        c = self.clues
        return (
            c.file_prefix,
            c.file_seq if c.file_seq is not None else 10**9,
            self.scanned_at or self.modified_at or self.imported_at,
            self.scan_id,
            self.page_index,
        )

    @property
    def when(self) -> str | None:
        """When it was scanned, as near as can be told."""
        return self.scanned_at or self.modified_at

    @property
    def size_in(self) -> tuple[float, float] | None:
        """The sheet's size in inches, upright: (width, height)."""
        if not (self.dpi and self.width and self.height):
            return None
        return self.width / self.dpi, self.height / self.dpi


def weigh_terms(pages: list[Page], library: Library | None = None) -> None:
    """Get pages ready to compare: weigh each page's words by how rare they are (in the whole
    library when it's given, else among these pages), and say what each is about, when there's
    a model for that."""
    for p, v in zip(pages, weigh([page_terms(p.text) for p in pages], library), strict=True):
        p.terms = v
    for p, t in zip(pages, encode([p.text for p in pages]), strict=True):
        p.topic = t


@dataclass
class Group:
    pages: list[Page]  # in reading order
    confidence: int  # 0-100: that these pages, and only these, belong together
    reasons: list[str] = field(default_factory=list)
    kind: str | None = None
    name: str = ""
    name_is_guess: bool = True  # a placeholder name the AI may improve on
    date: str | None = None
    order_settled: bool = True
    set_aside: bool = False  # a blank page or a stray note
    by_ai: bool = False
    features: dict[str, float] = field(default_factory=dict)  # what its confidence comes from

    @property
    def ids(self) -> list[int]:
        return [p.id for p in self.pages]
