"""What the assembler knows about a page, and the groups it proposes."""

from __future__ import annotations

from dataclasses import dataclass, field

from lindley.assembler.clues import PageClues, page_clues


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
    clues: PageClues = field(init=False)

    def __post_init__(self) -> None:
        self.clues = page_clues(
            self.text, self.file_name, self.words, self.height, self.blank_score
        )

    def scan_key(self) -> tuple:
        """Scanning order: the strongest single hint about which pages go together."""
        c = self.clues
        return (
            c.file_prefix,
            c.file_seq if c.file_seq is not None else 10**9,
            self.scanned_at or self.imported_at,
            self.scan_id,
            self.page_index,
        )


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

    @property
    def ids(self) -> list[int]:
        return [p.id for p in self.pages]
