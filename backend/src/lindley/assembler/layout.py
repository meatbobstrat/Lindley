"""How the writing sits on a page: a fingerprint of the typewriter, the margins and the spacing.

Pages typed at one sitting, on one machine, with the same line spacing and line length, look
alike; a page from another document usually doesn't. Both are in character widths, so free of
the scan's size and resolution. Margins aren't compared: where the writing sits on a scan depends
on where the sheet lay on the glass and how the scan was cropped.
"""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median

from lindley.assembler.clues import Line, is_junk

MIN_LINES = 5  # fewer lines than this say too little about a page's layout


@dataclass(frozen=True)
class Layout:
    spacing: float  # from one line to the next, in character widths
    line_chars: float  # characters in a full line

    def differs(self, other: Layout) -> float:
        """How far apart two layouts are: about 1 for pages of different documents that happen
        to be typed alike, more for pages set out differently, near 0 for pages typed alike."""
        return max(
            abs(self.spacing - other.spacing) / max(0.25 * min(self.spacing, other.spacing), 0.3),
            abs(self.line_chars - other.line_chars)
            / max(0.12 * min(self.line_chars, other.line_chars), 4),
        )


def _quantile(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, round(q * (len(s) - 1))))]


def page_layout(lines: list[Line]) -> Layout | None:
    """The layout of a page read with word boxes, or None if there's too little to go on."""
    rows = [[w for w in ln.words if not is_junk(w)] for ln in lines]
    rows = [r for r in rows if len(r) >= 3]
    if len(rows) < MIN_LINES:
        return None
    sizes = [
        w["bbox"][2] / len(w["text"])
        for r in rows
        for w in r
        if len(w["text"]) >= 4 and w.get("conf", 100) >= 80
    ]
    if len(sizes) < 10:
        return None
    char = median(sizes)
    starts = [min(w["bbox"][0] for w in r) for r in rows]
    ends = [max(w["bbox"][0] + w["bbox"][2] for w in r) for r in rows]
    mids = sorted(sum(w["bbox"][1] + w["bbox"][3] / 2 for w in r) / len(r) for r in rows)
    steps = [b - a for a, b in zip(mids, mids[1:], strict=False) if b - a > char]
    if not steps:
        return None
    left, right = _quantile(starts, 0.2), _quantile(ends, 0.8)  # indents and short lines aside
    return Layout(median(steps) / char, (right - left) / char)
