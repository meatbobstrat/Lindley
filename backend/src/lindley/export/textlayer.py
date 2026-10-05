"""Where each word of a page's current reading sits on the scan, for a PDF's text layer.

Tesseract gives every word a box. A vision model's reading and a person's correction don't, so
their words are laid over Tesseract's boxes for the same page: a word Tesseract also read takes
its box, a word read differently takes the boxes of the words it replaces, and a word Tesseract
missed goes in the gap beside its neighbours. A page with no boxes at all is still searchable:
its lines are spaced down the page, not over the writing, and the page counts as unplaced.

Boxes are [x, y, w, h] in pixels of the upright page, as Tesseract reads it. A page turned, or
turned round, since it was read has its boxes turned with it (reframe).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher

Box = tuple[float, float, float, float]


@dataclass(frozen=True)
class Word:
    text: str
    box: Box


@dataclass
class PageText:
    words: list[Word]
    placed: bool  # False: there were no word boxes, so the words aren't over the writing


def page_text(
    conn: sqlite3.Connection,
    page_id: int,
    size: tuple[int, int],
    turned: tuple[int, bool] | None = None,
) -> PageText:
    """The words of a page's current reading, each with a box. `size` is the upright page's
    (width, height) in pixels, for a reading with no boxes to lay over. `turned`: how the page
    is turned now (rotation, turned round), for boxes read with it turned another way."""
    cur = conn.execute(
        "SELECT text, words, read_rotation, read_mirror FROM transcriptions"
        " WHERE page_id = ? AND is_current = 1",
        (page_id,),
    ).fetchone()
    if cur is None or not cur["text"].strip():
        return PageText([], True)
    if cur["words"]:
        return PageText(_turned(_boxed(json.loads(cur["words"])), cur, size, turned), True)
    tess = conn.execute(
        "SELECT words, read_rotation, read_mirror FROM transcriptions WHERE page_id = ?"
        " AND source = 'tesseract' AND words IS NOT NULL ORDER BY id DESC LIMIT 1",
        (page_id,),
    ).fetchone()
    if tess and (boxed := _boxed(json.loads(tess["words"]))):
        return PageText(align(cur["text"], _turned(boxed, tess, size, turned)), True)
    return PageText(lay_out(cur["text"], size), False)


def _turned(
    words: list[Word], reading: sqlite3.Row, size: tuple[int, int], now: tuple[int, bool] | None
) -> list[Word]:
    if now is None or reading["read_rotation"] is None:  # not known: as it's turned now
        return words
    was = (reading["read_rotation"], bool(reading["read_mirror"]))
    return [Word(w.text, reframe(w.box, size, was, now)) for w in words]


def reframe(box: Box, size: tuple[int, int], was: tuple[int, bool], now: tuple[int, bool]) -> Box:
    """A box on the page turned `was` (rotation clockwise, and turned round: as
    image.upright_page turns it, round first, then the rotation), moved to the page turned
    `now`, whose upright size is `size`."""
    if was == now:
        return box
    w, h = size
    scan = (h, w) if now[0] % 180 else (w, h)  # the scan as it is, neither turned nor round
    at = (scan[1], scan[0]) if was[0] % 180 else scan
    for _ in range((4 - was[0] // 90) % 4):  # undo `was`: turn it back...
        box, at = _quarter(box, at)
    if was[1]:  # ...and round again
        box = _round(box, at)
    if now[1]:  # then as `now`: round...
        box = _round(box, at)
    for _ in range(now[0] // 90 % 4):  # ...and turned
        box, at = _quarter(box, at)
    return box


def _quarter(box: Box, size: tuple[float, float]) -> tuple[Box, tuple[float, float]]:
    """A quarter turn clockwise, with the page (`size`: the page's before)."""
    x, y, w, h = box
    return (size[1] - y - h, x, h, w), (size[1], size[0])


def _round(box: Box, size: tuple[float, float]) -> Box:
    """Turned round left to right."""
    x, y, w, h = box
    return (size[0] - x - w, y, w, h)


def _boxed(words: list[dict]) -> list[Word]:
    return [
        Word(w["text"], tuple(float(v) for v in w["bbox"]))
        for w in words
        if w.get("text", "").strip() and w.get("bbox") and w["bbox"][2] > 0 and w["bbox"][3] > 0
    ]


def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def align(text: str, boxed: list[Word]) -> list[Word]:
    """Lay the words of `text` over the boxes of another reading of the same page."""
    if not boxed:
        raise ValueError("There are no boxes to lay the words over")
    words = text.split()
    sm = SequenceMatcher(None, [_norm(w) for w in words], [_norm(w.text) for w in boxed], False)
    out: list[Word] = []
    for op, i1, i2, j1, j2 in sm.get_opcodes():
        run = words[i1:i2]
        if op == "equal":
            out += [Word(t, b.box) for t, b in zip(run, boxed[j1:j2], strict=True)]
        elif op == "replace":
            out += _spread(run, [b.box for b in boxed[j1:j2]])
        elif op == "delete":  # words Tesseract missed (words it read alone are left out)
            before = boxed[j1 - 1].box if j1 > 0 else None
            after = boxed[j1].box if j1 < len(boxed) else None
            out += _spread(run, [_gap(before, after, sum(len(t) for t in run))])
    return out


def same_line(a: Box, b: Box) -> bool:
    overlap = min(a[1] + a[3], b[1] + b[3]) - max(a[1], b[1])
    return overlap > min(a[3], b[3]) / 2


def _gap(before: Box | None, after: Box | None, chars: int) -> Box:
    """Room for `chars` characters a reading has between two boxed words (or at one end)."""
    if before and after and same_line(before, after) and after[0] > before[0] + before[2]:
        x0 = before[0] + before[2]
        top = min(before[1], after[1])
        bottom = max(before[1] + before[3], after[1] + after[3])
        return (x0, top, after[0] - x0, bottom - top)
    near = before or after
    assert near is not None  # align() has boxes, so a missed word has a boxed neighbour
    x, y, w, h = near
    width = h * 0.5 * max(chars, 1)  # about half a line's height per character
    if before:
        return (x + w + h * 0.3, y, width, h)
    return (max(x - h * 0.3 - width, 0), y, width, h)


def _spread(run: list[str], slots: list[Box]) -> list[Word]:
    """Share the slots out among the words, by length, in reading order.

    The slots are taken end to end as one strip. Each word gets a stretch of it as long as its
    share of the characters, and is put in the slot that holds the middle of its stretch."""
    total = sum(s[2] for s in slots)
    chars = sum(len(t) + 1 for t in run)
    scale = total / chars
    out: list[Word] = []
    at = 0.0
    for t in run:
        a, b = at, at + len(t) * scale
        at += (len(t) + 1) * scale
        mid, off = (a + b) / 2, 0.0
        for s in slots:
            if mid < off + s[2] or s is slots[-1]:
                x0 = s[0] + max(a - off, 0.0)
                x1 = s[0] + min(b - off, s[2])
                out.append(Word(t, (x0, s[1], max(x1 - x0, 1.0), s[3])))
                break
            off += s[2]
    return out


def lay_out(text: str, size: tuple[int, int]) -> list[Word]:
    """Lines spaced evenly down the page, for a reading with no boxes to lay over."""
    width, height = size
    lines = [ln.split() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    margin = 0.05
    line_h = min(height * (1 - 2 * margin) / len(lines), height / 40)
    longest = max(sum(len(t) + 1 for t in ln) for ln in lines)
    char_w = min(line_h * 0.5, width * (1 - 2 * margin) / longest)
    out: list[Word] = []
    for i, ln in enumerate(lines):
        y = height * margin + i * line_h
        x = width * margin
        for t in ln:
            out.append(Word(t, (x, y, len(t) * char_w, line_h * 0.8)))
            x += (len(t) + 1) * char_w
    return out
