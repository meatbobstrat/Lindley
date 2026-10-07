"""Whether page B carries straight on from page A: a question for a small local model (the
`continues` job, lm_continues in the evidence).

The rules' runs_on only sees that A stops mid-sentence and B starts mid-sentence, which in a
typescript is nearly every page. A model shown A's last lines and B's first lines can tell
whether the sentence really runs on: on the 23 documents of the dev library, Qwen3.5 4B with the
rules ranked the pairs that go together 0.94 (AUC) where runs_on fires, against 0.77-0.82 for the
rules alone (design/database.md, "Local models"). Its answer is its chance of saying yes, read
from its token probabilities (p_yes), so only an AI that gives those can do this job.

It's asked about the pairs the rules are unsure of, and at most MOST a run: each takes a few
seconds on a laptop. Answers are kept by the text asked about (Answers.chance), so a pair is
asked once however many times the Inbox is sorted, and the rest are asked on later runs.
"""

from __future__ import annotations

import logging

from lindley.assembler.answers import Answers
from lindley.assembler.clues import parse_marker, text_lines, trim_noise
from lindley.assembler.evidence import pair
from lindley.assembler.model import Page
from lindley.assembler.segment import rescans, scan_order
from lindley.providers.base import ProviderError

log = logging.getLogger(__name__)

SYSTEM = (
    "You check scanned pages of old typed and handwritten documents. You are shown the last "
    "lines of page A and the first lines of page B. Answer yes if page B is the very next page "
    "of the same text as page A, so the writing carries straight on from A to B. Answer no if B "
    "starts something else or belongs somewhere else. Answer with one word: yes or no."
)
LINES = 4  # lines shown from each page (more didn't help on the bench)
UNSURE = (0.15, 0.85)  # pairs the rules score in here are asked about
MOST = 40  # questions a run at most


def body(p: Page) -> list[str]:
    """The page's lines, without specks at the edges or a page number."""
    lines = [ln.text for ln in trim_noise(text_lines(p.text, p.words))]
    for i in (0, -1):
        if lines and parse_marker(lines[i]):
            lines.pop(i)
    return [ln for ln in lines if ln.strip()]


def question(a: Page, b: Page, n: int = LINES) -> str:
    end, start = body(a)[-n:], body(b)[:n]
    return "Page A ends:\n" + "\n".join(end) + "\n\nPage B starts:\n" + "\n".join(start)


def unsure_pairs(pages: list[Page]) -> list[tuple[Page, Page]]:
    """Pairs worth asking about, the least sure first: pages scanned one after the other that
    the rules can't call, and pages scanned apart that the rules might join because one stops
    mid-sentence and the other starts mid-sentence."""
    ordered = scan_order(pages)
    again = rescans(ordered)
    stream = [p for p in ordered if p.id not in again and body(p)]
    found: dict[tuple[int, int], tuple[float, Page, Page]] = {}
    for a, b in zip(stream, stream[1:], strict=False):
        score = pair(a, b).score
        if UNSURE[0] <= score <= UNSURE[1]:
            found[(a.id, b.id)] = (score, a, b)
    ends = [p for p in stream if p.clues.ends_mid and not p.clues.ends_doc]
    starts = [p for p in stream if p.clues.starts_mid and not p.clues.starts_doc]
    for a in ends:
        for b in starts:
            if a is b or (a.id, b.id) in found:
                continue
            score = pair(a, b).score
            if UNSURE[0] <= score <= UNSURE[1]:
                found[(a.id, b.id)] = (score, a, b)
    ranked = sorted(found.values(), key=lambda x: (abs(x[0] - 0.5), x[1].id, x[2].id))
    return [(a, b) for _, a, b in ranked]


def judge(pages: list[Page], judge, answers: Answers, most: int | None = MOST) -> int:
    """Put the model's chance that each unsure pair runs on onto the pages (Page.runs_into),
    from earlier answers, and from `judge` for at most `most` new questions (None: as many as
    there are). Without `judge`, earlier answers only. Returns the questions asked. A judge
    that fails is asked nothing more this run: the rules sort without it."""
    todo = unsure_pairs(pages)
    known = []
    for a, b in todo:
        try:
            chance = answers.chance(None, SYSTEM, question(a, b), [a.id, b.id])
        except ProviderError:
            continue
        if chance is None:
            known.append((a, b))
        else:
            a.runs_into[b.id] = chance
    if judge is None or not known:
        return 0
    asking = known if most is None else known[:most]
    answers.expect(len(asking))
    asked = 0
    for a, b in asking:
        try:
            a.runs_into[b.id] = answers.chance(judge, SYSTEM, question(a, b), [a.id, b.id])
        except ProviderError as e:
            log.warning("Checking whether pages carry on failed, so the rules sort alone: %s", e)
            break
        finally:
            asked += 1
    return asked
