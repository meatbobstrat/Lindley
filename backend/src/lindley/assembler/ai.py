"""Ask the chat AI about the hard parts: uncertain breaks, page order and names.

The AI only ever sees page text and the rules' clues, never images, and only for pages the
rules couldn't settle. Its reply is checked strictly; anything off and the rules' answer stands.
The AI never writes to the database. With `answers` (lindley.assembler.answers), a question asked
before is answered from the earlier reply, with no call.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from lindley.assembler.answers import Answers
from lindley.assembler.model import Group, Page
from lindley.providers.base import ChatMessage, ChatProvider

SYSTEM = """You sort scanned pages from a family or local-history archive into documents.
You get some pages, each with an id, the start and end of its text, and clues found by simple rules.
You also get the rules' own proposal, which may be wrong.

Decide which pages belong together as one document, and the reading order of each document.
Reply with JSON only, no other text, in exactly this shape:
{"documents": [{"pages": [ids in reading order], "name": "short descriptive name",
  "type": "letter|receipt|deed|diary|notes|other", "date": "YYYY or YYYY-MM or YYYY-MM-DD or null",
  "confidence": 0-100, "reasons": ["short plain sentence", ...]}],
 "unplaced": [ids you can't place with any confidence]}

Rules: use only the ids given; use each id exactly once, in a document or in "unplaced";
reasons are read by the archive's owner, so be concrete ("Page 2 ends mid-sentence and page 3
finishes it") and never invent facts that aren't in the text."""

NAME_SYSTEM = """You name documents in a family or local-history archive.
For each document you get its id and the start of its text. Reply with JSON only:
{"names": {"<id>": "short descriptive name, e.g. Letter from Will to his mother, March 1892"}}"""


@dataclass
class AiResult:
    groups: list[Group] | None  # None: the reply was rejected, or there was none
    problem: str = ""  # why it was rejected; empty when the AI wasn't asked


def _page_json(p: Page) -> dict:
    c, t = p.clues, p.text.strip()
    clues = {
        k: v
        for k, v in {
            "page_number": f"{c.marker[0]}"
            + (f" of {c.marker[1]}" if c.marker and c.marker[1] else "")
            if c.marker
            else None,
            "greeting": c.salutation,
            "closing": c.closing,
            "signature": c.signature,
            "letterhead": c.letterhead or c.heading,
            "dates": [d[1] for d in c.dates][:3] or None,
            "looks_like": c.kind,
            "ends_mid_sentence": c.ends_mid or None,
            "starts_mid_sentence": c.starts_mid or None,
        }.items()
        if v
    }
    return {
        "id": p.id,
        "file": p.file_name,
        "start": t[:300],
        "end": t[-300:] if len(t) > 600 else "",
        "clues": clues,
    }


def _parse(text: str) -> dict | None:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _clean_reasons(v: object) -> list[str]:
    return (
        [str(r)[:200] for r in v if isinstance(r, str | int | float)][:5]
        if isinstance(v, list)
        else []
    )


def _ask(
    chat: ChatProvider | None,
    answers: Answers | None,
    purpose: str,
    system: str,
    question: dict,
    extra: dict,
    page_ids: list[int],
) -> str | None:
    messages = [ChatMessage("system", system), ChatMessage("user", json.dumps(question | extra))]
    if answers:
        return answers.ask(chat, purpose, messages, question, page_ids)
    return chat.chat(messages) if chat else None


def refine(
    chat: ChatProvider | None,
    pages: list[Page],
    proposal: list[Group],
    answers: Answers | None = None,
) -> AiResult:
    """Ask the AI to group and order these pages. Checks the reply before trusting it.
    Without `chat`, only an earlier reply in `answers` is used."""
    by_id = {p.id: p for p in pages}
    question = {"pages": [_page_json(p) for p in pages]}
    proposed = {"rules_proposal": [{"pages": g.ids, "confidence": g.confidence} for g in proposal]}
    try:
        reply = _ask(chat, answers, "assemble", SYSTEM, question, proposed, list(by_id))
    except Exception as e:  # noqa: BLE001 - any provider failure means "use the rules"
        return AiResult(None, f"the AI call failed: {e}")
    if reply is None:
        return AiResult(None)
    data = _parse(reply)
    if not data or not isinstance(data.get("documents"), list):
        return AiResult(None, "the reply wasn't the JSON asked for")

    seen: set[int] = set()
    groups: list[Group] = []
    for d in data["documents"]:
        ids = d.get("pages") if isinstance(d, dict) else None
        if not isinstance(ids, list) or not ids or not all(isinstance(i, int) for i in ids):
            return AiResult(None, "a document had no valid page list")
        if any(i not in by_id for i in ids):
            return AiResult(None, "the reply used a page id it wasn't given")
        if seen & set(ids) or len(set(ids)) != len(ids):
            return AiResult(None, "the reply used a page twice")
        seen |= set(ids)
        conf = d.get("confidence")
        conf = int(conf) if isinstance(conf, int | float) and 0 <= conf <= 100 else 50
        name = str(d.get("name") or "").strip()[:120]
        date = (
            d.get("date")
            if isinstance(d.get("date"), str)
            and re.fullmatch(r"1[5-9]\d\d(-\d\d(-\d\d)?)?", d.get("date"))
            else None
        )
        g = Group(
            [by_id[i] for i in ids],
            min(conf, 95),
            _clean_reasons(d.get("reasons")),
            str(d.get("type") or "") or None,
            name,
            not name,
            date,
            True,
            by_ai=True,
        )
        groups.append(g)
    unplaced = data.get("unplaced", [])
    if not isinstance(unplaced, list) or any(i not in by_id or i in seen for i in unplaced):
        return AiResult(None, "the unplaced list was wrong")
    if seen | set(unplaced) != set(by_id):
        return AiResult(None, "the reply left pages out")
    for i in unplaced:
        groups.append(Group([by_id[i]], 0, ["The AI couldn't place this page"], by_ai=True))
    return AiResult(groups)


def suggest_names(
    chat: ChatProvider | None, groups: list[Group], answers: Answers | None = None
) -> dict[int, str]:
    """Better names for groups whose rules-made name is only a placeholder. Index -> name.
    Without `chat`, only an earlier reply in `answers` is used."""
    question = {
        "documents": [
            {"id": i, "start": g.pages[0].text.strip()[:400]} for i, g in enumerate(groups)
        ]
    }
    ids = [p.id for g in groups for p in g.pages]
    try:
        reply = _ask(chat, answers, "name", NAME_SYSTEM, question, {}, ids)
    except Exception:  # noqa: BLE001
        return {}
    data = _parse(reply or "") or {}
    names = data.get("names") if isinstance(data.get("names"), dict) else {}
    out = {}
    for k, v in names.items():
        if str(k).isdigit() and int(k) < len(groups) and isinstance(v, str) and v.strip():
            out[int(k)] = v.strip()[:120]
    return out
