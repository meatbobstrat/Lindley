"""What Ask Lindley sends the AI: its instructions, the conversation so far, and the pages found
for the question, numbered so the answer can cite them as [n]."""

from __future__ import annotations

from html import escape

from lindley.ask.retrieve import Source
from lindley.providers.base import ChatMessage

SYSTEM = """You are Lindley, helping a person with their archive of scanned letters, papers and \
typescripts. Answer their question from the pages Lindley found for it, given with the question.

- Use only what the pages say. If they don't answer the question, say so plainly, and say what \
they do show if that helps. Never make up names, dates or events.
- Cite the pages each statement comes from by their number, like [2] or [1][3]. Cite only the \
numbers given.
- The pages were read by OCR, or by an AI from handwriting, so expect misspellings and odd line \
breaks. A page marked unsure="yes" was read with low confidence and nobody has checked it yet: \
if your answer rests on one, say it may have been misread.
- Answer briefly, in plain words, in the language of the question. No headings."""


def pages_block(sources: list[Source]) -> str:
    if not sources:
        return "<pages>\nNo pages were found for this question.\n</pages>"
    parts = []
    for s in sources:
        unsure = ' unsure="yes"' if s.unsure else ""
        parts.append(f'<page n="{s.n}" title="{escape(s.label)}"{unsure}>\n{s.text}\n</page>')
    return "<pages>\n" + "\n".join(parts) + "\n</pages>"


def messages(
    question: str,
    sources: list[Source],
    history: list[tuple[str, str]],
    looking: str | None = None,
) -> list[ChatMessage]:
    """`history`: the conversation's earlier messages, (role, text), oldest first. The pages go
    with the new question only: earlier ones were found for earlier questions."""
    out = [ChatMessage("system", SYSTEM)]
    out += [ChatMessage("user" if role == "user" else "assistant", text) for role, text in history]
    open_now = f"The person has open: {looking}\n\n" if looking else ""
    out.append(ChatMessage("user", f"{pages_block(sources)}\n\n{open_now}Question: {question}"))
    return out
