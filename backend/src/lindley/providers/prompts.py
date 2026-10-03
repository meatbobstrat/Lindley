"""Prompts every connector shares, so each AI is asked the same way."""

from __future__ import annotations

TRANSCRIBE = """This is one scanned page from a family or local-history archive.
Write out all the text on it exactly as it is written. Keep the original spelling, punctuation,
capitals and line breaks; don't correct, modernise or translate anything.
Include everything written on the page: headings, page numbers, notes in the margins, stamps
and signatures.
Write [illegible] for a word you can't read, and put [?] after a word you're unsure of.
Reply with the page's text only, with no comments of your own. If nothing is written on the
page, reply with nothing."""


def transcribe_prompt(hints: str | None = None) -> str:
    return f"{TRANSCRIBE}\n\nWhat is known about this page: {hints}" if hints else TRANSCRIBE
