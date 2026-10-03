"""The AI's answers about pages, kept so the same question is never paid for twice.

The assembler runs whenever new scans settle, and pages it couldn't place stay in the Inbox, so
without this the same pages would be sent again on every run. A question is known by what the AI
is shown about the pages (their ids, text and clues) and the instructions it's given: change a
page's reading and it's asked again. The rules' own proposal is left out of the key, since it
shifts whenever new scans arrive beside the pages.

A reply that was checked and rejected is kept too: asking again would most likely get the same.
A call that failed (no network, a refused key) isn't kept, so it's tried again next time.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Literal

from lindley.providers.base import ChatMessage, ChatProvider

Purpose = Literal["assemble", "name"]  # grouping pages, naming documents


class Answers:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.calls = 0  # calls made to the AI
        self.reused = 0  # questions answered from earlier replies

    @staticmethod
    def key(purpose: Purpose, system: str, question: object) -> str:
        blob = json.dumps([purpose, system, question], sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def ask(
        self,
        chat: ChatProvider | None,
        purpose: Purpose,
        messages: list[ChatMessage],
        question: object,
        page_ids: list[int],
    ) -> str | None:
        """The AI's reply to `messages`, from an earlier reply to the same `question` if there
        is one. Without `chat` (the AI may not be called now), only an earlier reply is given;
        None if there's none. A failing call raises, and isn't kept."""
        k = self.key(purpose, messages[0].content, question)
        row = self.conn.execute("SELECT reply FROM ai_answers WHERE key = ?", (k,)).fetchone()
        if row:
            self.reused += 1
            return row[0]
        if chat is None:
            return None
        self.calls += 1
        reply = chat.chat(messages)
        # Kept at once, unless it's part of a decision being saved: that commits it, or not.
        on_its_own = not self.conn.in_transaction
        self.conn.execute(
            "INSERT OR REPLACE INTO ai_answers (key, purpose, page_ids, reply) VALUES (?, ?, ?, ?)",
            (k, purpose, json.dumps(sorted(page_ids)), reply),
        )
        if on_its_own:
            self.conn.commit()
        return reply
