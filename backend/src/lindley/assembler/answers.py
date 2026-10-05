"""The AI's answers about pages, kept so the same question is never paid for twice.

The assembler runs whenever new scans settle, and pages it couldn't place stay in the Inbox, so
without this the same pages would be sent again on every run. A question is known by what the AI
is shown about the pages (their ids, text and clues) and the instructions it's given: change a
page's reading and it's asked again. The rules' own proposal is left out of the key, since it
shifts whenever new scans arrive beside the pages.

A reply that was checked and rejected is kept too: asking again would most likely get the same.
So is a call the AI answered, and was paid for, but whose answer can't be used (cut off, or it
declined): it fails the same way next time without a call. A call that failed with no answer (no
network, a refused key) isn't kept, so it's tried again next time.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sqlite3
from typing import Literal

from lindley.providers.base import ChatMessage, ChatProvider, ProviderError

Purpose = Literal["assemble", "name"]  # grouping pages, naming documents
FAILED = "lindley:failed:"  # a kept reply that's an answer that couldn't be used, and why


class AskFirst(Exception):  # noqa: N818 - a signal, not an error
    """Questions came up while a decision was being saved: put it back, ask them, decide again."""


class Answers:
    """The AI is never called while the database is held for a decision being saved (a write
    transaction is open): a call can take minutes, and everything else that writes would fail
    meanwhile. A question met then gets no answer yet. It's kept, the decision is put back
    (AskFirst), the questions are asked (ask_waiting), and the decision made again with them."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.calls = 0  # calls made to the AI, or to be made once the database is let go
        self.reused = 0  # questions answered from earlier replies
        self.failed = 0  # of the calls, those that failed: they answered nothing
        self._waiting: dict[str, tuple[ChatProvider, Purpose, list[ChatMessage], list[int]]] = {}
        self._fresh: set[str] = set()  # asked in this run: not reused when met again
        self._failed: dict[str, Exception] = {}  # failed in this run: fails the same way again

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
        None if there's none. A failing call raises, and isn't kept. While the database is held,
        a new question gets None, and waits to be asked (see the class)."""
        k = self.key(purpose, messages[0].content, question)
        if k in self._failed:
            raise self._failed[k]
        row = self.conn.execute("SELECT reply FROM ai_answers WHERE key = ?", (k,)).fetchone()
        if row:
            if k not in self._fresh:
                self.reused += 1
            if row[0].startswith(FAILED):
                raise ProviderError(row[0].removeprefix(FAILED), answered=True)
            return row[0]
        if chat is None:
            return None
        if self.conn.in_transaction:
            if k not in self._waiting:
                self.calls += 1
                self._waiting[k] = (chat, purpose, messages, page_ids)
            return None
        self.calls += 1
        return self._call(k, chat, purpose, messages, page_ids)

    def _call(
        self,
        k: str,
        chat: ChatProvider,
        purpose: Purpose,
        messages: list[ChatMessage],
        page_ids: list[int],
    ) -> str:
        self._fresh.add(k)
        try:
            reply = chat.chat(messages)
        except Exception as e:
            self.failed += 1
            self._failed[k] = e
            if getattr(e, "answered", False):  # paid for: not asked again
                self._keep(k, purpose, page_ids, FAILED + str(e))
            raise
        self._keep(k, purpose, page_ids, reply)
        return reply

    def _keep(self, k: str, purpose: Purpose, page_ids: list[int], reply: str) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO ai_answers (key, purpose, page_ids, reply)"
                " VALUES (?, ?, ?, ?)",
                (k, purpose, json.dumps(sorted(page_ids)), reply),
            )

    @property
    def waiting(self) -> bool:
        """Questions met while the database was held, not asked yet."""
        return bool(self._waiting)

    def ask_waiting(self) -> None:
        """Ask the questions met while the database was held: it mustn't be now. A call that
        fails is remembered, and fails the same way when the decision is made again."""
        assert not self.conn.in_transaction
        waiting, self._waiting = self._waiting, {}
        for k, (chat, purpose, messages, page_ids) in waiting.items():
            with contextlib.suppress(Exception):  # kept in _failed, and met again
                self._call(k, chat, purpose, messages, page_ids)

    def drop_waiting(self) -> None:
        """Leave the questions waiting unasked (they wait for a later run): never called."""
        self.calls -= len(self._waiting)
        self._waiting = {}
