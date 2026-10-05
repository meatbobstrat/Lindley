"""Ask Lindley's conversations, kept in the library (tables chats and chat_messages).

Each change is its own short transaction, committed at once: an answer is saved after the AI has
finished, never while it's writing (see lindley.ask.answer).
"""

from __future__ import annotations

import json
import sqlite3

TITLE_CHARS = 60


def title_of(question: str) -> str:
    """A conversation's title: its first question, cut at a word if it's long."""
    q = " ".join(question.split())
    if len(q) <= TITLE_CHARS:
        return q
    return q[:TITLE_CHARS].rsplit(" ", 1)[0].rstrip(",;:") + "…"


def exists(conn: sqlite3.Connection, chat_id: int) -> bool:
    return conn.execute("SELECT 1 FROM chats WHERE id = ?", (chat_id,)).fetchone() is not None


def add_question(
    conn: sqlite3.Connection, chat_id: int | None, question: str, scope: dict | None
) -> tuple[int, int]:
    """Save a person's question, in a new conversation unless `chat_id` is one: (chat_id,
    message id)."""
    with conn:
        if chat_id is None:
            chat_id = conn.execute(
                "INSERT INTO chats (title) VALUES (?)", (title_of(question),)
            ).lastrowid
        else:
            conn.execute("UPDATE chats SET updated_at = datetime('now') WHERE id = ?", (chat_id,))
        message_id = conn.execute(
            "INSERT INTO chat_messages (chat_id, role, text, scope) VALUES (?, 'user', ?, ?)",
            (chat_id, question, json.dumps(scope) if scope else None),
        ).lastrowid
    return chat_id, message_id


def add_answer(
    conn: sqlite3.Connection,
    chat_id: int,
    text: str,
    sources: list[dict],
    status: str,
    connection: str | None,
    model: str | None,
) -> int:
    """Save an answer (the caller commits, with the calls it took)."""
    conn.execute("UPDATE chats SET updated_at = datetime('now') WHERE id = ?", (chat_id,))
    return conn.execute(
        "INSERT INTO chat_messages (chat_id, role, text, sources, status, connection, model)"
        " VALUES (?, 'assistant', ?, ?, ?, ?, ?)",
        (chat_id, text, json.dumps(sources), status, connection, model),
    ).lastrowid


def history(
    conn: sqlite3.Connection, chat_id: int, before: int, turns: int
) -> list[tuple[str, str]]:
    """The conversation before message `before`, as (role, text), oldest first: its last `turns`
    messages, in whole question-and-answer pairs. A question whose answer failed is left out, so
    questions and answers take turns, as every AI requires."""
    rows = conn.execute(
        "SELECT role, text, status FROM chat_messages WHERE chat_id = ? AND id < ? ORDER BY id",
        (chat_id, before),
    ).fetchall()
    pairs: list[tuple[str, str]] = []
    question = None
    for r in rows:
        if r["role"] == "user":
            question = r["text"]
        elif question is not None and r["status"] != "failed" and r["text"].strip():
            pairs += [("user", question), ("assistant", r["text"])]
            question = None
    keep = turns // 2 * 2
    return pairs[-keep:] if keep else []


def last_exchange(
    conn: sqlite3.Connection, chat_id: int, before: int
) -> tuple[str | None, list[int]]:
    """The question before message `before`, and the pages its answer cited, for a follow-up."""
    q = conn.execute(
        "SELECT id, text FROM chat_messages WHERE chat_id = ? AND id < ? AND role = 'user'"
        " ORDER BY id DESC LIMIT 1",
        (chat_id, before),
    ).fetchone()
    if q is None:
        return None, []
    a = conn.execute(
        "SELECT text, sources FROM chat_messages WHERE chat_id = ? AND id > ? AND id < ?"
        " AND role = 'assistant' ORDER BY id LIMIT 1",
        (chat_id, q["id"], before),
    ).fetchone()
    cited: list[int] = []
    if a is not None and a["sources"]:
        for s in json.loads(a["sources"]):
            if f"[{s['n']}]" in a["text"]:
                cited.append(s["page_id"])
    return q["text"], cited


def chats(conn: sqlite3.Connection, limit: int = 100) -> list[dict]:
    """Conversations, the latest first."""
    return [
        dict(r)
        for r in conn.execute(
            "SELECT c.id, c.title, c.created_at, c.updated_at,"
            " (SELECT COUNT(*) FROM chat_messages m WHERE m.chat_id = c.id AND m.role = 'user')"
            " AS questions"
            " FROM chats c ORDER BY c.updated_at DESC, c.id DESC LIMIT ?",
            (limit,),
        )
    ]


def messages(conn: sqlite3.Connection, chat_id: int) -> list[dict]:
    out = []
    for r in conn.execute(
        "SELECT id, role, text, sources, scope, status, connection, model, created_at"
        " FROM chat_messages WHERE chat_id = ? ORDER BY id",
        (chat_id,),
    ):
        m = dict(r)
        m["sources"] = json.loads(m["sources"]) if m["sources"] else []
        m["scope"] = json.loads(m["scope"]) if m["scope"] else None
        out.append(m)
    return out


def delete(conn: sqlite3.Connection, chat_id: int) -> bool:
    with conn:
        conn.execute("DELETE FROM chat_messages WHERE chat_id = ?", (chat_id,))
        return conn.execute("DELETE FROM chats WHERE id = ?", (chat_id,)).rowcount > 0
