"""Answering a question in Ask Lindley, as a series of events for the pane:

- ("chat", {chat_id, message_id}): the question is saved, in this conversation
- ("sources", {sources}): the pages found for it, sent with it to the AI
- ("text", {text}): the answer, a piece at a time, as the AI writes it
- ("done", {status, message_id}), or ("error", {message, message_id}): the answer is saved

The AI is never waited for while the database is held: the question is saved and committed
first, the pages are only read, and the answer and the calls it took are saved once it's over.
A question is always sent, whatever the connection's `allow`: asking is a person's OK.

Run it on one thread from start to end: what each call used is noted per thread (`metered`).
Closing it part-way (a person pressed Stop) closes the AI's answer too, and saves what came.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from lindley.ask import conversation, prompt
from lindley.ask.retrieve import Scope, gather, question_words, search_words
from lindley.ask.status import budget
from lindley.config import Settings
from lindley.db.database import connect
from lindley.providers import allowance
from lindley.providers.base import ProviderError, Usage
from lindley.providers.registry import get_provider
from lindley.providers.throttle import metered

log = logging.getLogger(__name__)

Event = tuple[str, dict]

WENT_WRONG = "Something went wrong while answering. Nothing more was sent to the AI."


def answer(
    settings: Settings,
    question: str,
    chat_id: int | None = None,
    scope: Scope | None = None,
    looking: str | None = None,
) -> Iterator[Event]:
    scope = scope or Scope()
    conn = connect(settings.db_path, any_thread=True)
    try:
        yield from _answer(conn, settings, question, chat_id, scope, looking)
    finally:
        conn.close()


def _answer(conn, settings, question, chat_id, scope, looking) -> Iterator[Event]:
    opened = {k: v for k, v in vars(scope).items() if v is not None} or None
    chat_id, question_id = conversation.add_question(conn, chat_id, question, opened)
    yield "chat", {"chat_id": chat_id, "message_id": question_id}

    name = settings.ai.connection_for("chat")
    text, sources, status, error, model = "", [], "failed", None, None
    worked = failed = 0
    used: list[Usage] = []
    stream = None
    try:
        chat = get_provider(settings.ai, "chat")
        model = chat.model
        with metered(chat) as used:
            earlier, cited = conversation.last_exchange(conn, chat_id, question_id)
            try:
                words = search_words(chat, question, earlier)
                worked += 1
            except ProviderError as e:
                failed += 1
                if not e.answered:  # it can't be reached: it can't answer either
                    raise
                words = question_words(question)  # it answered, but not with words
            found = gather(conn, words, scope, budget(settings), settings.ocr.review_below, cited)
            sources = [s.brief() for s in found]
            yield "sources", {"sources": sources}

            history = conversation.history(conn, chat_id, question_id, settings.ask.history_turns)
            stream = chat.chat_stream(prompt.messages(question, found, history, looking))
            failed += 1  # until it's over
            for piece in stream:
                text += piece
                yield "text", {"text": piece}
            failed, worked, status = failed - 1, worked + 1, "done"
    except ProviderError as e:
        error = str(e)
    except GeneratorExit:
        if stream is not None:  # it had started: it answered, part of the way
            failed, worked = failed - 1, worked + 1
        status = "stopped"
        raise
    except Exception:
        log.exception("Ask Lindley failed to answer")
        error = WENT_WRONG
    finally:
        if stream is not None:
            stream.close()  # a stop part-way: the AI is told to stop writing
        if status == "failed":
            text = error or WENT_WRONG
        with conn:
            answer_id = conversation.add_answer(conn, chat_id, text, sources, status, name, model)
        if worked or failed or used:
            allowance.record_run(conn, name, "chat", False, worked, failed, used)
    if status == "failed":
        yield "error", {"message": text, "message_id": answer_id}
    else:
        yield "done", {"status": status, "message_id": answer_id}
