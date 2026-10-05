"""Ask Lindley: whether it can answer, its conversations, and answers as they're written.

POST /api/chat answers with server-sent events (lindley.ask.answer has what each one is). The
answer is worked out on a thread of its own, start to end, and its events passed to the
response: a person who closes the pane or presses Stop ends the stream, and the answer with it.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Annotated

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel, Field

from lindley.api.deps import Conn
from lindley.ask import conversation
from lindley.ask.answer import WENT_WRONG, answer
from lindley.ask.retrieve import Scope
from lindley.ask.status import chat_status
from lindley.db.database import connect

router = APIRouter(prefix="/chat", tags=["chat"])

Event = tuple[str, dict]
_END = None


@router.get("/status")
def status(request: Request) -> dict:
    return dict(chat_status(request.app.state.settings))


@router.get("/conversations")
def conversations(conn: Conn) -> dict:
    return {"conversations": conversation.chats(conn)}


@router.get("/conversations/{chat_id}")
def one_conversation(chat_id: int, conn: Conn) -> dict:
    row = conn.execute(
        "SELECT id, title, created_at FROM chats WHERE id = ?", (chat_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, "That conversation isn't there any more")
    return dict(row) | {"messages": conversation.messages(conn, chat_id)}


@router.delete("/conversations/{chat_id}")
def delete_conversation(chat_id: int, conn: Conn) -> dict:
    if not conversation.delete(conn, chat_id):
        raise HTTPException(404, "That conversation isn't there any more")
    return {"deleted": chat_id}


class Opened(BaseModel):
    document_id: int | None = None
    page_id: int | None = None


class Question(BaseModel):
    question: str = Field(min_length=1, max_length=4000)
    chat_id: int | None = None
    scope: Opened | None = None
    looking: str | None = Field(default=None, max_length=300)  # "Looking at:", as the pane says


def asked(body: Question, request: Request) -> Iterator[Event]:
    """The answer to come, once the question can be asked: a refusal is an HTTP error, before
    the stream starts."""
    settings = request.app.state.settings
    st = chat_status(settings)
    if st["state"] != "ready":
        raise HTTPException(409, f"Ask Lindley can't answer questions: {st['reason']}")
    if not body.question.strip():
        raise HTTPException(422, "Ask a question first")
    if body.chat_id is not None:
        conn = connect(settings.db_path, any_thread=True)
        try:
            if not conversation.exists(conn, body.chat_id):
                raise HTTPException(404, "That conversation isn't there any more")
        finally:
            conn.close()
    scope = Scope(**body.scope.model_dump()) if body.scope else Scope()
    return answer(settings, body.question.strip(), body.chat_id, scope, body.looking)


@router.post("", response_class=EventSourceResponse)
async def ask(work: Annotated[Iterator[Event], Depends(asked)]) -> AsyncIterator[ServerSentEvent]:
    events: queue.Queue = queue.Queue()
    stop = threading.Event()

    def run() -> None:
        try:
            for event in work:
                events.put(event)
                if stop.is_set():
                    break  # closing `work` stops the AI, and saves what it wrote
        except Exception:  # noqa: BLE001 - answer() logs and saves its own failures
            events.put(("error", {"message": WENT_WRONG}))
        finally:
            work.close()
            events.put(_END)

    threading.Thread(target=run, name="lindley-ask", daemon=True).start()
    try:
        while True:
            event = await anyio.to_thread.run_sync(events.get, abandon_on_cancel=True)
            if event is _END:
                break
            yield ServerSentEvent(event=event[0], data=event[1])
    finally:
        stop.set()
