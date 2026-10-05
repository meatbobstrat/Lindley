"""Ask Lindley's API: whether it can answer, answers as server-sent events, and conversations."""

import json

from lindley.config import JobConfig

from .test_api_library import add_page, db


def ask(client, **body) -> list[tuple[str, dict]]:
    """POST /api/chat, read to the end: its events, as (event, data)."""
    events, kind = [], None
    with client.stream("POST", "/api/chat", json=body) as r:
        assert r.status_code == 200, r.read()
        assert r.headers["content-type"].startswith("text/event-stream")
        for line in r.iter_lines():
            if line.startswith("event: "):
                kind = line.removeprefix("event: ")
            elif line.startswith("data: "):
                events.append((kind, json.loads(line.removeprefix("data: "))))
    return events


def test_the_status_says_ask_lindley_is_ready(client):
    st = client.get("/api/chat/status").json()
    assert st["state"] == "ready" and st["connection"]["name"] == "local"


def test_a_connection_that_could_answer_is_offered(client):
    client.app.state.settings.ai.jobs["chat"] = JobConfig()
    st = client.get("/api/chat/status").json()
    assert st["state"] == "offer" and st["offers"][0]["name"] == "local"
    r = client.post("/api/chat", json={"question": "Who?"})
    assert r.status_code == 409 and "No AI is chosen" in r.json()["detail"]


def test_a_question_is_answered_as_it_s_written_and_kept(client, settings, tmp_path):
    conn = db(client, settings)
    p = add_page(conn, tmp_path, "Edith lived in Leeds.")
    events = ask(client, question="Where did Edith live?", scope={"page_id": p}, looking="Inbox")
    kinds = [k for k, _ in events]
    assert kinds[:2] == ["chat", "sources"] and kinds[-1] == "done"
    assert kinds.count("text") > 1  # in pieces
    assert events[1][1]["sources"][0]["page_id"] == p
    chat_id = events[0][1]["chat_id"]

    [c] = client.get("/api/chat/conversations").json()["conversations"]
    assert (c["id"], c["title"], c["questions"]) == (chat_id, "Where did Edith live?", 1)
    full = client.get(f"/api/chat/conversations/{chat_id}").json()
    q, a = full["messages"]
    assert q["scope"] == {"page_id": p} and a["status"] == "done"
    assert a["text"] == "".join(d["text"] for k, d in events if k == "text")
    calls = conn.execute("SELECT purpose, automatic FROM ai_calls").fetchall()
    assert [tuple(r) for r in calls] == [("chat", 0), ("chat", 0)]  # asking is the OK

    ask(client, question="And her brother?", chat_id=chat_id)
    assert len(client.get(f"/api/chat/conversations/{chat_id}").json()["messages"]) == 4

    assert client.delete(f"/api/chat/conversations/{chat_id}").json() == {"deleted": chat_id}
    assert client.get("/api/chat/conversations").json()["conversations"] == []
    assert client.get(f"/api/chat/conversations/{chat_id}").status_code == 404


def test_a_question_in_a_conversation_that_s_gone_is_refused(client):
    r = client.post("/api/chat", json={"question": "Who?", "chat_id": 99})
    assert r.status_code == 404
    assert client.post("/api/chat", json={"question": ""}).status_code == 422
    assert client.post("/api/chat", json={"question": "   "}).status_code == 422
