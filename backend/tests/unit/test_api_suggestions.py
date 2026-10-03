from lindley.assembler import assemble
from lindley.assembler.bench import TruePage, load
from lindley.db.database import connect

from .test_decide import STORY


def inbox(client, settings):
    client.get("/api/health")  # the app has set up the database
    conn = connect(settings.db_path)
    ids = list(load(conn, [TruePage(t, "story", i, "page") for i, t in enumerate(STORY)]))
    assemble(conn)
    return conn, ids


def test_hints_are_listed_and_a_person_can_accept_one(client, settings):
    conn, ids = inbox(client, settings)
    [h] = client.get("/api/suggestions").json()["suggestions"]
    assert h["kind"] == "group_pages" and h["payload"]["pages"] == ids and h["reasons"]
    done = client.post(f"/api/suggestions/{h['id']}/accept").json()
    assert done["pages"] == ids and done["document_id"]
    assert client.post(f"/api/suggestions/{h['id']}/accept").status_code == 404
    assert client.post(f"/api/undo/{done['undo']}").status_code == 200
    assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 0


def test_a_dismissed_hint_is_gone(client, settings):
    conn, _ = inbox(client, settings)
    [h] = client.get("/api/suggestions").json()["suggestions"]
    assert client.post(f"/api/suggestions/{h['id']}/dismiss").json() == {"ok": True}
    assert client.get("/api/suggestions").json()["suggestions"] == []


def test_asking_the_ai_sends_the_pages_at_once_and_is_recorded_as_asked(client, settings):
    conn, ids = inbox(client, settings)
    body = client.post("/api/assembler/ask", json={"page_ids": ids[:1]}).json()
    assert body["ai_calls"] == 1 and body["rejected"]  # the fake AI's reply isn't sorting
    assert [tuple(r) for r in conn.execute("SELECT purpose, automatic FROM ai_calls")] == [
        ("assemble", 0)
    ]
    again = client.post("/api/assembler/ask", json={"page_ids": ids[:1]}).json()
    assert again["ai_calls"] == 0 and again["reused"] == 1


def test_asking_with_no_ai_set_up_says_so(client, settings):
    settings.ai.jobs["assemble"].connection = None
    assert client.post("/api/assembler/ask", json={"page_ids": [1]}).status_code == 400
