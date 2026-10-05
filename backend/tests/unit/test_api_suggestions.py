import json

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


def test_a_group_the_ai_checked_is_offered_from_assembler_offer_at(client, settings):
    conn, _ = inbox(client, settings)
    [h] = client.get("/api/suggestions").json()["suggestions"]
    assert h["payload"]["checked_by_ai"] is False and h["offer"] is False  # the rules'
    offer_at = settings.assembler.offer_at
    for by_ai, confidence, offered in ((True, offer_at, True), (True, offer_at - 1, False)):
        payload = {**h["payload"], "checked_by_ai": by_ai}
        with conn:
            conn.execute(
                "UPDATE suggestions SET payload = ?, confidence = ? WHERE id = ?",
                (json.dumps(payload), confidence, h["id"]),
            )
        [h2] = client.get("/api/suggestions").json()["suggestions"]
        assert h2["offer"] is offered


def test_every_group_offered_is_accepted_at_once_and_undone_at_once(client, settings):
    conn, ids = inbox(client, settings)
    [h] = client.get("/api/suggestions").json()["suggestions"]
    half = len(ids) // 2
    with conn:
        for i, part in enumerate((ids[:half], ids[half:])):
            payload = json.dumps({**h["payload"], "pages": part, "checked_by_ai": True})
            if i == 0:
                conn.execute(
                    "UPDATE suggestions SET payload = ?, confidence = 70 WHERE id = ?",
                    (payload, h["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO suggestions (kind, page_id, payload, confidence, reasons)"
                    " VALUES ('group_pages', ?, ?, 65, '[]')",
                    (part[0], payload),
                )
    done = client.post("/api/suggestions/accept-offers").json()
    assert len(done["documents"]) == 2 and sorted(done["pages"]) == sorted(ids)
    assert client.post("/api/suggestions/accept-offers").status_code == 404
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
    calls = conn.execute("SELECT purpose, automatic, model, output_tokens > 0 FROM ai_calls")
    assert [tuple(r) for r in calls] == [("assemble", 0, "fake", 1)]  # with what it used
    again = client.post("/api/assembler/ask", json={"page_ids": ids[:1]}).json()
    assert again["ai_calls"] == 0 and again["reused"] == 1


def test_asking_with_no_ai_set_up_says_so(client, settings):
    settings.ai.jobs["assemble"].connection = None
    assert client.post("/api/assembler/ask", json={"page_ids": [1]}).status_code == 400
