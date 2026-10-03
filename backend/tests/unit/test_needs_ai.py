"""Needs AI: scans Lindley couldn't read or sort on its own, sent by a person or on its own."""

import json

import pytest

from lindley.assembler import assemble
from lindley.assembler.auto import read_on_its_own, sort_on_its_own
from lindley.assembler.bench import TruePage, load
from lindley.db.database import connect, init_db
from lindley.providers.connectors.fake import FakeProvider
from lindley.worker.intake import import_file
from lindley.worker.pipeline import Pipeline, waiting_for_vision

from .test_decide import STORY, Answers
from .test_pipeline import StubOcr, written_page


@pytest.fixture
def conn(settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


@pytest.fixture
def scan(conn, settings, tmp_path):
    """Import a one-page scan with some writing, each a different size so none are copies."""
    made = []

    def make(name):
        path = tmp_path / name
        written_page((400 + len(made), 560)).save(path)
        made.append(path)
        return import_file(conn, settings, path).scan_id

    return make


HARD = ("Dcar Sistcr", 41.0)  # Tesseract unsure: below ocr.confidence_threshold


def queue_hard_pages(conn, settings, scan, n=1):
    """Hard pages read while the vision model must ask first: they wait for it."""
    ids = [scan(f"s{i}.png") for i in range(n)]
    pipe = Pipeline(settings, StubOcr(*[HARD] * n), FakeProvider())
    for sid in ids:
        pipe.process_scan(conn, sid)
    assert waiting_for_vision(conn) == n
    return pipe


def story(conn):
    return list(load(conn, [TruePage(t, "story", i, "page") for i, t in enumerate(STORY)]))


def test_hard_pages_are_listed_and_a_person_can_send_them(client, settings, conn, scan):
    queue_hard_pages(conn, settings, scan)
    body = client.get("/api/needs-ai").json()
    assert body["count"] == 1
    [page] = body["read"]["pages"]
    assert page["confidence"] == 41.0 and not page["failed"] and page["document_id"] is None
    assert "OK" in page["why"]
    assert body["read"]["connection"]["allow"] == "ask"
    done = client.post("/api/needs-ai/read", json={}).json()
    assert done["read"] == 1 and done["waiting"] == 0
    assert client.get("/api/needs-ai").json()["read"]["pages"] == []
    calls = conn.execute("SELECT purpose, automatic FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("vision", 0)]  # a person's OK


def test_with_no_ai_to_read_with_sending_says_so(client, settings):
    settings.ai.jobs["vision"].connection = None
    assert client.post("/api/needs-ai/read", json={}).status_code == 400


def test_once_the_vision_model_may_run_on_its_own_waiting_pages_go_by_themselves(
    conn,
    settings,
    scan,
):
    pipe = queue_hard_pages(conn, settings, scan, n=2)
    assert read_on_its_own(conn, settings, pipe) is None  # it still has to ask
    cfg = settings.ai.providers["local"]
    cfg.allow, cfg.daily_limit = "auto", 1
    run = read_on_its_own(conn, settings, pipe)
    assert run.read == 1 and run.stopped  # no more than its limit allows
    assert waiting_for_vision(conn) == 1
    assert [r[0] for r in conn.execute("SELECT automatic FROM ai_calls")] == [1]
    assert read_on_its_own(conn, settings, pipe) is None  # the limit is used up


def test_pages_the_rules_cant_sort_wait_for_the_ai(conn):
    a, b = story(conn)
    report = assemble(conn)  # no AI may be called
    assert report.ai_waiting == 1
    [row] = conn.execute("SELECT * FROM needs_ai").fetchall()
    assert json.loads(row["pages"]) == [a, b]
    [guess] = json.loads(row["proposal"])
    assert guess["pages"] == [a, b] and guess["confidence"] == 53
    conn.execute("UPDATE needs_ai SET since = '2026-01-01 00:00:00'")
    conn.commit()
    assemble(conn)
    assert conn.execute("SELECT since FROM needs_ai").fetchone()[0] == "2026-01-01 00:00:00"


def test_an_ai_that_may_run_on_its_own_leaves_nothing_waiting(conn, settings):
    story(conn)
    settings.ai.providers["local"].allow = "auto"
    report = sort_on_its_own(conn, settings, Answers())
    assert report.ai_calls == 1 and report.documents_created == 1
    assert not conn.execute("SELECT 1 FROM needs_ai").fetchone()
    assert [tuple(r) for r in conn.execute("SELECT purpose, automatic FROM ai_calls")] == [
        ("assemble", 1)
    ]


def test_a_person_can_send_one_question_or_all_of_them(client, settings, conn):
    client.get("/api/health")
    a, b = story(conn)
    assemble(conn)
    [item] = client.get("/api/needs-ai").json()["sort"]["items"]
    assert [p["id"] for p in item["pages"]] == [a, b] and item["proposal"][0]["confidence"] == 53
    done = client.post(f"/api/needs-ai/{item['id']}/sort").json()
    assert done["ai_calls"] == 1  # the fake AI's reply isn't sorting, so it's rejected
    # The AI has looked at them now: they're left to the person, not asked about again
    assert client.get("/api/needs-ai").json()["sort"]["items"] == []
    assert client.post("/api/needs-ai/sort").status_code == 404
    assert client.post(f"/api/needs-ai/{item['id']}/sort").status_code == 404


def test_pages_a_person_placed_leave_the_queue(client, settings, conn):
    client.get("/api/health")
    story(conn)
    assemble(conn)
    [hint] = client.get("/api/suggestions").json()["suggestions"]
    client.post(f"/api/suggestions/{hint['id']}/accept")
    assert client.get("/api/needs-ai").json()["count"] == 0
