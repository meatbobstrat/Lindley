"""Needs AI: scans Lindley couldn't read or sort on its own, sent by a person or on its own."""

import json
import threading
from unittest.mock import patch

import pytest

from lindley import activity
from lindley.assembler import assemble
from lindley.assembler.auto import read_on_its_own, sort_on_its_own
from lindley.assembler.bench import TruePage, load
from lindley.db.database import connect, init_db
from lindley.providers import allowance
from lindley.providers.base import ProviderError, Usage
from lindley.providers.connectors.fake import FakeProvider
from lindley.worker.intake import import_file
from lindley.worker.pipeline import Pipeline, follow_settings, waiting_for_vision

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
    sent = client.post("/api/needs-ai/read", json={}).json()
    assert sent == {"queued": 1, "already": 0, "connection": "Fake AI for tests"}
    assert client.app.state.ai_work.wait_idle()
    assert client.get("/api/needs-ai").json()["read"]["pages"] == []
    calls = conn.execute("SELECT purpose, automatic FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("vision", 0)]  # a person's OK
    [done] = client.get("/api/overview").json()["ai"]["finished"]
    assert done["message"].startswith("The AI read 1 page.") and done["ok"]
    assert client.post("/api/needs-ai/read", json={}).status_code == 404  # nothing waits now


def test_a_person_can_send_a_page_under_review_though_it_reads_well_enough(
    client, settings, conn, scan
):
    """Read at 75: above ocr.confidence_threshold, so it doesn't wait for the AI, but below
    ocr.review_below. A page read better, or checked by a person, can't be sent."""
    fair, good, checked = scan("fair.png"), scan("good.png"), scan("checked.png")
    pipe = Pipeline(
        settings,
        StubOcr(("Dear Sistor", 75.0), ("Dear Sister", 85.0), ("Dear Sistor", 75.0)),
        FakeProvider(),
    )
    for sid in (fair, good, checked):
        pipe.process_scan(conn, sid)
    page = {
        sid: conn.execute("SELECT id FROM pages WHERE scan_id = ?", (sid,)).fetchone()[0]
        for sid in (fair, good, checked)
    }
    with conn:
        conn.execute(
            "UPDATE transcriptions SET confirmed_at = datetime('now') WHERE page_id = ?",
            (page[checked],),
        )
    assert waiting_for_vision(conn) == 0
    for sid in (good, checked):
        sent = client.post("/api/needs-ai/read", json={"page_ids": [page[sid]]})
        assert sent.status_code == 404
    sent = client.post("/api/needs-ai/read", json={"page_ids": [page[fair]]}).json()
    assert sent["queued"] == 1
    assert client.app.state.ai_work.wait_idle()
    source = conn.execute(
        "SELECT source FROM v_current_text WHERE page_id = ?", (page[fair],)
    ).fetchone()[0]
    assert source == "vision"
    assert [tuple(c) for c in conn.execute("SELECT purpose, automatic FROM ai_calls")] == [
        ("vision", 0)
    ]


def test_pages_on_their_way_to_the_ai_are_marked_and_not_sent_twice(client, settings, conn, scan):
    queue_hard_pages(conn, settings, scan, n=2)
    gate, inside = threading.Event(), threading.Event()
    fake = FakeProvider.transcribe

    def slow(self, image, hints=None):
        inside.set()
        assert gate.wait(10)
        return fake(self, image, hints)

    with patch.object(FakeProvider, "transcribe", slow):
        first = client.get("/api/needs-ai").json()["read"]["pages"][0]["page_id"]
        assert client.post("/api/needs-ai/read", json={"page_ids": [first]}).json()["queued"] == 1
        assert inside.wait(10)
        again = client.post("/api/needs-ai/read", json={}).json()
        assert (again["queued"], again["already"]) == (1, 1)  # only the other page is new
        pages = client.get("/api/needs-ai").json()["read"]["pages"]
        assert [p["sending"] for p in pages] == [True, True]
        overview = client.get("/api/overview").json()
        [working] = overview["ai"]["working"]
        assert working["kind"] == "read" and working["of"] == 1 and working["asked"]
        assert overview["ai"]["waiting"] == [{"kind": "read", "of": 1}]
        assert overview["counts"]["needs_ai"] == 0  # sent: no longer waiting for a person
        [state] = {p["state"] for p in client.get("/api/inbox").json()["pages"]} - {"needs_ai"}
        assert state == "ai_reading"
        gate.set()
        assert client.app.state.ai_work.wait_idle()
    overview = client.get("/api/overview").json()
    assert overview["ai"]["working"] == [] and len(overview["ai"]["finished"]) == 2


def test_a_page_in_two_questions_for_the_sorting_ai_counts_once(client, conn, scan):
    a, b, c = (
        conn.execute("SELECT id FROM pages WHERE scan_id = ?", (scan(f"{n}.png"),)).fetchone()[0]
        for n in "abc"
    )
    with conn:
        conn.executemany(
            "INSERT INTO needs_ai (pages, proposal, since) VALUES (?, '[]', datetime('now'))",
            [(json.dumps([a, b]),), (json.dumps([b, c]),)],
        )
    assert client.get("/api/overview").json()["counts"]["needs_ai"] == 3
    sent = client.post("/api/needs-ai/sort").json()
    assert sent["queued"] + sent["already"] == 3
    assert client.app.state.ai_work.wait_idle()


def test_pages_the_ai_didnt_manage_say_so(client, settings, conn, scan):
    queue_hard_pages(conn, settings, scan)

    def cut_off(self, image, hints=None):
        raise ProviderError("The AI stopped part way through its answer: it ran out of room")

    with patch.object(FakeProvider, "transcribe", cut_off):
        client.post("/api/needs-ai/read", json={})
        assert client.app.state.ai_work.wait_idle()
    [done] = client.get("/api/overview").json()["ai"]["finished"]
    assert done["message"] == (
        "The AI didn’t manage 1 page: The AI stopped part way through its answer:"
        " it ran out of room"
    )
    assert not done["ok"]
    [page] = client.get("/api/needs-ai").json()["read"]["pages"]
    assert page["failed"] and not page["sending"] and "ran out of room" in page["why"]


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


def test_a_new_threshold_counts_for_pages_read_before(conn, settings, scan):
    sid = scan("s.png")
    Pipeline(settings, StubOcr(("Dear Sister", 80.0)), FakeProvider()).process_scan(conn, sid)
    assert waiting_for_vision(conn) == 0  # read well enough at 70
    settings.ocr.confidence_threshold = 85
    assert follow_settings(conn, settings) == (1, 0)
    assert waiting_for_vision(conn) == 1
    settings.ocr.confidence_threshold = 75
    assert follow_settings(conn, settings) == (0, 1)
    assert waiting_for_vision(conn) == 0
    assert follow_settings(conn, settings) == (0, 0)


def test_a_threshold_saved_in_settings_takes_effect_at_once(client, conn, settings, scan):
    sid = scan("s.png")
    Pipeline(settings, StubOcr(("Dear Sister", 80.0)), FakeProvider()).process_scan(conn, sid)
    current = client.get("/api/settings").json()
    current["ocr"]["confidence_threshold"] = 85
    assert client.put("/api/settings", json=current).status_code == 200
    [page] = client.get("/api/needs-ai").json()["read"]["pages"]
    assert page["confidence"] == 80.0 and "OK" in page["why"]


def test_pages_in_a_completed_document_dont_wait_for_the_ai(conn, settings, scan):
    sid = scan("s.png")
    Pipeline(settings, StubOcr(("Dear Sister", 80.0)), FakeProvider()).process_scan(conn, sid)
    doc = conn.execute("INSERT INTO documents (name, status) VALUES ('Letter', 'complete')")
    conn.execute("UPDATE pages SET document_id = ?, position = 0", (doc.lastrowid,))
    conn.commit()
    settings.ocr.confidence_threshold = 85
    assert follow_settings(conn, settings) == (0, 0)  # done: not sent again
    conn.execute("UPDATE documents SET status = 'progress'")
    conn.commit()
    assert follow_settings(conn, settings) == (1, 0)  # reopened, it waits like any other
    conn.execute("UPDATE documents SET status = 'complete'")
    conn.commit()
    assert follow_settings(conn, settings) == (0, 1)
    assert waiting_for_vision(conn) == 0


def test_a_page_a_person_checked_no_longer_waits(client, conn, settings, scan):
    queue_hard_pages(conn, settings, scan)
    conn.execute("UPDATE transcriptions SET confirmed_at = datetime('now') WHERE is_current = 1")
    conn.commit()
    assert waiting_for_vision(conn) == 0
    assert client.get("/api/needs-ai").json()["count"] == 0
    assert follow_settings(conn, settings) == (0, 1)
    assert follow_settings(conn, settings) == (0, 0)  # and isn't queued again


def test_with_no_vision_model_hard_pages_wait_for_a_person_instead(conn, settings, scan):
    queue_hard_pages(conn, settings, scan)
    settings.ai.jobs["vision"].connection = None
    assert follow_settings(conn, settings) == (0, 1)
    assert waiting_for_vision(conn) == 0
    settings.ai.jobs["vision"].connection = "local"  # set up again: they wait for it again
    assert follow_settings(conn, settings) == (1, 0)
    assert waiting_for_vision(conn) == 1


def test_pages_the_rules_cant_sort_wait_for_the_ai(conn):
    a, b = story(conn)
    report = assemble(conn)  # no AI may be called
    assert report.ai_waiting == 1
    [row] = conn.execute("SELECT * FROM needs_ai").fetchall()
    assert json.loads(row["pages"]) == [a, b]
    [guess] = json.loads(row["proposal"])
    assert guess["pages"] == [a, b] and guess["confidence"] == 53
    assert guess["reasons"] and all(isinstance(r, str) for r in guess["reasons"])
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


def test_a_sorting_call_is_recorded_as_its_made_though_the_job_is_cut_short(conn, settings):
    story(conn)
    settings.ai.providers["local"].allow = "auto"
    closed = RuntimeError("Lindley closed")  # after the AI answered, before the job ended
    with (
        patch("lindley.assembler.apply.save_links", side_effect=closed),
        pytest.raises(RuntimeError),
    ):
        sort_on_its_own(conn, settings)
    calls = conn.execute("SELECT purpose, automatic, ok, model, output_tokens > 0 FROM ai_calls")
    assert [tuple(c) for c in calls] == [("assemble", 1, 1, "fake", 1)]  # with what it used


def test_a_sorting_call_paid_for_whose_answer_was_cut_off_is_one_failed_call(conn, settings):
    story(conn)
    settings.ai.providers["local"].allow = "auto"

    def cut_off(self, messages):
        self.on_usage(Usage("fake", 500, 4096))
        raise ProviderError("The AI stopped part way through its answer", answered=True)

    with patch.object(FakeProvider, "chat", cut_off):
        report = sort_on_its_own(conn, settings)
    assert (report.ai_calls, report.ai_failed) == (1, 1)
    calls = conn.execute("SELECT ok, output_tokens FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [(0, 4096)]


def test_a_person_can_send_one_question_or_all_of_them(client, settings, conn):
    client.get("/api/health")
    a, b = story(conn)
    assemble(conn)
    [item] = client.get("/api/needs-ai").json()["sort"]["items"]
    assert [p["id"] for p in item["pages"]] == [a, b] and item["proposal"][0]["confidence"] == 53
    assert client.post(f"/api/needs-ai/{item['id']}/sort").json()["queued"] == 2
    assert client.app.state.ai_work.wait_idle()
    [done] = client.get("/api/overview").json()["ai"]["finished"]
    assert done["message"].startswith("The AI sorted the pages: 0 new documents")
    calls = conn.execute("SELECT purpose, automatic FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("assemble", 0)]  # the fake's reply is rejected
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


class Down:
    """A sorting AI that can't be reached."""

    def __init__(self):
        self.calls = 0

    def chat(self, messages):
        self.calls += 1
        raise ProviderError("no route to host")


def test_failed_sorting_calls_are_recorded_as_failed_and_an_ai_that_keeps_failing_is_rested(
    conn, settings
):
    settings.ai.providers["local"].allow = "auto"
    settings.ai.providers["local"].daily_limit = 10
    down = Down()
    story(conn)
    for _ in range(3):  # a failed call isn't kept, so the same question is asked again
        report = sort_on_its_own(conn, settings, down)
        assert (report.ai_calls, report.ai_failed) == (1, 1)
    calls = conn.execute("SELECT purpose, automatic, ok FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("assemble", 1, 0)] * 3
    # Failed calls use up nothing of the day's limit, and after three the AI is left alone
    assert allowance.automatic_left(conn, settings, "local") == 10
    assert sort_on_its_own(conn, settings, down).ai_calls == 0 and down.calls == 3


def test_sorting_says_how_many_questions_it_has_asked_of_how_many(conn):
    story(conn)
    steps = []
    assemble(conn, chat=FakeProvider(), on_progress=lambda done, of: steps.append((done, of)))
    assert steps[0] == (0, 1) and steps[-1] == (1, 1)  # known before it's asked
    steps.clear()  # the fake's reply was rejected: the pages wait, and it's not asked again
    assemble(conn, chat=FakeProvider(), on_progress=lambda done, of: steps.append((done, of)))
    assert set(steps) == {(0, 0)}


def test_questions_the_ai_may_not_be_asked_arent_counted(conn):
    story(conn)
    steps = []
    report = assemble(
        conn, chat=FakeProvider(), max_ai_calls=0, on_progress=lambda *s: steps.append(s)
    )
    assert report.ai_calls == 0 and set(steps) == {(0, 0)}


def test_quiet_work_isnt_shown_until_theres_something_to_do():
    with activity.doing("sort", 0, asked=False, quiet=True) as step:
        assert activity.current() == []
        step(0, 2)
        [shown] = activity.current()
        assert (shown["done"], shown["of"], shown["pages"]) == (0, 2, 0) and "quiet" not in shown
    assert activity.current() == []


def test_sorting_on_its_own_shows_in_the_status_bar_while_it_asks(conn, settings):
    story(conn)
    settings.ai.providers["local"].allow = "auto"
    seen = []
    fake = FakeProvider.chat

    def watched(self, messages):
        seen.extend(activity.current())
        return fake(self, messages)

    with patch.object(FakeProvider, "chat", watched):
        sort_on_its_own(conn, settings)
    [entry] = seen
    assert entry["kind"] == "sort" and not entry["asked"] and (entry["done"], entry["of"]) == (0, 1)
    assert activity.current() == []


def test_the_status_bar_counts_the_questions_while_a_person_s_sort_runs(client, settings, conn):
    client.get("/api/health")
    story(conn)
    assemble(conn)
    gate, inside = threading.Event(), threading.Event()
    fake = FakeProvider.chat

    def slow(self, messages):
        inside.set()
        assert gate.wait(10)
        return fake(self, messages)

    with patch.object(FakeProvider, "chat", slow):
        assert client.post("/api/needs-ai/sort").json()["queued"] == 2
        assert inside.wait(10)
        [working] = client.get("/api/overview").json()["ai"]["working"]
        assert working["kind"] == "sort" and working["asked"] and working["pages"] == 2
        assert (working["done"], working["of"]) == (0, 1)
        gate.set()
        assert client.app.state.ai_work.wait_idle()
    assert client.get("/api/overview").json()["ai"]["working"] == []
