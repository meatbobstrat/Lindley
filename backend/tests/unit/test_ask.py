"""Ask Lindley: finding the pages for a question, what the AI is sent, when it can answer, and
answering, with the fake AI."""

import sqlite3
from unittest.mock import patch

import pytest

from lindley.ask import conversation, prompt
from lindley.ask.answer import answer
from lindley.ask.retrieve import Scope, gather, question_words, search_words
from lindley.ask.status import budget, chat_status
from lindley.config import AiSettings, JobConfig, ProviderConfig
from lindley.db.database import connect, init_db
from lindley.providers.base import ProviderError
from lindley.providers.connectors.fake import FakeProvider
from lindley.providers.registry import get_provider
from lindley.search.fts import search_any

from .test_api_library import add_page


@pytest.fixture
def conn(settings):
    init_db(settings.db_path)
    c = connect(settings.db_path)
    yield c
    c.close()


def document(conn, name):
    d = conn.execute("INSERT INTO documents (name) VALUES (?)", (name,)).lastrowid
    conn.commit()
    return d


# ---------------------------------------------------------------- Finding pages


def test_any_of_the_words_finds_a_page_best_match_first(conn, tmp_path):
    a = add_page(conn, tmp_path, "John Branson came home from Leeds.")
    b = add_page(conn, tmp_path, "The weather in Leeds.")
    add_page(conn, tmp_path, "Nothing to see.")
    assert search_any(conn, ["Branson", "Leeds"]) == [a, b]
    assert search_any(conn, ["Bran"]) == [a]  # the start of a word
    assert search_any(conn, ['"(', "OR", ")*"]) == []  # FTS5's own syntax is only text
    assert search_any(conn, []) == []


def test_only_the_reading_in_use_is_searched(conn, tmp_path):
    p = add_page(conn, tmp_path, "Dcar Brausou")
    conn.execute("UPDATE transcriptions SET is_current = 0 WHERE page_id = ?", (p,))
    conn.execute(
        "INSERT INTO transcriptions (page_id, source, text, is_current) VALUES (?, 'user', ?, 1)",
        (p, "Dear Branson"),
    )
    conn.commit()
    assert search_any(conn, ["Brausou"]) == [] and search_any(conn, ["Branson"]) == [p]


def test_the_question_s_own_words_leave_out_the_small_ones():
    assert question_words("Where did John Branson live in 1940?") == [
        "John",
        "Branson",
        "live",
        "1940",
    ]


def test_search_words_are_the_ai_s_then_the_question_s(settings):
    chat = get_provider(settings.ai, "chat")
    with patch.object(FakeProvider, "chat", return_value='Sure: ["Branson", "Leeds", 3]'):
        assert search_words(chat, "Where did John Branson live?") == [
            "Branson",
            "Leeds",
            "John",
            "live",
        ]
    # The fake AI answers with no list: the question's own words are searched for
    assert search_words(chat, "Who was Edith?") == ["Edith"]


def test_the_open_page_and_its_document_come_before_pages_found(conn, tmp_path):
    d = document(conn, "Letter from Will")
    pages = [add_page(conn, tmp_path, f"Page {i} of Will's letter.", d, i) for i in range(5)]
    other = add_page(conn, tmp_path, "Edith wrote about Will too.")
    found = gather(conn, ["Edith"], Scope(page_id=pages[2]), 40000, 80)
    ids = [s.page_id for s in found]
    assert ids[:3] == [pages[2], pages[1], pages[3]]  # the open page, and either side of it
    assert ids[3:] == [pages[0], pages[4], other]  # the rest of its document, then the rest
    assert [s.n for s in found] == [1, 2, 3, 4, 5, 6]
    assert found[0].label == "Letter from Will, page 3" and found[0].page_number == 3
    assert found[-1].label.startswith("Inbox: ") and found[-1].document_id is None


def test_pages_are_cut_to_the_budget_around_the_words(conn, tmp_path):
    long = "x " * 3000 + "Branson came to tea. " + "y " * 3000
    add_page(conn, tmp_path, long)
    [only] = gather(conn, ["Branson"], Scope(), 6000, 80)
    assert len(only.text) == 1500  # a quarter of the budget, at most
    assert "Branson came to tea" in only.text and only.text.startswith("…")
    for i in range(10):
        add_page(conn, tmp_path, f"Branson again, {i}. " + "z " * 1000)
    found = gather(conn, ["Branson"], Scope(), 6000, 80)
    assert 3 <= len(found) < 11 and sum(len(s.text) for s in found) <= 6000


def test_a_page_read_poorly_and_not_checked_is_unsure(conn, tmp_path):
    poor = add_page(conn, tmp_path, "Branson, poorly read", conf=50)
    good = add_page(conn, tmp_path, "Branson, well read", conf=95)
    checked = add_page(conn, tmp_path, "Branson, checked", conf=50)
    conn.execute(
        "UPDATE transcriptions SET confirmed_at = datetime('now') WHERE page_id = ?", (checked,)
    )
    conn.commit()
    unsure = {s.page_id: s.unsure for s in gather(conn, ["Branson"], Scope(), 40000, 80)}
    assert unsure == {poor: True, good: False, checked: False}


# ---------------------------------------------------------------- What the AI is sent


def test_the_pages_go_with_the_question_numbered_and_after_the_conversation(conn, tmp_path):
    add_page(conn, tmp_path, "Branson & <Sons>", conf=50)
    found = gather(conn, ["Branson"], Scope(), 40000, 80)
    msgs = prompt.messages("Who?", found, [("user", "Hi"), ("assistant", "Hello")], "Inbox")
    assert [m.role for m in msgs] == ["system", "user", "assistant", "user"]
    last = msgs[-1].content
    assert '<page n="1" title="Inbox: scan_0.jpg" unsure="yes">' in last
    assert "Branson & <Sons>" in last and last.endswith(
        "The person has open: Inbox\n\nQuestion: Who?"
    )
    assert "No pages were found" in prompt.messages("Who?", [], [])[-1].content


# ---------------------------------------------------------------- Conversations


def test_a_conversation_is_titled_by_its_first_question():
    assert conversation.title_of("  Who was   Edith? ") == "Who was Edith?"
    long = conversation.title_of("Where " + "and where " * 20)
    assert len(long) <= 61 and long.endswith("…")


def test_history_is_whole_questions_and_answers_without_failed_ones(conn):
    chat, _ = conversation.add_question(conn, None, "One?", None)
    with conn:
        conversation.add_answer(conn, chat, "First.", [], "done", "local", "fake")
    conversation.add_question(conn, chat, "Two?", None)
    with conn:
        conversation.add_answer(conn, chat, "It failed", [], "failed", "local", "fake")
    _, now = conversation.add_question(conn, chat, "Three?", None)
    assert conversation.history(conn, chat, now, 6) == [("user", "One?"), ("assistant", "First.")]
    assert conversation.history(conn, chat, now, 1) == []


# ---------------------------------------------------------------- When it can answer


def test_ready_when_the_chat_job_has_a_connection_that_can_answer(settings):
    st = chat_status(settings)
    assert st["state"] == "ready" and st["connection"]["name"] == "local"


def test_offered_when_a_connection_could_answer_but_none_is_chosen(settings):
    settings.ai.jobs["chat"] = JobConfig()
    settings.ai.providers["claude"] = ProviderConfig(type="anthropic")  # no key saved
    st = chat_status(settings)
    assert st["state"] == "offer" and [o["name"] for o in st["offers"]] == ["local"]


def test_none_when_nothing_can_answer(settings):
    settings.ai = AiSettings()
    assert chat_status(settings)["state"] == "none"


def test_broken_when_a_cloud_ai_s_key_isnt_saved(settings, keys):
    settings.ai = AiSettings(
        providers={"claude": ProviderConfig(type="anthropic")},
        jobs={"chat": JobConfig(connection="claude")},
    )
    st = chat_status(settings)
    assert st["state"] == "broken" and "No API key" in st["reason"]
    keys.set_password("Lindley", "claude", "sk-test")
    assert chat_status(settings)["state"] == "ready"
    assert budget(settings) == settings.ask.cloud_chars


def test_an_ai_on_this_computer_gets_less_text(settings):
    assert budget(settings) == settings.ask.local_chars


def test_lindleys_own_ai_gets_text_for_its_models_context(settings):
    """Its context is known: half of it goes to the pages. Not downloaded, it can't answer."""
    settings.ai = AiSettings(
        providers={"own": ProviderConfig(type="builtin")},
        jobs={"chat": JobConfig(connection="own")},
        local=settings.ai.local,
    )
    assert budget(settings) == 16384 // 2 * 3  # Gemma 4 E4B, out of the box
    settings.ai.jobs["chat"].model = "qwen3.5-4b"
    assert budget(settings) == 8192 // 2 * 3
    from lindley.localai import server

    server.use(settings.ai.local)
    st = chat_status(settings)
    assert st["state"] == "broken" and "Qwen3.5 4B isn't downloaded yet" in st["reason"]


# ---------------------------------------------------------------- Answering


def test_an_answer_is_saved_with_its_pages_and_calls(conn, settings, tmp_path):
    p = add_page(conn, tmp_path, "Edith lived in Leeds.")
    events = list(answer(settings, "Where did Edith live?"))
    kinds = [e for e, _ in events]
    assert kinds[:2] == ["chat", "sources"] and kinds[-1] == "done" and "text" in kinds
    chat_id = events[0][1]["chat_id"]
    [source] = events[1][1]["sources"]
    assert source["page_id"] == p and source["n"] == 1
    written = "".join(d["text"] for e, d in events if e == "text")
    assert "Edith" in written  # the fake AI says back what it was sent
    q, a = conversation.messages(conn, chat_id)
    assert (q["role"], q["text"]) == ("user", "Where did Edith live?")
    assert (a["role"], a["status"], a["text"], a["connection"]) == (
        "assistant",
        "done",
        written,
        "local",
    )
    assert a["sources"][0]["page_id"] == p
    calls = conn.execute("SELECT purpose, automatic, ok FROM ai_calls").fetchall()
    assert [tuple(c) for c in calls] == [("chat", 0, 1)] * 2  # the search words, the answer


def test_the_database_is_free_while_the_ai_answers(conn, settings, tmp_path):
    add_page(conn, tmp_path, "Edith lived in Leeds.")

    def stream(self, messages):
        other = sqlite3.connect(settings.db_path, timeout=0.1)
        other.execute("BEGIN IMMEDIATE")  # raises if anyone holds the database
        other.rollback()
        other.close()
        yield "Leeds."

    with patch.object(FakeProvider, "chat_stream", stream):
        assert list(answer(settings, "Where?"))[-1][0] == "done"


def test_a_follow_up_is_sent_with_the_conversation_and_the_pages_cited(conn, settings, tmp_path):
    p = add_page(conn, tmp_path, "Edith lived in Leeds.")
    sent = []

    def stream(self, messages):
        sent.append(messages)
        yield "In Leeds [1]."

    with patch.object(FakeProvider, "chat_stream", stream):
        chat_id = list(answer(settings, "Where did Edith live?"))[0][1]["chat_id"]
        events = list(answer(settings, "And her brother?", chat_id))
    roles = [m.role for m in sent[-1]]
    assert roles == ["system", "user", "assistant", "user"]
    assert sent[-1][2].content == "In Leeds [1]."
    assert events[1][1]["sources"][0]["page_id"] == p  # cited before, so sent again


def test_an_ai_that_fails_says_so_and_the_call_is_recorded(conn, settings):
    def stream(self, messages):
        raise ProviderError("The AI at localhost isn't answering. Is it running?")
        yield

    with patch.object(FakeProvider, "chat_stream", stream):
        events = list(answer(settings, "Who?"))
    kind, data = events[-1]
    assert kind == "error" and "isn't answering" in data["message"]
    a = conversation.messages(conn, events[0][1]["chat_id"])[-1]
    assert (a["status"], a["text"]) == ("failed", data["message"])
    calls = conn.execute("SELECT ok FROM ai_calls ORDER BY id").fetchall()
    assert [c[0] for c in calls] == [1, 0]


def test_an_ai_that_cant_be_reached_isnt_asked_twice(conn, settings):
    with patch.object(FakeProvider, "chat", side_effect=ProviderError("Not running")):
        events = list(answer(settings, "Who?"))
    assert [e for e, _ in events] == ["chat", "error"]
    assert [tuple(r) for r in conn.execute("SELECT ok FROM ai_calls")] == [(0,)]


def test_stopping_part_way_keeps_what_was_written(conn, settings):
    closed = []

    def stream(self, messages):
        try:
            yield "Edith "
            yield "lived "
            yield "in Leeds."
        finally:
            closed.append(True)

    with patch.object(FakeProvider, "chat_stream", stream):
        work = answer(settings, "Where?")
        for kind, _ in work:
            if kind == "text":
                break
        work.close()
    assert closed == [True]  # the AI's answer was closed too
    [a] = [m for m in conversation.messages(conn, 1) if m["role"] == "assistant"]
    assert (a["status"], a["text"]) == ("stopped", "Edith ")
