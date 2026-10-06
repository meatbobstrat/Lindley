"""The connectors, against made-up servers: what each company's library sends for them, and how
they read the answers and errors.

No test here touches the network. The Anthropic and OpenAI libraries run on an
httpx2.MockTransport, Google's on an httpx.MockTransport.
"""

import base64
import json

import httpx
import httpx2
import pytest

from lindley.config import ProviderConfig
from lindley.providers.base import ChatMessage, ProviderError, Usage
from lindley.providers.connectors import anthropic, google, local, openai, openai_compat
from lindley.providers.prompts import TRANSCRIBE

JPEG = b"\xff\xd8\xff\xe0" + b"\0" * 20
PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 20
TALK = [
    ChatMessage("system", "Be brief."),
    ChatMessage("user", "Who wrote it?"),
    ChatMessage("assistant", "Which letter?"),
    ChatMessage("user", "The first."),
]
# Busy replies ask to be left a millisecond before trying again
BUSY_NOW = {"retry-after-ms": "1"}


class Server:
    """Answers each request with the next reply, and keeps the requests."""

    def __init__(self, *replies, lib=httpx2) -> None:
        self.replies = list(replies)
        self.requests: list = []
        self.lib = lib

    def __call__(self, request):
        self.requests.append(request)
        reply = self.replies.pop(0)
        return reply(request) if callable(reply) else reply

    def client(self):
        return self.lib.Client(transport=self.lib.MockTransport(self))

    @property
    def body(self) -> dict:
        return json.loads(self.requests[-1].content)


def sse(*events: dict | str, lib=httpx2, key="type"):
    lines = []
    for e in events:
        if isinstance(e, str):
            lines.append(f"data: {e}\n\n")
        else:
            lines.append(f"event: {e[key]}\ndata: {json.dumps(e)}\n\n")
    return lib.Response(200, text="".join(lines), headers={"content-type": "text/event-stream"})


def down(request):
    raise httpx2.ConnectError("refused")


# Local and other OpenAI-compatible services: Chat Completions, through OpenAI's library


def completion(text: str, finish="stop"):
    return httpx2.Response(
        200,
        json={
            "id": "c",
            "object": "chat.completion",
            "created": 0,
            "model": "m",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": finish,
                    "message": {"role": "assistant", "content": text},
                }
            ],
        },
    )


def make(module, server, key="k", **cfg):
    config = ProviderConfig(type=module.INFO.id, **cfg)
    return module.Provider(
        config=config, model=cfg.get("model"), api_key=key, http_client=server.client()
    )


def test_local_chat():
    server = Server(completion("Will Branson."))
    assert make(local, server).chat(TALK) == "Will Branson."
    req = server.requests[0]
    assert str(req.url) == "http://localhost:11434/v1/chat/completions"
    assert req.headers["authorization"] == "Bearer k"
    assert server.body["model"] == local.INFO.default_models["chat"]
    assert server.body["messages"][0] == {"role": "system", "content": "Be brief."}
    assert server.body["messages"][2] == {"role": "assistant", "content": "Which letter?"}


def test_a_local_ai_never_gets_the_openai_key_from_the_environment(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-real-openai-key")
    server = Server(completion("ok"))
    make(local, server, key=None).chat(TALK)
    assert server.requests[0].headers["authorization"] == "Bearer none"


def test_another_service_needs_an_address(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    p = make(openai_compat, Server())
    with pytest.raises(ProviderError, match="No address"):
        p.chat(TALK)
    server = Server(completion("ok"))
    make(openai_compat, server, base_url="https://ai.example.com/v1/").chat(TALK)
    assert str(server.requests[0].url) == "https://ai.example.com/v1/chat/completions"


def test_local_reads_a_page():
    server = Server(completion("  Dear Sister,\nWe are well.  "))
    t = make(local, server, model="llama3.2-vision").transcribe(PNG, hints="A letter, 1892")
    assert t.text == "Dear Sister,\nWe are well." and t.confidence is None
    text, image = server.body["messages"][0]["content"]
    assert text["text"].startswith(TRANSCRIBE) and "A letter, 1892" in text["text"]
    assert image["image_url"]["url"] == "data:image/png;base64," + base64.b64encode(PNG).decode()


def test_an_answer_cut_off_is_a_failed_call_and_an_empty_one_is_not():
    # A thinking model that used its whole context sent back nothing, cut off
    with pytest.raises(ProviderError, match="stopped part way through its answer"):
        make(local, Server(completion("", finish="length"))).transcribe(PNG)
    with pytest.raises(ProviderError, match="declined"):
        make(local, Server(completion("", finish="content_filter"))).chat(TALK)
    assert make(local, Server(completion(""))).transcribe(PNG).text == ""  # a blank page
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 0, "model": "m"}
    cut = sse(
        {**chunk, "choices": [{"index": 0, "delta": {"content": "Dear"}}]},
        {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "length"}]},
        "[DONE]",
        key="object",
    )
    with pytest.raises(ProviderError, match="ran out of room"):
        list(make(local, Server(cut)).chat_stream(TALK))


def test_a_local_ai_reads_a_page_and_sorts_without_thinking():
    server = Server(completion("Dear Sister,"))
    make(local, server).transcribe(PNG)
    assert server.body["reasoning_effort"] == "none"
    server = Server(completion("Will."))
    make(local, server).chat(TALK)
    assert server.body["reasoning_effort"] == "none"
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 0, "model": "m"}
    done = {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
    server = Server(sse(done, "[DONE]", key="object"))
    list(make(local, server).chat_stream(TALK))
    assert "reasoning_effort" not in server.body  # Ask Lindley may think
    server = Server(completion("Dear Sister,"))
    make(openai_compat, server, base_url="https://ai.example.com/v1").transcribe(PNG)
    assert "reasoning_effort" not in server.body


def test_a_server_that_doesnt_take_the_option_is_asked_without_it_once():
    unknown = httpx2.Response(400, json={"error": {"message": "unknown field reasoning_effort"}})
    server = Server(unknown, completion("Dear Sister,"), completion("We are well."))
    p = make(local, server)
    assert p.transcribe(PNG).text == "Dear Sister,"
    assert p.transcribe(PNG).text == "We are well."
    sent = [json.loads(r.content) for r in server.requests]
    assert [("reasoning_effort" in b) for b in sent] == [True, False, False]
    other = httpx2.Response(400, json={"error": {"message": "bad image"}})
    with pytest.raises(ProviderError, match="answered 400: bad image"):
        server = Server(other)
        make(openai_compat, server, base_url="https://ai.example.com/v1").chat(TALK)
    assert len(server.requests) == 1  # nothing to leave out: not sent again


def test_how_long_each_ai_is_waited_for():
    assert local.INFO.timeout_s == 600 and anthropic.INFO.timeout_s == 120
    assert make(local, Server()).client.timeout == 600
    assert make(local, Server(), timeout_s=30).client.timeout == 30
    assert make(openai, Server()).client.timeout == 120


def test_local_streams():
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 0, "model": "m"}
    server = Server(
        sse(
            {**chunk, "choices": [{"index": 0, "delta": {"role": "assistant"}}]},
            {**chunk, "choices": [{"index": 0, "delta": {"content": "Will "}}]},
            {**chunk, "choices": [{"index": 0, "delta": {"content": "Branson."}}]},
            "[DONE]",
            key="object",
        )
    )
    assert list(make(local, server).chat_stream(TALK)) == ["Will ", "Branson."]
    assert server.body["stream"] is True


def test_local_says_what_each_call_used():
    """Ollama reports usage as OpenAI does: prompt tokens include those read from a cache. A
    streamed answer's comes in a last chunk, after it's cut off too (it's charged)."""
    reply = json.loads(completion("ok").content) | {
        "usage": {
            "prompt_tokens": 11,
            "completion_tokens": 4,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 3},
        }
    }
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 0, "model": "m"}
    cut = sse(
        {**chunk, "choices": [{"index": 0, "delta": {"content": "Will"}}]},
        {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "length"}]},
        {**chunk, "choices": [], "usage": {"prompt_tokens": 9, "completion_tokens": 2}},
        "[DONE]",
        key="object",
    )
    server = Server(httpx2.Response(200, json=reply), cut)
    p, used = make(local, server), []
    p.on_usage = used.append
    p.chat(TALK)
    with pytest.raises(ProviderError, match="ran out of room"):
        list(p.chat_stream(TALK))
    assert server.body["stream_options"] == {"include_usage": True}
    model = local.INFO.default_models["chat"]
    assert used == [Usage(model, 8, 4, 3), Usage(model, 9, 2)]


def test_a_server_that_wont_say_what_a_stream_used_still_streams():
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 0, "model": "m"}
    refused = httpx2.Response(400, json={"error": {"message": "unknown field stream_options"}})
    server = Server(
        refused,
        sse(
            {**chunk, "choices": [{"index": 0, "delta": {"content": "ok"}}]}, "[DONE]", key="object"
        ),
    )
    assert list(make(local, server).chat_stream(TALK)) == ["ok"]
    assert "stream_options" not in server.body


def test_embeds_in_order():
    server = Server(
        httpx2.Response(
            200,
            json={
                "object": "list",
                "model": "m",
                "data": [
                    {"object": "embedding", "index": 1, "embedding": [0.2]},
                    {"object": "embedding", "index": 0, "embedding": [0.1]},
                ],
                "usage": {"prompt_tokens": 1, "total_tokens": 1},
            },
        )
    )
    p = make(openai, server, model="text-embedding-3-small")
    assert p.embed(["a", "b"]) == [[0.1], [0.2]]
    assert str(server.requests[0].url) == "https://api.openai.com/v1/embeddings"
    assert server.body["model"] == "text-embedding-3-small" and server.body["input"] == ["a", "b"]


def test_local_check_finds_the_model():
    def models():
        return httpx2.Response(
            200,
            json={
                "object": "list",
                "data": [
                    {
                        "id": "gemma4:e4b",
                        "object": "model",
                        "created": 0,
                        "owned_by": "o",
                    },
                    {
                        "id": "nomic-embed-text:latest",
                        "object": "model",
                        "created": 0,
                        "owned_by": "o",
                    },
                ],
            },
        )

    assert "has gemma4:e4b" in make(local, Server(models())).check()
    with pytest.raises(ProviderError, match="no model qwen2.5vl. It has: gemma4:e4b"):
        make(local, Server(models()), model="qwen2.5vl").check()


def test_a_server_that_isnt_there():
    server = Server(down, down, down)
    with pytest.raises(ProviderError, match="Couldn't reach The AI at localhost"):
        make(local, server).chat(TALK)
    assert len(server.requests) == 1  # the library doesn't try again


def test_a_call_that_took_too_long_or_failed_is_sent_once():
    def slow(request):
        raise httpx2.ReadTimeout("timed out")

    server = Server(slow, slow, slow)
    with pytest.raises(ProviderError, match="took too long") as e:
        make(local, server).transcribe(PNG)
    assert len(server.requests) == 1 and not e.value.busy
    broken = httpx2.Response(500, json={"error": {"message": "out of memory"}})
    server = Server(broken, broken, broken)
    with pytest.raises(ProviderError, match="answered 500: out of memory"):
        make(local, server).transcribe(PNG)
    assert len(server.requests) == 1


# OpenAI: the Responses API


def response(*content: dict):
    return httpx2.Response(
        200,
        json={
            "id": "r",
            "object": "response",
            "created_at": 0,
            "status": "completed",
            "model": "m",
            "output": [
                {
                    "type": "message",
                    "id": "m",
                    "status": "completed",
                    "role": "assistant",
                    "content": list(content),
                }
            ],
            "parallel_tool_calls": False,
            "tool_choice": "auto",
            "tools": [],
        },
    )


def said(text: str) -> dict:
    return {"type": "output_text", "text": text, "annotations": []}


def test_openai_chat_through_responses_and_not_stored():
    server = Server(response(said("Will Branson.")))
    assert make(openai, server).chat(TALK) == "Will Branson."
    req = server.requests[0]
    assert str(req.url) == "https://api.openai.com/v1/responses"
    assert req.headers["authorization"] == "Bearer k"
    assert server.body == {
        "model": openai.INFO.default_models["chat"],
        "instructions": "Be brief.",
        "input": [
            {"role": "user", "content": "Who wrote it?"},
            {"role": "assistant", "content": "Which letter?"},
            {"role": "user", "content": "The first."},
        ],
        "store": False,
    }


def test_openai_says_what_each_call_used():
    usage = {
        "input_tokens": 12,
        "input_tokens_details": {"cached_tokens": 2, "cache_write_tokens": 3},
        "output_tokens": 5,
        "output_tokens_details": {"reasoning_tokens": 1},
        "total_tokens": 17,
    }
    reply = json.loads(response(said("ok")).content) | {"usage": usage}
    p, used = make(openai, Server(httpx2.Response(200, json=reply))), []
    p.on_usage = used.append
    p.chat(TALK)
    assert used == [Usage(openai.INFO.default_models["chat"], 7, 5, 2, 3)]


def test_openai_reads_a_page():
    server = Server(response(said(" Dear Sister, ")))
    assert make(openai, server).transcribe(JPEG).text == "Dear Sister,"
    text, image = server.body["input"][0]["content"]
    assert text == {"type": "input_text", "text": TRANSCRIBE}
    data = base64.b64encode(JPEG).decode()
    assert image == {
        "type": "input_image",
        "image_url": f"data:image/jpeg;base64,{data}",
        "detail": "high",
    }
    assert "instructions" not in server.body


def test_openai_refusal():
    refused = {"type": "refusal", "refusal": "I can't help with that."}
    with pytest.raises(ProviderError, match="OpenAI declined"):
        make(openai, Server(response(refused))).chat(TALK)


def test_openai_streams():
    def delta(n, text):
        return {
            "type": "response.output_text.delta",
            "sequence_number": n,
            "item_id": "m",
            "output_index": 0,
            "content_index": 0,
            "delta": text,
            "logprobs": [],
        }

    server = Server(sse(delta(1, "Will "), delta(2, "Branson.")))
    assert list(make(openai, server).chat_stream(TALK)) == ["Will ", "Branson."]
    assert server.body["stream"] is True and server.body["store"] is False
    failed = sse({"type": "error", "sequence_number": 1, "message": "boom", "code": "x"})
    with pytest.raises(ProviderError, match="OpenAI: boom"):
        list(make(openai, Server(failed)).chat_stream(TALK))


def test_openai_incomplete_is_a_failed_call():
    def incomplete(reason):
        r = response(said("Dear Si"))
        body = json.loads(r.content) | {
            "status": "incomplete",
            "incomplete_details": {"reason": reason},
        }
        return httpx2.Response(200, json=body)

    with pytest.raises(ProviderError, match="OpenAI stopped part way"):
        make(openai, Server(incomplete("max_output_tokens"))).transcribe(JPEG)
    with pytest.raises(ProviderError, match="OpenAI declined"):
        make(openai, Server(incomplete("content_filter"))).chat(TALK)


def test_openai_check():
    model = {"id": "gpt-6.1-sol", "object": "model", "created": 0, "owned_by": "openai"}
    server = Server(httpx2.Response(200, json=model))
    assert make(openai, server).check() == "Connected. OpenAI has gpt-6.1-sol."
    assert str(server.requests[0].url) == "https://api.openai.com/v1/models/gpt-6.1-sol"


def test_openai_errors_in_words_for_a_person():
    refused = httpx2.Response(401, json={"error": {"message": "Incorrect API key"}})
    with pytest.raises(ProviderError, match="OpenAI refused the key .*Incorrect API key"):
        make(openai, Server(refused)).chat(TALK)
    missing = httpx2.Response(404, json={"error": {"message": "No such model"}})
    with pytest.raises(ProviderError, match="no such model .*No such model"):
        make(openai, Server(missing)).chat(TALK)


def test_busy_says_so_and_how_long_to_wait():
    busy = httpx2.Response(429, json={"error": {"message": "slow down"}}, headers=BUSY_NOW)
    server = Server(busy, response(said("ok")))
    with pytest.raises(ProviderError, match="OpenAI is busy \\(429\\), even after trying") as e:
        make(openai, server).chat(TALK)
    assert e.value.busy and e.value.retry_after == 0.001
    assert len(server.requests) == 1  # trying again is the throttle's (test_throttle)


# Anthropic: the Messages API, through Anthropic's library


def claude(server, model="claude-opus-5-5", key="k", effort=None):
    return anthropic.Provider(model=model, api_key=key, http_client=server.client(), effort=effort)


def message(*blocks, stop="end_turn"):
    return httpx2.Response(
        200,
        json={
            "id": "m",
            "type": "message",
            "role": "assistant",
            "model": "m",
            "content": list(blocks),
            "stop_reason": stop,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        },
    )


def text(t: str) -> dict:
    return {"type": "text", "text": t}


def test_anthropic_chat():
    server = Server(message({"type": "thinking", "thinking": "", "signature": "s"}, text("Will.")))
    assert claude(server, model="claude-haiku-4-5").chat(TALK) == "Will."
    req = server.requests[0]
    assert str(req.url) == "https://api.anthropic.com/v1/messages"
    assert req.headers["x-api-key"] == "k" and req.headers["anthropic-version"] == "2023-06-01"
    assert server.body == {
        "model": "claude-haiku-4-5",
        "max_tokens": anthropic.MAX_TOKENS,
        "system": "Be brief.",
        "messages": [
            {"role": "user", "content": "Who wrote it?"},
            {"role": "assistant", "content": "Which letter?"},
            {"role": "user", "content": "The first."},
        ],
    }
    assert "anthropic-beta" not in req.headers


def test_anthropic_says_what_each_call_used():
    used = []
    p = claude(Server(message(text("ok")), anthropic_stream("Wi", "ll"), message(stop="refusal")))
    p.on_usage = used.append
    p.chat(TALK)
    list(p.chat_stream(TALK))
    with pytest.raises(ProviderError, match="declined"):
        p.chat(TALK)  # charged all the same
    assert used == [
        Usage("claude-opus-5-5", input_tokens=1, output_tokens=1),
        Usage("claude-opus-5-5", input_tokens=1, output_tokens=2),
        Usage("claude-opus-5-5", input_tokens=1, output_tokens=1),
    ]


def test_anthropic_effort_when_asked():
    server = Server(message(text("ok")))
    claude(server, effort="low").chat(TALK)
    assert server.body["output_config"] == {"effort": "low"}
    assert anthropic.INFO.default_models["vision"] == "claude-opus-5-5"


def test_anthropic_falls_back_on_a_refusal_where_the_model_can():
    server = Server(message(text("ok")))
    claude(server).chat(TALK)
    assert server.body["fallbacks"] == "default"
    assert server.requests[0].headers["anthropic-beta"] == anthropic.FALLBACK_BETA


def test_anthropic_refusal_and_no_key(monkeypatch):
    with pytest.raises(ProviderError, match="declined"):
        claude(Server(message(stop="refusal"))).chat(TALK)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-from-the-environment")
    with pytest.raises(ProviderError, match="No API key"):
        claude(Server(), key=None).chat(TALK)
    with pytest.raises(ProviderError, match="No API key"):
        claude(Server(), key=None).check()


def test_anthropic_reads_a_page():
    server = Server(message(text("Dear Sister,")))
    assert claude(server).transcribe(JPEG).text == "Dear Sister,"
    image, prompt = server.body["messages"][0]["content"]
    assert image["source"] == {
        "type": "base64",
        "media_type": "image/jpeg",
        "data": base64.b64encode(JPEG).decode(),
    }
    assert prompt["text"] == TRANSCRIBE
    assert "system" not in server.body


def anthropic_stream(*deltas, stop="end_turn"):
    events = [
        {
            "type": "message_start",
            "message": {
                "id": "m",
                "type": "message",
                "role": "assistant",
                "model": "m",
                "content": [],
                "stop_reason": None,
                "usage": {"input_tokens": 1, "output_tokens": 0},
            },
        },
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
        *(
            {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": d}}
            for d in deltas
        ),
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": stop}, "usage": {"output_tokens": 2}},
        {"type": "message_stop"},
    ]
    return sse(*events)


def test_anthropic_streams():
    server = Server(anthropic_stream("Wi", "ll"))
    assert list(claude(server).chat_stream(TALK)) == ["Wi", "ll"]
    assert server.body["stream"] is True and server.body["fallbacks"] == "default"
    with pytest.raises(ProviderError, match="declined"):
        list(claude(Server(anthropic_stream("I", stop="refusal"))).chat_stream(TALK))


def test_anthropic_busy_in_a_stream_and_after_trying_again():
    overloaded = sse(
        {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}
    )
    with pytest.raises(ProviderError, match="Anthropic is busy \\(529\\)"):
        list(claude(Server(overloaded)).chat_stream(TALK))
    busy = httpx2.Response(
        529,
        json={"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}},
        headers=BUSY_NOW,
    )
    server = Server(busy, busy, busy)
    with pytest.raises(ProviderError, match="Anthropic is busy \\(529\\), even .*Overloaded") as e:
        claude(server).chat(TALK)
    assert len(server.requests) == 1 and e.value.busy and e.value.retry_after == 0.001


def test_anthropic_out_of_room_is_a_failed_call():
    with pytest.raises(ProviderError, match="Anthropic stopped part way"):
        claude(Server(message(text("Dear Si"), stop="max_tokens"))).transcribe(JPEG)
    with pytest.raises(ProviderError, match="ran out of room"):
        list(claude(Server(anthropic_stream("Dear", stop="max_tokens"))).chat_stream(TALK))


def test_anthropic_check():
    model = {
        "id": "claude-opus-5-5",
        "type": "model",
        "display_name": "Claude Opus 5.5",
        "created_at": "2026-01-01T00:00:00Z",
    }
    server = Server(httpx2.Response(200, json=model))
    assert claude(server).check() == "Connected. Anthropic has Claude Opus 5.5."
    assert str(server.requests[0].url) == "https://api.anthropic.com/v1/models/claude-opus-5-5"


def test_pages_must_be_an_image_every_ai_takes():
    with pytest.raises(ProviderError, match="JPEG, PNG or WebP"):
        claude(Server()).transcribe(b"GIF89a")


# Google: the Interactions API, through google-genai


def gemini(server, model=None, key="k"):
    return google.Provider(model=model, api_key=key, http_client=server.client())


def interaction(t: str):
    return httpx.Response(
        200,
        json={
            "id": "",
            "status": "completed",
            "steps": [
                {"type": "thought", "signature": "s"},
                {"type": "model_output", "content": [{"type": "text", "text": t}]},
            ],
        },
    )


def test_google_chat_through_interactions_and_not_stored():
    server = Server(interaction("Will Branson."), lib=httpx)
    assert gemini(server).chat(TALK) == "Will Branson."
    req = server.requests[0]
    assert str(req.url) == "https://generativelanguage.googleapis.com/v1beta/interactions"
    assert req.headers["x-goog-api-key"] == "k"
    assert server.body == {
        "model": google.INFO.default_models["chat"],
        "system_instruction": "Be brief.",
        "input": [
            {"type": "user_input", "content": [{"type": "text", "text": "Who wrote it?"}]},
            {"type": "model_output", "content": [{"type": "text", "text": "Which letter?"}]},
            {"type": "user_input", "content": [{"type": "text", "text": "The first."}]},
        ],
        "store": False,
    }


def test_google_reads_a_page():
    server = Server(interaction(" Dear Sister, "), lib=httpx)
    assert gemini(server).transcribe(PNG).text == "Dear Sister,"
    prompt, image = server.body["input"][0]["content"]
    assert prompt == {"type": "text", "text": TRANSCRIBE}
    assert image == {
        "type": "image",
        "mime_type": "image/png",
        "data": base64.b64encode(PNG).decode(),
    }
    assert server.body["store"] is False and "system_instruction" not in server.body


def test_google_streams():
    events = [
        {"event_type": "interaction.created", "interaction": {"id": "", "status": "in_progress"}},
        {"event_type": "step.start", "index": 0, "step": {"type": "model_output"}},
        {"event_type": "step.delta", "index": 0, "delta": {"type": "text", "text": "Wi"}},
        {"event_type": "step.delta", "index": 0, "delta": {"type": "text", "text": "ll"}},
        {"event_type": "interaction.completed", "interaction": {"id": "", "status": "completed"}},
    ]
    server = Server(sse(*events, lib=httpx, key="event_type"), lib=httpx)
    assert list(gemini(server).chat_stream(TALK)) == ["Wi", "ll"]
    assert server.body["stream"] is True and server.body["store"] is False
    failed = sse({"event_type": "error", "error": {"message": "boom"}}, lib=httpx, key="event_type")
    with pytest.raises(ProviderError, match="Google: boom"):
        list(gemini(Server(failed, lib=httpx)).chat_stream(TALK))


def test_google_incomplete_is_a_failed_call():
    r = interaction("Dear Si")
    body = json.loads(r.content) | {"status": "incomplete"}
    with pytest.raises(ProviderError, match="Google stopped part way"):
        gemini(Server(httpx.Response(200, json=body), lib=httpx)).transcribe(PNG)
    events = [
        {"event_type": "step.delta", "index": 0, "delta": {"type": "text", "text": "Wi"}},
        {"event_type": "interaction.completed", "interaction": {"id": "", "status": "incomplete"}},
    ]
    server = Server(sse(*events, lib=httpx, key="event_type"), lib=httpx)
    with pytest.raises(ProviderError, match="ran out of room"):
        list(gemini(server).chat_stream(TALK))


def test_google_says_what_each_call_used():
    """Gemini counts cached tokens among those sent, and thinking apart from the answer. One
    cut off is charged too."""
    usage = {
        "total_input_tokens": 10,
        "total_cached_tokens": 4,
        "total_output_tokens": 3,
        "total_thought_tokens": 2,
    }
    body = json.loads(interaction("ok").content) | {"usage": usage}
    cut = body | {"status": "incomplete"}
    server = Server(httpx.Response(200, json=body), httpx.Response(200, json=cut), lib=httpx)
    p, used = gemini(server), []
    p.on_usage = used.append
    p.chat(TALK)
    with pytest.raises(ProviderError, match="ran out of room"):
        p.transcribe(PNG)
    model = google.INFO.default_models["chat"]
    assert used == [Usage(model, 6, 5, 4), Usage(model, 6, 5, 4)]


def test_google_embeds():
    reply = httpx.Response(200, json={"embeddings": [{"values": [0.1]}, {"values": [0.2]}]})
    server = Server(reply, lib=httpx)
    assert gemini(server, model="gemini-embedding-2").embed(["a", "b"]) == [[0.1], [0.2]]
    assert str(server.requests[0].url).endswith("/models/gemini-embedding-2:batchEmbedContents")


def test_google_check():
    model = {"name": "models/gemini-3.8-flash", "displayName": "Gemini 3.8 Flash"}
    server = Server(httpx.Response(200, json=model), lib=httpx)
    assert gemini(server).check() == "Connected. Google has Gemini 3.8 Flash."
    assert str(server.requests[0].url).endswith("/v1beta/models/gemini-3.8-flash")


def test_google_errors_in_words_for_a_person(monkeypatch):
    def error(status, message):
        body = {"error": {"code": status, "message": message, "status": "X"}}
        return httpx.Response(status, json=body, headers=BUSY_NOW)

    with pytest.raises(ProviderError, match="Google refused the key .*API key not valid"):
        gemini(Server(error(401, "API key not valid"), lib=httpx)).chat(TALK)
    with pytest.raises(ProviderError, match="Google refused the key .*API key not valid"):
        gemini(Server(error(403, "API key not valid"), lib=httpx)).check()
    server = Server(*[error(429, "Quota exceeded")] * 4, lib=httpx)
    with pytest.raises(ProviderError, match="Google is busy \\(429\\)") as e:
        gemini(server).chat(TALK)
    assert len(server.requests) == 1 and e.value.busy and e.value.retry_after == 0.001
    server = Server(*[error(500, "Internal")] * 4, lib=httpx)
    with pytest.raises(ProviderError, match="Google answered 500"):
        gemini(server).chat(TALK)
    assert len(server.requests) == 1

    def gone(request):
        raise httpx.ConnectError("refused")

    server = Server(*[gone] * 4, lib=httpx)
    with pytest.raises(ProviderError, match="Couldn't reach Google"):
        gemini(server).chat(TALK)
    assert len(server.requests) == 1
    monkeypatch.setenv("GEMINI_API_KEY", "from-the-environment")
    with pytest.raises(ProviderError, match="No API key"):
        gemini(Server(lib=httpx), key=None).chat(TALK)
