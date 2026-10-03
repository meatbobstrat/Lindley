"""The HTTP connectors, against made-up servers: what they send, and how they read answers.

No test here touches the network: every client runs on an httpx.MockTransport.
"""

import base64
import json

import httpx
import pytest

from lindley.config import ProviderConfig
from lindley.providers.base import ChatMessage, ProviderBusy, ProviderError
from lindley.providers.connectors import anthropic, google, local, openai, openai_compat
from lindley.providers.prompts import TRANSCRIBE

JPEG = b"\xff\xd8\xff\xe0" + b"\0" * 20
PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 20
TALK = [ChatMessage("system", "Be brief."), ChatMessage("user", "Who wrote it?")]


class Server:
    """Answers each request with the next reply, and keeps the requests."""

    def __init__(self, *replies: httpx.Response) -> None:
        self.replies = list(replies)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.replies.pop(0)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self))

    @property
    def body(self) -> dict:
        return json.loads(self.requests[-1].content)


def sse(*events: dict | str) -> httpx.Response:
    lines = [f"data: {e if isinstance(e, str) else json.dumps(e)}\n\n" for e in events]
    return httpx.Response(200, text="".join(lines), headers={"content-type": "text/event-stream"})


def completion(text: str) -> httpx.Response:
    return httpx.Response(200, json={"choices": [{"message": {"content": text}}]})


# OpenAI-compatible: local, OpenAI, Google, any other


def make(module, server, **cfg):
    config = ProviderConfig(type=module.INFO.id, **cfg)
    return module.Provider(
        config=config, model=cfg.get("model"), api_key="k", client=server.client()
    )


@pytest.mark.parametrize(
    ("module", "url"),
    [
        (openai, "https://api.openai.com/v1/chat/completions"),
        (google, "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"),
        (local, "http://localhost:11434/v1/chat/completions"),
    ],
)
def test_openai_wire_chat(module, url):
    server = Server(completion("Will Branson."))
    p = make(module, server)
    assert p.chat(TALK) == "Will Branson."
    req = server.requests[0]
    assert str(req.url) == url
    assert req.headers["authorization"] == "Bearer k"
    assert server.body["model"] == module.INFO.default_models["chat"]
    assert server.body["messages"][0] == {"role": "system", "content": "Be brief."}


def test_openai_wire_needs_an_address_for_another_service():
    p = make(openai_compat, Server())
    with pytest.raises(ProviderError, match="No address"):
        p.chat(TALK)


def test_openai_wire_reads_a_page():
    server = Server(completion("  Dear Sister,\nWe are well.  "))
    p = make(local, server, model="llama3.2-vision")
    t = p.transcribe(PNG, hints="A letter, 1892")
    assert t.text == "Dear Sister,\nWe are well." and t.confidence is None
    text, image = server.body["messages"][0]["content"]
    assert text["text"].startswith(TRANSCRIBE) and "A letter, 1892" in text["text"]
    assert image["image_url"]["url"] == "data:image/png;base64," + base64.b64encode(PNG).decode()


def test_openai_wire_streams():
    server = Server(
        sse(
            {"choices": [{"delta": {"role": "assistant"}}]},
            {"choices": [{"delta": {"content": "Will "}}]},
            {"choices": [{"delta": {"content": "Branson."}}]},
            "[DONE]",
        )
    )
    assert list(make(openai, server).chat_stream(TALK)) == ["Will ", "Branson."]
    assert server.body["stream"] is True


def test_openai_wire_embeds_in_order():
    server = Server(
        httpx.Response(
            200, json={"data": [{"index": 1, "embedding": [0.2]}, {"index": 0, "embedding": [0.1]}]}
        )
    )
    p = make(openai, server, model="text-embedding-3-small")
    assert p.embed(["a", "b"]) == [[0.1], [0.2]]
    assert str(server.requests[0].url).endswith("/embeddings")


def test_openai_wire_check_finds_the_model():
    models = {"data": [{"id": "llama3.2-vision:latest"}, {"id": "nomic-embed-text:latest"}]}
    p = make(local, Server(httpx.Response(200, json=models)))
    assert "has llama3.2-vision" in p.check()
    p = make(local, Server(httpx.Response(200, json=models)), model="qwen2.5vl")
    with pytest.raises(ProviderError, match="no model qwen2.5vl. It has: llama3.2-vision"):
        p.check()
    p = make(
        google, Server(httpx.Response(200, json={"data": [{"id": "models/gemini-2.5-flash"}]}))
    )
    assert "Google has gemini-2.5-flash" in p.check()


def test_errors_in_words_for_a_person():
    refused = httpx.Response(401, json={"error": {"message": "Incorrect API key"}})
    with pytest.raises(ProviderError, match="OpenAI refused the key .*Incorrect API key"):
        make(openai, Server(refused)).chat(TALK)
    busy = httpx.Response(
        429, json={"error": {"message": "slow down"}}, headers={"retry-after": "7"}
    )
    with pytest.raises(ProviderBusy) as e:
        make(google, Server(busy)).chat(TALK)
    assert e.value.retry_after == 7
    with pytest.raises(ProviderError, match="The AI at localhost answered 500"):
        make(local, Server(httpx.Response(500, text="boom"))).chat(TALK)


def test_a_server_that_isnt_there():
    def down(request):
        raise httpx.ConnectError("refused")

    p = local.Provider(
        config=ProviderConfig(type="local"),
        client=httpx.Client(transport=httpx.MockTransport(down)),
    )
    with pytest.raises(ProviderError, match="Couldn't reach The AI at localhost"):
        p.chat(TALK)


# Anthropic


def claude(server, model="claude-opus-5", key="k"):
    return anthropic.Provider(config=None, model=model, api_key=key, client=server.client())


def message(*blocks, stop="end_turn"):
    return httpx.Response(200, json={"content": list(blocks), "stop_reason": stop})


def test_anthropic_chat():
    server = Server(
        message({"type": "thinking", "thinking": ""}, {"type": "text", "text": "Will."})
    )
    assert claude(server).chat(TALK) == "Will."
    req = server.requests[0]
    assert str(req.url) == "https://api.anthropic.com/v1/messages"
    assert req.headers["x-api-key"] == "k" and req.headers["anthropic-version"] == "2023-06-01"
    body = server.body
    assert body["system"] == "Be brief."
    assert body["messages"] == [{"role": "user", "content": "Who wrote it?"}]
    assert body["max_tokens"] == anthropic.MAX_TOKENS


def test_anthropic_falls_back_on_a_refusal_where_the_model_can():
    server = Server(
        message({"type": "text", "text": "ok"}), message({"type": "text", "text": "ok"})
    )
    claude(server).chat(TALK)
    assert server.body["fallbacks"] == "default"
    assert server.requests[0].headers["anthropic-beta"] == anthropic.FALLBACK_BETA
    claude(server, model="claude-haiku-4-5").chat(TALK)
    assert "fallbacks" not in server.body and "anthropic-beta" not in server.requests[1].headers


def test_anthropic_refusal_and_no_key():
    with pytest.raises(ProviderError, match="declined"):
        claude(Server(message(stop="refusal"))).chat(TALK)
    with pytest.raises(ProviderError, match="No API key"):
        claude(Server(), key=None).chat(TALK)


def test_anthropic_reads_a_page():
    server = Server(message({"type": "text", "text": "Dear Sister,"}))
    assert claude(server).transcribe(JPEG).text == "Dear Sister,"
    image, text = server.body["messages"][0]["content"]
    assert image["source"] == {
        "type": "base64",
        "media_type": "image/jpeg",
        "data": base64.b64encode(JPEG).decode(),
    }
    assert text["text"] == TRANSCRIBE


def test_anthropic_streams():
    server = Server(
        sse(
            {"type": "message_start", "message": {}},
            {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta"}},
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": "Wi"},
            },
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": "ll"},
            },
            {"type": "message_delta", "delta": {"stop_reason": "end_turn"}},
            {"type": "message_stop"},
        )
    )
    assert list(claude(server).chat_stream(TALK)) == ["Wi", "ll"]
    overloaded = sse({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}})
    with pytest.raises(ProviderBusy):
        list(claude(Server(overloaded)).chat_stream(TALK))


def test_anthropic_busy_and_check():
    with pytest.raises(ProviderBusy):
        claude(Server(httpx.Response(529, json={"error": {"message": "Overloaded"}}))).chat(TALK)
    server = Server(
        httpx.Response(200, json={"id": "claude-opus-5", "display_name": "Claude Opus 5"})
    )
    assert claude(server).check() == "Connected. Anthropic has Claude Opus 5."
    assert str(server.requests[0].url).endswith("/v1/models/claude-opus-5")


def test_pages_must_be_an_image_every_ai_takes():
    with pytest.raises(ProviderError, match="JPEG, PNG or WebP"):
        claude(Server()).transcribe(b"GIF89a")
