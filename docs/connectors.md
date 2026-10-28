# Adding an AI connector

Each AI service Lindley can use is one file in `backend/src/lindley/providers/connectors/`, and
every file there is found when Lindley starts. To add one, drop in a module that defines:

- `INFO`, a `ConnectorInfo`: its id (the `type` in settings), its name, whether it runs on your
  own computers or a company's, the jobs it can do, and its usual model for each.
- `Provider`, built as `Provider(config=..., model=..., api_key=...)`. It implements the calls its
  jobs need (`chat` and `chat_stream`, `transcribe`, `embed`), plus `check()`, which Test
  connection uses.

Call the AI through its company's own Python library, the way the company's documentation
shows, rather than writing the HTTP requests by hand. The library keeps up with changes to the
AI's API (updating it is usually all a change needs). Turn off its own retries (`max_retries=0`
or the like): it would repeat a call that took too long. Lindley's throttle tries again only
when the AI says it's busy, so set `busy` and `retry_after` on the error (`_common.failure`).
Dependabot (`.github/dependabot.yml`) opens a pull request each week a library has a new
release, and CI tests it; a new major version comes in a pull request of its own.
`_common.py` turns its errors into messages a person can read. Each built-in connector follows
its company's advice:

| Connector | Library | API |
| --- | --- | --- |
| `anthropic` | `anthropic` | Messages, with refusal fallbacks on the models that have them |
| `openai` | `openai` | Responses (embeddings: Embeddings), `store=False` |
| `google` | `google-genai` | Interactions (embeddings: `embed_content`), `store=False` |
| `builtin` | `openai`, at Lindley's own `llama-server` | Chat Completions, with thinking turned off through the chat template |
| `local`, `openai_compat` | `openai`, at the server's address | Chat Completions, which Ollama, LM Studio and vLLM all support |

A service that speaks the OpenAI API needs only `INFO` and a subclass of `OpenAIChat` (see
`local.py`). Test it against a made-up server, as `tests/unit/test_connectors.py` does:
`httpx2.MockTransport` for the Anthropic and OpenAI libraries, `httpx.MockTransport` for
Google's. Settings and first-run setup list it with no other change.
