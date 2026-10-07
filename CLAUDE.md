# Lindley: notes for Claude

## Layout
- `backend/`: Python 3.13 FastAPI app, `src/lindley/`, setuptools, pytest and ruff
  - `config.py`: Pydantic `Settings`, plus the settings.json lookup and load/save
  - `app.py`: `create_app(settings)` factory; routers live in `api/`
  - `db/`: plain `sqlite3` with FTS5; `schema.sql` is applied idempotently by `init_db`
  - `providers/`: the AI abstraction (`ChatProvider`, `VisionProvider`, `EmbeddingProvider`).
    `registry.get_provider(ai, job)` builds the one settings give a job (`vision`, `assemble`,
    `chat`, `embed`), wrapped in its connection's throttle (`throttle.py`). Connectors are files
    in `providers/connectors/`, found at start-up: `builtin` (Lindley's own AI, `localai/`), `local`,
    `anthropic`, `openai`, `google`, `openai_compat` and `fake` (for tests). Each calls its AI through the company's own library
    (`anthropic`, `openai`, `google-genai`), as its docs recommend: never hand-written HTTP.
    `_openai_chat.py` (Chat Completions, for OpenAI-compatible servers) and `_common.py` (errors
    in words, images) are shared helpers. A connector tells `on_usage` what each call used,
    before it checks the answer (a cut-off one is charged): `throttle.metered` collects it.
    `keys.py`: API keys in the system credential store (keyring). `tiers.py`: two choices,
    what this computer runs (Low, Middle, High: each job's model on Lindley's own AI) and who
    does the rest (none, server, cloud); the UI sets `ai.jobs` from them (`withTier` in
    `frontend/src/lib/tiers.ts`) and records them in `ai.tier` and `ai.help`
  - `localai/`: Lindley's own AI, llama.cpp's `llama-server` (the Vulkan build). `catalog.py`
    (the pinned engine and models, with sizes and SHA-256), `download.py` (only when a person
    asks: resumable, checked), `server.py` (router mode on 127.0.0.1, `--offline`, started when a
    job first needs it; on Windows in a job that ends with Lindley). Files in `ai.local.models_dir`.
    `scripts/local_ai.py` downloads and serves from the command line. API in `api/local_ai.py`.
    `computer.py`: a look at this computer (processor, memory, graphics cards), from which
    `tiers.suggest` picks a tier and `tiers.hundred_pages` says how long 100 pages would take
    (`GET /api/local-ai/computer`, shown in Setup and Settings).
    Test Lindley with its own engine and models, not Ollama. How to bench them, where the kept
    answers and Claude's reference readings are, and what was learned: design/benches.md
  - `worker/`: `intake.py` (hash, library copy, EXIF, split; `ingest` = import + read),
    `pipeline.py` (step records, Tesseract/vision reading; `read_turned_again`: pages a person
    turned since Tesseract read them, read again by the watcher), `ocr/` engines, `ai_work.py` (AI
    work a person asked for, done in the background, one job at a time). `activity.py`: what
    the AI is doing now, and what came of it, for the status bar (in the overview)
  - `watcher/`: watchdog folder watcher, started by the app lifespan (`create_app(watch=False)` in tests)
  - `assembler/`: Inbox pages → documents. `clues.py` (rules per page), `evidence.py` (features
    for a page pair, scored with `weights.py`), `layout.py`, `terms.py`, `segment.py`, `learn.py`
    (fitting the weights), `continues.py` (the `continues` job: a local model's p_yes that page B
    carries on from A, asked about unsure pairs before grouping; `lm_continues` evidence). Benches: `scripts/bench_assembler.py` (made-up, or `--real DB` with
    assembled PDFs as the answer key; `--sweep`: groups by confidence, to check the bars) and
    `scripts/fit_assembler.py`. `search/fts.py`: FTS5 search (`search_pages`, every word;
    `search_any`, any word)
  - `ask/`: Ask Lindley. `status.py` (whether it can answer: ready, broken, offer, none; the one
    place that decides), `retrieve.py` (search words from the AI, then the pages), `prompt.py`,
    `conversation.py` (tables `chats`, `chat_messages`), `answer.py` (one answer, as events).
    API in `api/chat.py`: `POST /api/chat` streams server-sent events (`fastapi.sse`), the
    answer worked out on a thread of its own (`metered` notes usage per thread)
  - `providers/allowance.py`: when an AI may be called on its own (`allow`, `daily_limit`);
    every call is recorded in `ai_calls`, with its tokens (`throttle.metered`) and estimated
    cost (`providers/prices.py`, list prices by model id: update it when prices change)
  - `duplicates/`: `detect.py` (pages scanned twice, found by text), `resolve.py` (keep a copy or
    document, not duplicates). API in `api/duplicates.py`; page images in `api/pages.py`
  - `export/`: searchable PDFs. `textlayer.py` (word boxes; other readings aligned to Tesseract's),
    `pdf.py` (fpdf2, invisible text), `document.py` (Exports folder, status, `exports` row).
    API in `api/documents.py`; `scripts/export.py` exports from the command line
  - `browse.py`: what the UI shows (pages with a `state`, documents, folders, review queue,
    counts); `organise.py`: a person's changes (move, reorder, rotate, flip, new document, rename,
    folders, checking text), each one undoable batch in `history`, made in
    `history.deciding(conn)` (BEGIN IMMEDIATE; a change of nothing returns no batch). API in
    `api/library.py`,
    `api/documents.py`, `api/pages.py`, `api/folders.py`; search in `search/fts.py`;
    Add scans… uploads in `api/scans.py` (read by the watcher, `read_later`)
  - `scripts/intake.py` reads real scans end to end; tests stub Tesseract (not installed in CI)
- `frontend/`: Vite, React and TypeScript, built from `design/mockup` (its CSS is `index.css`).
  The dev server proxies `/api` to `127.0.0.1:8765`. `api/client.ts` (typed API), `api/store.ts`
  (`useApi`, `invalidate` after a change), `lib/` (contexts, actions and dialogs, wording),
  `ui/` (tooltips, toolbar, menus, dialogs), `components/`, `views/` (one per screen).
  Every control gets a tooltip: `data-tip="…"` (ui/Tooltip.tsx shows it on hover and focus)
- `scripts/dev.ps1`: runs both dev servers

## Commands
- Backend: `cd backend; .venv\Scripts\Activate.ps1; pytest; ruff check .; python -m lindley [--reload]`
- Frontend: `cd frontend; npm run dev | npm run lint | npm run build`

## Conventions
- The repo is **public**. Never commit `settings.json`, `.env`, databases or scanned data.
  API keys live in the credential store (`providers/keys.py`), or the env var named by a
  provider's `api_key_env`. Tests use an in-memory keyring (conftest).
- Tests use `FakeProvider` and temp dirs. They never touch real providers or the network:
  connector tests run on `httpx2.MockTransport` (Anthropic, OpenAI) or `httpx.MockTransport`
  (Google).
- Never call an AI while a write transaction is open: a call can take minutes and holds
  SQLite's lock. The assembler's `Answers` defers questions met then (`AskFirst`).
- The API answers only to its own names and pages (`app.py`: TrustedHost, `FromLindleyOnly`):
  tests use `TestClient(app, base_url="http://127.0.0.1")`.
- Local AI and ML must run on an ordinary laptop: no GPU, no heavy ML packages.
- Make one commit per logical step, after tests and lint pass, and push to `origin main`.
- Do web searches when neccessary to confirm you are using up to date best practices when planning and coding.
