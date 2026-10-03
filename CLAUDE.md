# Lindley: notes for Claude

## Layout
- `backend/`: Python 3.13 FastAPI app, `src/lindley/`, setuptools, pytest and ruff
  - `config.py`: Pydantic `Settings`, plus the settings.json lookup and load/save
  - `app.py`: `create_app(settings)` factory; routers live in `api/`
  - `db/`: plain `sqlite3` with FTS5; `schema.sql` is applied idempotently by `init_db`
  - `providers/`: the AI abstraction (`ChatProvider`, `VisionProvider`, `EmbeddingProvider`).
    `registry.get_provider(ai, job)` builds the one settings give a job (`vision`, `assemble`,
    `chat`, `embed`), wrapped in its connection's throttle (`throttle.py`). Connectors are files
    in `providers/connectors/`, found at start-up: `local`, `anthropic`, `openai`, `google`,
    `openai_compat` and `fake` (for tests). Each calls its AI through the company's own library
    (`anthropic`, `openai`, `google-genai`), as its docs recommend: never hand-written HTTP.
    `_openai_chat.py` (Chat Completions, for OpenAI-compatible servers) and `_common.py` (errors
    in words, images) are shared helpers.
    `keys.py`: API keys in the system credential store (keyring)
  - `worker/`: `intake.py` (hash, library copy, EXIF, split; `ingest` = import + read),
    `pipeline.py` (step records, Tesseract/vision reading), `ocr/` engines
  - `watcher/`: watchdog folder watcher, started by the app lifespan (`create_app(watch=False)` in tests)
  - `assembler/`: Inbox pages → documents. `clues.py` (rules per page), `evidence.py` (features
    for a page pair, scored with `weights.py`), `layout.py`, `terms.py`, `segment.py`, `learn.py`
    (fitting the weights). Benches: `scripts/bench_assembler.py` (made-up, or `--real DB` with
    assembled PDFs as the answer key) and `scripts/fit_assembler.py`. `search/`: stub for now
  - `providers/allowance.py`: when an AI may be called on its own (`allow`, `daily_limit`);
    every call is recorded in `ai_calls`
  - `duplicates/`: `detect.py` (pages scanned twice, found by text), `resolve.py` (keep a copy or
    document, not duplicates). API in `api/duplicates.py`; page images in `api/pages.py`
  - `export/`: searchable PDFs. `textlayer.py` (word boxes; other readings aligned to Tesseract's),
    `pdf.py` (fpdf2, invisible text), `document.py` (Exports folder, status, `exports` row).
    API in `api/documents.py`; `scripts/export.py` exports from the command line
  - `scripts/intake.py` reads real scans end to end; tests stub Tesseract (not installed in CI)
- `frontend/`: Vite, React and TypeScript. The dev server proxies `/api` to `127.0.0.1:8765`
- `scripts/dev.ps1`: runs both dev servers

## Commands
- Backend: `cd backend; .venv\Scripts\Activate.ps1; pytest; ruff check .; python -m lindley`
- Frontend: `cd frontend; npm run dev | npm run lint | npm run build`

## Conventions
- The repo is **public**. Never commit `settings.json`, `.env`, databases or scanned data.
  API keys live in the credential store (`providers/keys.py`), or the env var named by a
  provider's `api_key_env`. Tests use an in-memory keyring (conftest).
- Tests use `FakeProvider` and temp dirs. They never touch real providers or the network:
  connector tests run on `httpx2.MockTransport` (Anthropic, OpenAI) or `httpx.MockTransport`
  (Google).
- Local AI and ML must run on an ordinary laptop: no GPU, no heavy ML packages.
- Make one commit per logical step, after tests and lint pass, and push to `origin main`.
- Do web searches when neccessary to confirm you are using up to date best practices when planning and coding.
