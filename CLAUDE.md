# Lindley: notes for Claude

## Layout
- `backend/`: Python 3.13 FastAPI app, `src/lindley/`, setuptools, pytest and ruff
  - `config.py`: Pydantic `Settings`, plus the settings.json lookup and load/save
  - `app.py`: `create_app(settings)` factory; routers live in `api/`
  - `db/`: plain `sqlite3` with FTS5; `schema.sql` is applied idempotently by `init_db`
  - `providers/`: the AI abstraction (`ChatProvider`, `VisionProvider`, `EmbeddingProvider`),
    built by `registry.build_provider`. Adapters: `anthropic`, `openai_compat`
    (OpenAI/Ollama/LM Studio/vLLM) and `fake` (for tests)
  - `worker/`: `intake.py` (hash, library copy, EXIF, split; `ingest` = import + read),
    `pipeline.py` (step records, Tesseract/vision reading), `ocr/` engines
  - `watcher/`: watchdog folder watcher, started by the app lifespan (`create_app(watch=False)` in tests)
  - `assembler/`: Inbox pages → documents; `search/`: stub for now
  - `scripts/intake.py` reads real scans end to end; tests stub Tesseract (not installed in CI)
- `frontend/`: Vite, React and TypeScript. The dev server proxies `/api` to `127.0.0.1:8765`
- `scripts/dev.ps1`: runs both dev servers

## Commands
- Backend: `cd backend; .venv\Scripts\Activate.ps1; pytest; ruff check .; python -m lindley`
- Frontend: `cd frontend; npm run dev | npm run lint | npm run build`

## Conventions
- The repo is **public**. Never commit `settings.json`, `.env`, databases or scanned data.
  API keys come only from the env var named by a provider's `api_key_env`.
- Tests use `FakeProvider` and temp dirs. They never touch real providers or the network.
- Make one commit per logical step, after tests and lint pass, and push to `origin main`.
