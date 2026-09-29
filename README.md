# Lindley

[![CI](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml)

Lindley is a research partner for working through thousands of scanned pages: letters,
deeds, books, manuscripts and more. It turns raw images into sorted, searchable PDFs,
and gives you an AI you can talk to about your documents.

## Why Lindley?

- Organize massive archives without manual sorting
- Search across handwritten or printed text instantly
- Ask questions like *"show me every letter mentioning John Branson in 1892"*
- Connect scattered documents into a story

## How it works

```
watch folders ──► watcher ──► processing queue ──► worker ──► library (searchable PDFs)
                                                    │  ├─ Tesseract/OCRmyPDF (printed text)
                                                    │  └─ vision model (handwriting / low confidence)
                                                    └─► SQLite + full-text index ──► search & AI chat
```

- **Backend:** Python 3.13 and FastAPI (`backend/`)
- **Frontend:** React, TypeScript and Vite (`frontend/`)
- **AI providers:** a pluggable layer that works with local models (Ollama, LM Studio)
  or hosted APIs (Anthropic, OpenAI and others), for both chat and handwriting OCR.

## Setup (Windows)

Prerequisites: Python 3.13 and Node 22+. The OCR worker also needs
[Tesseract](https://github.com/UB-Mannheim/tesseract/wiki) and Ghostscript. The
scaffolding runs without them.

```powershell
# backend
cd backend
py -3.13 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"

# frontend
cd ..\frontend
npm install
```

To run both dev servers:

```powershell
.\scripts\dev.ps1
```

This starts the backend on http://127.0.0.1:8765 and the UI on http://localhost:5173.

## Config

Copy `settings.example.json` to `settings.json` and edit it. The app looks for the
settings file in these places, in order:

1. the `--settings <path>` command-line argument
2. the `LINDLEY_SETTINGS` environment variable
3. `settings.json` in the current directory
4. the per-user config folder

| Key | Meaning |
| --- | --- |
| `watch_folders` | List of folders to monitor for new scans |
| `processing_dir` | Working area for files being processed |
| `quarantine_dir` | Where files that fail processing are put |
| `library_dir` | Where finished, sorted PDFs go |
| `db_path` | SQLite database location |
| `move_files` | `true` moves files out of watch folders; `false` copies them |
| `ocr` | OCR engine (`hybrid`, `tesseract` or `vision`), languages, confidence threshold, vision provider |
| `ai` | Named providers, plus which one to use for chat and for embeddings |

**API keys never go in `settings.json`.** Each provider names an environment variable
(`api_key_env`, for example `ANTHROPIC_API_KEY`) that holds its key.

## Tests

```powershell
cd backend; pytest; ruff check .
cd frontend; npm run lint; npm run build
```

## Roadmap

- [x] Project scaffolding
- [ ] UI design
- [ ] Watcher
- [ ] Worker (OCR → searchable PDFs)
- [ ] AI (chat with your docs)
- [ ] One-click installer (Windows/Mac)
