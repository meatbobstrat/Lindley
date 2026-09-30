# Lindley

[![CI](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml)

Lindley is a research partner for working through thousands of scanned pages: letters, deeds,
receipts, diaries, books and manuscripts. Drop scans into a folder and Lindley reads them, sorts
the pages into documents, and turns each document into a searchable PDF. You can also ask it
questions about everything it has read.

Lindley is an assistant, not an autopilot. It shows its reasoning, asks a person to check
anything it isn't sure of, and never changes or deletes your original scans.

## Why Lindley?

- Turn a box of loose scans into named, dated, ordered documents without sorting by hand
- Search every page, printed or handwritten
- Ask things like *"show me every letter mentioning John Branson in 1892"*
- Connect scattered pages and documents into a story

## Status

Lindley is in early development and isn't usable end to end yet.

| Part | State |
| --- | --- |
| Project scaffolding, CI | Done |
| UI design | Clickable mockup, close to MVP ([design/mockup](design/mockup/index.html)) |
| Database | Schema v2 designed and tested ([design/database.md](design/database.md)) |
| Assembler (Inbox pages → documents) | Built and tested on synthetic batches |
| Intake (hash, EXIF, split) and Tesseract reading | Built; try it on your scans with `scripts/intake.py` |
| Vision model reading (handwriting) | Wired into intake; the AI adapters are still stubs |
| Folder watcher | Next up |
| Searchable PDF export, search, AI chat | Not started |
| Real UI (React) | Scaffold only; to be built from the mockup |

## How it works

```
watched folders ─► watcher ─► intake ─────────────► assembler ────────────► Inbox / documents ─► PDF export
  (or Add scans)              hash, EXIF, split,     clues + evidence,        you review, rename,
                              image checks,          groups pages into        reorder, confirm
                              Tesseract / vision     documents; AI only
                              reading, facts         for the hard parts
                                        │
                                        └─► SQLite: every reading, fact and suggestion ─► search & Ask Lindley
```

1. **Intake.** Each new file is hashed so the same file is never imported twice. Multi-page PDFs
   and TIFFs are split into pages, and each page is read. Tesseract handles printed text; a
   vision model handles handwriting and pages Tesseract struggles with. Everything found on a
   page (dates, names, places, letterheads, page numbers, signatures) is stored with its source
   and a confidence score.
2. **Assembler.** Groups Inbox pages into documents.
   - **Rules first.** Old-fashioned text rules do most of the work. A number standing alone at
     the top or bottom of a page is a page number. "Dear Sister," starts a letter, and a closing
     plus a signature ends one. A sentence cut off at the bottom of a page carries on at the top
     of the next, and scanning order links neighbouring pages.
   - **AI for the hard parts.** The AI is asked only about uncertain breaks, page order and
     names. It sees page text, never images, and its answers are checked before they're used.
   - **What you see.** Confident groups appear under In progress with italic, suggested names
     and the reasons behind them. Less certain pages stay in the Inbox with an "Add to …?" hint.
3. **Review.** Any page read with less than 90% confidence (adjustable) goes to *Needs your
   review*, so a person checks it before it's trusted for search and chat.
4. **Export.** A finished document becomes a searchable PDF in your library.

## Design decisions

- **Lindley suggests; people decide.** Lindley only creates documents it's confident about.
  - It never changes a document a person has named, edited or completed. For those, it only
    makes suggestions.
  - It never repeats a suggestion you dismissed.
  - Every change it makes is logged and can be undone.
- **Nothing is destroyed.** Original scans are never altered. Corrections are added as new
  readings instead of overwriting old ones. Unwanted pages are *set aside* rather than deleted.
- **Everything lives in one place.** A page is in the Inbox, in one document, or set aside.
  A document is in one folder, or under In progress or Completed. The database enforces this.
- **Transparency.** Confidence scores, the source of every fact, and the reasons for every
  grouping are kept and shown.
- **Accessible.** The UI targets WCAG 2.2 AA, and status is never shown by colour alone.
- **Private by default.** Lindley works with an AI on your own computer (Ollama, LM Studio).
  - Cloud AI (Anthropic, OpenAI and others) is supported, but setup and Settings warn plainly
    that your scans are then sent to that company and are no longer private. You must
    acknowledge the warning before entering a key.
  - The app always shows where your pages and questions are sent.
  - With no AI connected, the rules and Tesseract still work.

## UI design

The design is a single-file clickable mockup: [design/mockup/index.html](design/mockup/index.html).
Open it in a browser; it uses sample data and saves nothing. It covers:
- first-run setup and Settings,
- the Inbox and adding scans,
- documents in three views (pages, reader, scan and text side by side),
- the review queue, folders and search,
- Ask Lindley,
- a movable action toolbar.

## Project layout

| Path | What's there |
| --- | --- |
| `backend/` | Python 3.13 and FastAPI (`src/lindley/`) |
| `backend/src/lindley/db/` | SQLite schema (with full-text search), migrations, document completeness |
| `backend/src/lindley/assembler/` | Clues, evidence, grouping, AI refinement, test bench |
| `backend/src/lindley/providers/` | Pluggable AI providers: Anthropic, OpenAI-compatible (OpenAI, Ollama, LM Studio, vLLM), and a fake one for tests |
| `backend/src/lindley/worker/` | Intake (hash, EXIF, split) and reading (Tesseract, vision model) |
| `backend/src/lindley/watcher/`, `search/` | Stubs for the next phase |
| `frontend/` | React, TypeScript and Vite |
| `design/` | UI mockup and database design |
| `scripts/dev.ps1` | Runs both dev servers |

## Setup (Windows)

Prerequisites: Python 3.13 and Node 22+. Reading scans needs
[Tesseract](https://github.com/UB-Mannheim/tesseract/wiki)
(`winget install UB-Mannheim.TesseractOCR`). Searchable PDF export will also need Ghostscript.

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
To run only the backend: `python -m lindley [--settings PATH] [--host HOST] [--port PORT]`.

### Try the assembler

The assembler can be tried now on made-up archive batches:

```powershell
cd backend
python scripts\demo_assembler.py              # watch the Inbox shrink as documents appear
python scripts\bench_assembler.py             # score it on 30 batches with known answers
python scripts\bench_assembler.py --ai settings   # include the chat AI from your settings
```

The rules were written knowing what the bench generates, so its scores are a ceiling, not a
forecast for real scans.

### Read your own scans

`scripts/intake.py` does what the folder watcher will: it imports scan files, reads every
page, and runs the assembler. Point it at a settings file whose `db_path`, `library_dir` and
`quarantine_dir` are in a scratch folder outside the repo:

```powershell
cd backend
python scripts\intake.py D:\scans --settings D:\scratch\settings.json
python scripts\intake.py D:\scans --settings D:\scratch\settings.json --no-ai   # rules and Tesseract only
```

Folders are searched recursively in natural name order (`scan_2` before `scan_10`). It's safe
to run again: files already read are skipped, and failed ones are tried again. Originals are
never changed, and with `move_files` they're removed only once their copy is checked.

## Config

Copy `settings.example.json` to `settings.json` and edit it. The app looks for the settings
file in these places, in order:

1. the `--settings <path>` command-line argument
2. the `LINDLEY_SETTINGS` environment variable
3. `settings.json` in the current directory
4. the per-user config folder

| Key | Meaning |
| --- | --- |
| `watch_folders` | Folders to watch for new scans |
| `processing_dir` | Working area for files being processed |
| `quarantine_dir` | Where files that fail processing are put |
| `library_dir` | Lindley's library: its copies of scans, and exported PDFs |
| `db_path` | SQLite database location |
| `move_files` | `true` moves scans out of watched folders; `false` copies them and leaves the originals |
| `ocr` | Reading engine (`hybrid`, `tesseract` or `vision`), languages, and `confidence_threshold`: below this, a Tesseract reading is retried with the vision model |
| `assembler` | `group_at`: confidence needed to create a document (75). `hint_at`: confidence needed for an "Add to …?" hint (45). `ai_band`: which uncertain breaks are sent to the AI. `use_ai`: turns the AI step on or off |
| `ai` | Named providers, plus which one to use for chat and for embeddings |

The review threshold (90%) is designed as a setting on the app's Settings screen. It isn't in
`settings.json` yet.

**API keys never go in `settings.json`.** For now, each provider names an environment variable
(`api_key_env`, for example `ANTHROPIC_API_KEY`) that holds its key. The design moves keys into
Windows Credential Manager, entered through the app's Settings screen.

## Tests

CI runs these on every push. Run them all before pushing:

```powershell
cd backend
ruff check .
ruff format --check .
pytest

cd ..\frontend
npm run lint
npm run build
```

Tests never touch real AI providers or the network; they use a fake provider and temporary
folders.

## Roadmap

- [x] Project scaffolding and CI
- [x] UI design (clickable mockup)
- [x] Database design
- [x] Assembler: grouping Inbox pages into documents
- [x] Intake: hashing, EXIF, splitting PDFs and TIFFs, Tesseract reading
- [ ] Folder watcher
- [ ] Image checks: blank pages, rotation, handwriting or print
- [ ] Vision model reading for handwriting (intake is ready; the AI adapters aren't)
- [ ] API and the real React UI, built from the mockup
- [ ] Searchable PDF export
- [ ] Search and Ask Lindley (chat with your documents)
- [ ] Details view: everything Lindley found about a page or document
- [ ] Settings in the app, with API keys in Windows Credential Manager
- [ ] One-click installer (Windows/Mac)
