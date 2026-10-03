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
| Database | Schema v5 designed and tested ([design/database.md](design/database.md)) |
| Assembler (Inbox pages → documents) | Built; scored on synthetic batches and on real assembled typescripts |
| Intake (hash, EXIF, split) and Tesseract reading | Built; try it on your scans with `scripts/intake.py` |
| AI connections (a local AI, Anthropic, OpenAI, Google) | Built: one file per connector, each calling its AI through the company's own library; a connection per job, limits and throttling, keys in Windows Credential Manager. Tried against made-up servers; not yet on real scans with each AI |
| Vision model reading (handwriting) | Built into intake, through any AI connection that can read pages |
| Folder watcher | Built; runs with the backend |
| Image checks (blank pages, rotation, handwriting or print) | Built, and tried on a first sample of real typewritten scans |
| Duplicates (pages and documents scanned more than once) | Detection, decisions and API built; designed in the mockup |
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
   - **Rules first.** Old-fashioned text rules do most of the work. A number on a row of its
     own at the top or bottom of a page is a page number, read past specks and OCR slips.
     "Dear Sister," or a byline starts a document, and a closing plus a signature ends one. A
     sentence cut off at the bottom of a page carries on at the top of the next, and scanning
     order links neighbouring pages. Pages set out differently (margins, line spacing, line
     length) are kept apart.
   - **Weighed, not guessed.** Each piece of evidence has a weight in a small, explainable
     model. A fitting script can set the weights from documents whose right answer is known.
   - **AI for the hard parts.** The AI is asked only about uncertain breaks, page order and
     names, and only if one is connected: with none, you match those pages to documents by hand. It sees page text, never images, and its answers are checked before they're used.
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
- **Duplicates are found by what pages say.** A page scanned twice with different settings
  looks different but reads the same, so Lindley compares text, not files or pixels. The
  Duplicates folder shows the copies side by side. You choose what to keep, and the other copies
  are set aside, never deleted.
- **Everything lives in one place.** A page is in the Inbox, in one document, or set aside.
  A document is in one folder, or under In progress or Completed. The database enforces this.
- **Transparency.** Confidence scores, the source of every fact, and the reasons for every
  grouping are kept and shown.
- **Few dependencies.** Everything is a Python package except Tesseract, which the installer
  will include. Searchable PDFs are built from Lindley's own readings and word positions, so
  they need no Ghostscript or second OCR pass, and they carry people's corrections.
- **Accessible.** The UI targets WCAG 2.2 AA, and status is never shown by colour alone.
- **No AI calls without your OK.** AI calls can cost money, so by default Lindley makes none
  until you say so.
  - Each AI connection says when Lindley may use it: *Ask me first* (the default), or
    *Whenever it's needed*, with a daily and a monthly limit after which it asks again.
  - Each connection is also throttled, for every call: at most so many calls a minute, and so
    many at once. That keeps Lindley under a cloud AI's rate limits, and a slow computer usable.
  - Pages that need the vision model wait for you, with their Tesseract reading in use meanwhile.
    So do pages the AI could help sort.
  - Questions you type in Ask Lindley are always sent: asking is your OK.
  - Every call is recorded: which AI, what for, and whether you OKed it.
  - Pages are reduced before they're sent (2000 px on the longer side, as JPEG).
  - A call that failed is never repeated on its own. The one exception: when the AI says it's
    busy, the AI company's library waits as long as it asks and tries again, a few times.
  - Cloud AIs are asked not to keep what's sent (OpenAI and Google keep it unless asked).
- **Runs on an ordinary laptop.** Matching pages uses rules and a small model in plain Python,
  with no graphics card and no heavy machine-learning packages. A local AI is optional.
- **Private by default.** Lindley works with an AI on your own computer (Ollama, LM Studio).
  - Cloud AI (Anthropic, OpenAI, Google and others) is supported, but setup and Settings warn plainly
    that your scans are then sent to that company and are no longer private. You must
    acknowledge the warning before entering a key.
  - The app always shows where your pages and questions are sent.
  - Each job can have its own AI: reading hard pages, sorting pages into documents, answering
    questions, and finding related pages. For example, reading on this computer and questions
    with a cloud AI.
  - With no AI connected, the rules and Tesseract still work, and you match pages to documents
    by hand where the rules aren't sure. First-run setup asks for one AI connection, or none;
    more can be added in Settings.

## UI design

The design is a single-file clickable mockup: [design/mockup/index.html](design/mockup/index.html).
Open it in a browser; it uses sample data and saves nothing. It covers:
- first-run setup and Settings,
- the Inbox and adding scans,
- documents in three views (pages, reader, scan and text side by side),
- the review queue, folders and search,
- Duplicates: copies side by side, with their differences marked,
- Ask Lindley,
- a movable action toolbar.

## Project layout

| Path | What's there |
| --- | --- |
| `backend/` | Python 3.13 and FastAPI (`src/lindley/`) |
| `backend/src/lindley/db/` | SQLite schema (with full-text search), migrations, document completeness |
| `backend/src/lindley/assembler/` | Clues, evidence, grouping, AI refinement, test bench |
| `backend/src/lindley/providers/` | AI connectors, one file each in `connectors/`: a local AI (Ollama, LM Studio, vLLM), Anthropic, OpenAI, Google, any OpenAI-compatible service, and a fake one for tests. Plus limits, throttling and keys |
| `backend/src/lindley/worker/` | Intake (hash, EXIF, split) and reading (Tesseract, vision model) |
| `backend/src/lindley/watcher/` | Folder watcher: new scans are imported, read and assembled |
| `backend/src/lindley/duplicates/` | Duplicate detection (by text) and a person's decisions |
| `backend/src/lindley/search/` | Stub for the next phase |
| `frontend/` | React, TypeScript and Vite |
| `design/` | UI mockup and database design |
| `scripts/dev.ps1` | Runs both dev servers |

## Setup (Windows)

Prerequisites: Python 3.13 and Node 22+. Reading scans needs
[Tesseract](https://github.com/UB-Mannheim/tesseract/wiki)
(`winget install UB-Mannheim.TesseractOCR`). The installer will include it, so it's only a
step for development.

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
The backend watches the `watch_folders` in your settings. Scans dropped there are imported and
read once they've finished copying. About 20 seconds after the last one arrives, the assembler
sorts the new pages.

### Try the assembler

The assembler can be tried now on made-up archive batches:

```powershell
cd backend
python scripts\demo_assembler.py              # watch the Inbox shrink as documents appear
python scripts\bench_assembler.py             # score it on 30 batches with known answers
python scripts\bench_assembler.py --ai settings   # include the AI your settings give to sorting pages
```

The rules were written knowing what the bench generates, so its scores are a ceiling, not a
forecast for real scans. To score it on real scans, read some documents you've already put
together as PDFs into a scratch database with `scripts/intake.py`, then:

```powershell
python scripts\bench_assembler.py --real D:\scratch\lindley.db   # the PDFs are the answer key
python scripts\fit_assembler.py --real D:\scratch\lindley.db     # fit the evidence weights to them
```

Results so far are in [design/database.md](design/database.md#the-assembler-from-inbox-pages-to-documents).

### Read your own scans

`scripts/intake.py` does what the folder watcher does, in one go: it imports scan files, reads every
page, and runs the assembler. Point it at a settings file whose `db_path`, `library_dir` and
`quarantine_dir` are in a scratch folder outside the repo:

```powershell
cd backend
python scripts\intake.py D:\scans --settings D:\scratch\settings.json
python scripts\intake.py D:\scans --settings D:\scratch\settings.json --no-ai   # rules and Tesseract only
python scripts\intake.py --vision --settings D:\scratch\settings.json           # OK the waiting pages
python scripts\intake.py D:\scans --settings D:\scratch\settings.json --ai      # OK the chat AI to help sort
```

Folders are searched recursively in natural name order (`scan_2` before `scan_10`). It's safe
to run again: files already read are skipped, and failed ones are tried again. Originals are
never changed, and with `move_files` they're removed only once their copy is checked.

Before a page is read, Lindley checks the image. Blank pages are noted and aren't sent to the
vision model. Sideways or upside-down pages are read from a turned copy (Tesseract's orientation
check needs `osd.traineddata`, which the UB-Mannheim installer includes). Each page is also
marked handwritten, printed or mixed. The script's summary counts blank pages, turned pages and
each kind of writing.

Pages Tesseract struggles with wait for the vision model, and the summary says how many there
are and where they'd be sent. `--vision` is your OK to send them. `--vision --retry-failed` also
tries again the ones whose vision call failed.

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
| `ocr` | Reading engine (`hybrid`, `tesseract` or `vision`), languages, and `confidence_threshold`: below this, a page needs the vision model. `vision_max_side`: pages are reduced to this many pixels on their longer side before sending (2000) |
| `assembler` | `group_at`: confidence needed to create a document (75). `hint_at`: confidence needed for an "Add to …?" hint (45). `ai_band`: which uncertain breaks are sent to the AI |
| `ai.providers` | Named AI connections. `type` is a connector (`local`, `anthropic`, `openai`, `google`, `openai_compat`), with `base_url` and `model` where needed. `allow` is `ask` (the default: background work waits for your OK) or `auto` (sent as soon as there is some). `daily_limit` and `monthly_limit` cap the calls it makes on its own. `per_minute` and `at_once` throttle every call |
| `ai.jobs` | Which connection does each job: `vision` (reading hard pages), `assemble` (sorting pages into documents), `chat` (Ask Lindley) and `embed` (finding related pages), each with an optional `model` of its own. Out of the box there are none |

The review threshold (90%) is designed as a setting on the app's Settings screen. It isn't in
`settings.json` yet.

**API keys never go in `settings.json`.** They're kept in Windows Credential Manager (the
Keychain on a Mac), under "Lindley", with the connection's name. Save one through the API
(`PUT /api/connections/<name>/key`); the app's Settings screen will do the same. A connection can
instead name an environment variable that holds its key (`api_key_env`, for example
`OPENAI_API_KEY`).

### Adding an AI connector

Each AI service Lindley can use is one file in `backend/src/lindley/providers/connectors/`, and
every file there is found when Lindley starts. To add one, drop in a module that defines:

- `INFO`, a `ConnectorInfo`: its id (the `type` in settings), its name, whether it runs on your
  own computers or a company's, the jobs it can do, and its usual model for each.
- `Provider`, built as `Provider(config=..., model=..., api_key=...)`. It implements the calls its
  jobs need (`chat` and `chat_stream`, `transcribe`, `embed`), plus `check()`, which Test
  connection uses.

Call the AI through its company's own Python library, the way the company's documentation
shows, rather than writing the HTTP requests by hand. The library keeps up with changes to the
AI's API (updating it is usually all a change needs) and tries again when the AI is busy.
Dependabot (`.github/dependabot.yml`) opens a pull request each week a library has a new
release, and CI tests it; a new major version comes in a pull request of its own.
`_common.py` turns its errors into messages a person can read. Each built-in connector follows
its company's advice:

| Connector | Library | API |
| --- | --- | --- |
| `anthropic` | `anthropic` | Messages, with refusal fallbacks on the models that have them |
| `openai` | `openai` | Responses (embeddings: Embeddings), `store=False` |
| `google` | `google-genai` | Interactions (embeddings: `embed_content`), `store=False` |
| `local`, `openai_compat` | `openai`, at the server's address | Chat Completions, which Ollama, LM Studio and vLLM all support |

A service that speaks the OpenAI API needs only `INFO` and a subclass of `OpenAIChat` (see
`local.py`). Test it against a made-up server, as `tests/unit/test_connectors.py` does:
`httpx2.MockTransport` for the Anthropic and OpenAI libraries, `httpx.MockTransport` for
Google's. Settings and first-run setup list it with no other change.

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
- [x] Folder watcher
- [x] Image checks: blank pages, rotation, handwriting or print
- [x] Duplicates: detection, decisions with undo, API and mockup (the React screen comes with the real UI)
- [x] AI connectors, one file each: a local AI, Anthropic, OpenAI, Google, and other OpenAI-compatible services
- [x] Vision model reading for handwriting
- [ ] API and the real React UI, built from the mockup
- [ ] Searchable PDF export, built from stored readings (no Ghostscript)
- [ ] Search and Ask Lindley (chat with your documents)
- [ ] Details view: everything Lindley found about a page or document
- [x] API keys in Windows Credential Manager
- [ ] Settings in the app, built from the mockup
- [ ] Advanced settings (hidden from standard users): an interface for creating custom connectors. They're files too, built the same way as the built-in ones
- [x] When each AI may be used: ask first or automatic, with a daily and a monthly limit; every call recorded
- [x] Throttling each AI: calls a minute and at once
- [ ] Settings lists the models each AI offers (each library can list them), so the defaults
  can't go out of date
- [ ] AI spending: a monthly limit, and the cost of each call
- [ ] Suggest groups of pages Lindley isn't sure of (typescripts, notes) for a person to confirm
- [ ] One-click installer (Windows/Mac), with Tesseract included
