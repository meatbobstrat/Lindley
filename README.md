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

Lindley is in early development. The app works end to end, from scans dropped in a folder to
searchable PDFs and questions answered about them, and has been tried on a first batch of real
scans. What's left for the MVP is the one-click installer (see the Roadmap).

| Part | State |
| --- | --- |
| Project scaffolding, CI | Done |
| UI design | Clickable mockup, close to MVP ([design/mockup](design/mockup/index.html)) |
| Database | Schema v5 designed and tested ([design/database.md](design/database.md)) |
| Assembler (Inbox pages → documents) | Built; scored on synthetic batches and on real assembled typescripts |
| Intake (hash, EXIF, split) and Tesseract reading | Built; try it on your scans with `scripts/intake.py` |
| Lindley's own AI (llama.cpp's llama-server, with models downloaded when a tier is chosen) | Built: started when a job first needs it, offline, on this computer only; downloads resume and are checked |
| AI connections (Lindley's own, a local AI, Anthropic, OpenAI, Google) | Built: one file per connector, each calling its AI through the company's own library; a connection per job, limits and throttling, keys in Windows Credential Manager. Tried against made-up servers; not yet on real scans with each AI |
| Vision model reading (handwriting) | Built into intake, through any AI connection that can read pages |
| Folder watcher | Built; runs with the backend |
| Image checks (blank pages, rotation, handwriting or print) | Built, and tried on a first sample of real typewritten scans |
| Duplicates (pages and documents scanned more than once) | Detection, decisions, API and the app's screen built |
| Searchable PDF export | Built: one PDF per document, from Lindley's own readings, with people's corrections; try it with `scripts/export.py` |
| Search | Built: every page's text as it reads now, with the words found marked |
| Ask Lindley (AI chat) | Built: questions about your documents, answered by the chat AI from the pages Lindley finds, citing each page; kept as conversations. With no AI for it, the pane finds words and what's waiting for review |
| Real UI (React) | Built from the mockup: every view, on the real API, with a tooltip for every control |

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
     "Dear Sister," a byline, or a date line with who a letter is to ("Ely, Nevada, June 24,
     1940 / Honorable Grey Mashburn,") starts a document, and a closing plus a signature ends one. A
     sentence cut off at the bottom of a page carries on at the top of the next, and scanning
     order links neighbouring pages. Pages set out differently (margins, line spacing, line
     length) are kept apart.
   - **Weighed, not guessed.** Each piece of evidence has a weight in a small, explainable
     model. A fitting script can set the weights from documents whose right answer is known.
   - **You before the AI.** Pages the rules aren't sure about stay in the Inbox with a hint:
     "Do these go together?" or "Add to …?". Your answer costs nothing. Groups the AI checked
     but was less sure of (60% or more, `assembler.offer_at`) come first, with its reasons,
     ready to accept with one click, or all at once.
   - **AI as the last resort.** The AI is asked only about uncertain breaks, page order and
     names, and only if one is connected. If its connection may run on its own, it's asked as
     they arrive (or after `ask_ai_after_days`, to give you first go). Otherwise they wait in
     *Needs AI* until you send them. Its answers are kept, so the same question is never paid
     for twice. It sees page text, never images, and its answers are checked before they're
     used.
   - **Needs AI.** One place for every scan waiting for an AI: pages too hard to read
     (Tesseract below 70% confidence, waiting for the vision model) and pages the rules
     couldn't sort, each with the rules' own guess at the documents. Send one, or all. A page
     waiting for your review can be sent too ("Ask the AI", in Review or on the scan).
   - **What you see.** Confident groups appear under In progress with italic, suggested names
     and the reasons behind them.
3. **Review.** Any page read with less than 80% confidence (adjustable) goes to *Needs your
   review*, so a person checks it before it's trusted for search and chat. AIs don't say how
   sure they are, so a vision model's confidence is the share of words it didn't mark as
   unsure or illegible. A reading that's mostly `[illegible]` doesn't replace Tesseract's. The
   80% bar was measured: against Claude, Tesseract got 16% of characters wrong on pages it read
   at 70–79%, 9% at 80–89% and 2% at 90% or more.
4. **Export.** A finished document becomes a searchable PDF in your library: each page is its
   scan, with what Lindley read from it as invisible text over the writing. Corrections and
   the vision model's readings are laid over Tesseract's word positions, so search and
   selection land on the right words.

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
  - Questions you type in Ask Lindley are always sent: asking is your OK. Each question is two
    calls: one to name the words to search the pages for, and the answer, sent with the pages
    found (about 6,000 characters of them for an AI on your own computers, 40,000 for a cloud AI).
  - Every call is recorded: which AI, what for, and whether you OKed it.
  - Pages are reduced before they're sent (2000 px on the longer side, as JPEG).
  - A call that failed is never repeated on its own. The one exception: when the AI says it's
    busy, Lindley waits as long as it asks and tries again, twice. A call that took too long
    isn't sent again: the AI may still be working on it, and a cloud AI charges for each one.
  - Cloud AIs are asked not to keep what's sent (OpenAI and Google keep it unless asked).
- **Runs on an ordinary laptop.** Matching pages uses rules and a small model in plain Python,
  with no graphics card and no heavy machine-learning packages. A local AI is optional.
- **Private by default.** Lindley comes with its own AI, which runs on your computer:
  llama.cpp's `llama-server`, with models it downloads when you choose a tier, once, and then
  runs offline. It also works with an AI on another computer you own (Ollama, LM Studio).
  - Cloud AI (Anthropic, OpenAI, Google and others) is supported, but setup and Settings warn plainly
    that your scans are then sent to that company and are no longer private. You must
    acknowledge the warning before entering a key.
  - The app always shows where your pages and questions are sent.
  - Each job can have its own AI: reading hard pages, sorting pages into documents, answering
    questions, and finding related pages. For example, reading on this computer and questions
    with a cloud AI.
  - With no AI connected, the rules and Tesseract still work, and you match pages to documents
    by hand where the rules aren't sure. First-run setup asks two things: how much AI this
    computer can run (Low, Middle or High), and who does the rest (nobody, your own AI server,
    or a cloud AI). It gives each job an AI from the two; Settings can change either, or any
    job's AI.

## UI design

The app in `frontend/` is built from the mockup, with the same look and wording. Every
change you make there can be undone (Undo in the message that confirms it, or Ctrl+Z), and every
button, icon and status has a tooltip, shown on hover and on keyboard focus and closed with
Escape. The fonts are bundled, so the app never asks another site for anything.

The design itself is a single-file clickable mockup: [design/mockup/index.html](design/mockup/index.html).
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
| `backend/src/lindley/localai/` | Lindley's own AI: the pinned llama.cpp build and models (`catalog.py`), downloading them when a person asks, and running `llama-server` |
| `backend/src/lindley/providers/` | AI connectors, one file each in `connectors/`: Lindley's own AI, a local AI (Ollama, LM Studio, vLLM), Anthropic, OpenAI, Google, any OpenAI-compatible service, and a fake one for tests. Plus limits, throttling and keys |
| `backend/src/lindley/worker/` | Intake (hash, EXIF, split) and reading (Tesseract, vision model) |
| `backend/src/lindley/watcher/` | Folder watcher: new scans are imported, read and assembled |
| `backend/src/lindley/duplicates/` | Duplicate detection (by text) and a person's decisions |
| `backend/src/lindley/export/` | Searchable PDFs: word positions for each reading, the PDF, and exporting a document |
| `backend/src/lindley/search/` | Full-text search over every page's reading in use (SQLite FTS5) |
| `backend/src/lindley/ask/` | Ask Lindley: the words to search for, the pages found, and the answer, kept as conversations |
| `frontend/` | The app: React, TypeScript and Vite (`views/` per screen, `components/`, `ui/` for tooltips, toolbar, menus and dialogs) |
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
Started that way (or from the menu, once installed), Lindley serves the built UI, opens it in
the browser and puts its icon in the tray, with Open and Quit; started again, it opens the one
already running. `--no-browser` and `--no-tray` leave those out. Quit Lindley (the power button
in the app) stops it; closing the tab leaves it running. Its log is kept beside the database
(`logs/lindley.log`), since an app started from the menu has no console.
Lindley has no login, so it answers only to `127.0.0.1` and `localhost`, and only its own pages
may change anything; other web pages can't reach it. Started with `--host` on a network
address, it answers to any name: only do that on a network you trust.
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
vision model. Tesseract finds which way up a page is as it reads it; an upside-down page's words
are turned back with it, and a sideways page is read again from a turned copy (this needs
`osd.traineddata`, which the UB-Mannheim installer includes). A page that still reads poorly
is tried turned round left to right, in case it's a mirror image such as the back of a carbon
copy, and a person can flip any page. A page a person turns or flips in the app (or turns
back with Undo) is read again by Tesseract the new way round, in the background; the new text
replaces Tesseract's own, but never a person's text or an AI's better reading. Each page is also
marked handwritten, printed or mixed. The script's summary counts blank pages, turned pages and
each kind of writing.

Pages Tesseract struggles with wait for the vision model, and the summary says how many there
are and where they'd be sent. `--vision` is your OK to send them. `--vision --retry-failed` also
tries again the ones whose vision call failed.

### Make PDFs

`scripts/export.py` turns documents into searchable PDFs, in the library's `Exports` folder:

```powershell
cd backend
python scripts\export.py --settings D:\scratch\settings.json            # list documents, and what each still needs
python scripts\export.py --doc 3 7 --settings D:\scratch\settings.json  # export these
python scripts\export.py --ready --settings D:\scratch\settings.json    # every document Lindley thinks is complete
python scripts\export.py --all --settings D:\scratch\settings.json      # every document
```

Each PDF holds the document's pages in order, named after the document, and exporting marks
the document Completed. Exporting it again replaces the PDF. In the app, it's Export PDF in a
document's toolbar (`POST /api/documents/<id>/export`).
- **What's in it.** Each page is its scan, turned upright, at its size on paper (from the
  scan's dpi). What Lindley read from it is invisible text over the writing: a person's
  correction if there is one, else the vision model's reading or Tesseract's.
- **What the export reports.** Pages still waiting for your review are exported with
  Lindley's best reading, and the export says how many there are. So are pages read only by
  the vision model, with no Tesseract word positions: their text is searchable but not over
  the writing.
- **Letters.** The text uses the PDF viewer's built-in Helvetica, so it needs no font file.
  That covers English and western European letters, curly quotes and dashes. Other characters
  become their plain letter ("ſ" is "s"), else "?".

## Config

Copy `settings.example.json` to `settings.json` and edit it. The app looks for the settings
file in these places, in order:

1. the `--settings <path>` command-line argument
2. the `LINDLEY_SETTINGS` environment variable
3. `settings.json` in the current directory
4. the per-user config folder

Out of the box, scans and PDFs go in `Lindley` in your Documents folder, and the database and
work in progress in your user data folder (`%LOCALAPPDATA%\Lindley` on Windows), never beside
the program.

| Key | Meaning |
| --- | --- |
| `watch_folders` | Folders to watch for new scans (`Documents/Lindley/Inbox`) |
| `processing_dir` | Working area for files being processed (`processing` in the data folder) |
| `quarantine_dir` | Where files that fail processing are put (`quarantine` in the data folder) |
| `library_dir` | Lindley's library (`Documents/Lindley/Library`): its copies of scans (in `scans`), each page as an image (in `pages`), and exported PDFs (in `Exports`) |
| `db_path` | SQLite database location (`lindley.db` in the data folder). It's kept outside the library, which is often in a synced folder, where SQLite isn't safe. Back up both |
| `move_files` | `true` moves scans out of watched folders; `false` copies them and leaves the originals |
| `add_mode` | Files added with Add scans… in the Inbox: `ask` each time (the default), or always `copy` or `move` them |
| `ocr` | Reading engine (`hybrid`, `tesseract` or `vision`), languages, and `confidence_threshold`: below this (20 to 95; 70), a page needs the vision model. `review_below`: a page whose reading falls below this (50 to 99; 80) waits for a person's review. `vision_max_side`: pages are reduced to this many pixels on their longer side before sending (2000). `workers`: scans read at once; Tesseract uses one core a page, so a few side by side finish sooner (`null`: one fewer than the computer's cores, at most 3) |
| `assembler` | `group_at`: confidence needed to create a document (75). `hint_at`: confidence needed for an "Add to …?" or "Do these go together?" hint (45). `offer_at`: a group the AI checked, at or above this but below `group_at`, is offered for one-click accept (60). `ai_band`: which uncertain breaks may be sent to the AI. `ask_ai_after_days`: when the sorting AI may run on its own, how long pages wait for a person first (0: at once) |
| `ai.providers` | Named AI connections. `type` is a connector (`builtin`, Lindley's own AI; `local`, `anthropic`, `openai`, `google`, `openai_compat`), with `base_url` and `model` where needed. `allow` is `ask` (the default: background work waits for your OK) or `auto` (sent as soon as there is some). `daily_limit` and `monthly_limit` cap the calls it makes on its own. `per_minute` and `at_once` throttle every call |
| `ask` | Ask Lindley: `local_chars` and `cloud_chars`, how much page text goes with a question to an AI on your own computers (6000) or a cloud AI (40000); `history_turns`, how many earlier questions and answers go with it (6) |
| `ai.jobs` | Which connection does each job: `vision` (reading hard pages), `assemble` (sorting pages into documents), `chat` (Ask Lindley), `embed` (finding related pages) and `continues` (whether a page carries on from the last: only an AI that gives token probabilities, such as Lindley's own), each with an optional `model` of its own. Out of the box there are none |
| `ai.tier`, `ai.help` | How much AI this computer runs (`low`, `middle` or `high`; see `providers/tiers.py`), and the connection that does the jobs it leaves (`null`: nobody). Setup and Settings set them; `tier` is `null` once a person changes a job's AI |
| `ai.local` | Lindley's own AI: `models_dir`, where its engine and models go (`null`: the per-user data folder); `server_path`, a `llama-server` of your own; `device`, the graphics it may use (`null`: any; `none`: the processor alone; or one from `llama-server --list-devices`) |

**API keys never go in `settings.json`.** They're kept in Windows Credential Manager (the
Keychain on a Mac, the desktop's keyring on Linux), under "Lindley", with the connection's name.
Settings › AI and privacy saves it there for you (through `PUT /api/connections/<name>/key`). A
connection can instead name an environment variable that holds its key (`api_key_env`, for
example `OPENAI_API_KEY`). A Linux desktop with no keyring running gets that advice in words:
Lindley never keeps a key in a plain file.

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

The MVP is what the top of this page promises, for someone who isn't a developer: drop scans
in a folder, and Lindley reads them, sorts the pages into documents and makes searchable PDFs,
and you can ask it about them. All of it from a one-click install.

### Done

- [x] Project scaffolding and CI
- [x] UI design (clickable mockup)
- [x] Database design
- [x] Assembler: grouping Inbox pages into documents
- [x] Intake: hashing, EXIF, splitting PDFs and TIFFs, Tesseract reading
- [x] Folder watcher
- [x] Image checks: blank pages, rotation, handwriting or print
- [x] Duplicates: detection, decisions with undo, API, mockup and the app's screen
- [x] AI connectors, one file each: a local AI, Anthropic, OpenAI, Google, and other OpenAI-compatible services
- [x] Vision model reading for handwriting
- [x] API and the real React UI, built from the mockup
- [x] Searchable PDF export, built from stored readings (no Ghostscript)
- [x] Search: full text, over every page's reading in use
- [x] API keys in Windows Credential Manager
- [x] Settings in the app, built from the mockup
- [x] When each AI may be used: ask first or automatic, with a daily and a monthly limit; every call recorded
- [x] Throttling each AI: calls a minute and at once
- [x] The app tried end to end on real scans, and what broke fixed (AI work in the background, no
  hidden retries, cut-off answers and doubtful AI readings caught)
- [x] Benches for local AI models, with hand-made PDFs matched back to their scans as the answer
  key; typed pages lying upside down now turned, including ones read before
- [x] Mirror images (the back of a carbon copy) found and turned round, including ones read
  before; a person can flip a page too
- [x] Claude (Opus 5.5) tried on real scans: it read 48 hard pages at about 3¢ a page, and sorted
  pages into 9 documents, leaving the groups it was less sure of as suggestions (design/database.md, "Claude on real scans")

### For the MVP

- [x] Ask Lindley (chat with your documents): the AI names the words to search for, Lindley
  finds the pages (the one open first), and the answer cites them; it streams, can be stopped, and
  is kept. Available when the chat job has an AI that can be used; an AI set up but not chosen is
  offered in one click
- [x] Record each sorting call as it's made, not when the whole job ends, so a job cut short
  still shows what it spent
- [x] See where a letter starts without "Dear …", from its date line and who it's to: the rules
  had run a 1940 letter on into the typescript before it (design/database.md, "Confidence bars")
- [x] Show progress while the AI sorts pages: the status bar counts the questions it asks the AI
  ("AI sorting 143 pages · question 2 of 4 so far": answers can raise more), and shows the
  AI sorting on its own too
- [x] Read a page again with Tesseract when a person turns it or flips it left to right in the
  app (or undoes either): before, its text stayed as read the wrong way up or from the mirror
  image. The new reading replaces Tesseract's own, never a person's text or an AI's better
  reading, the Inbox is sorted again with it, and the status bar says so while it's read
- [x] Two AI choices in Setup and Settings, in place of a list of performance tiers: how much AI
  this computer runs, and who does the rest (`providers/tiers.py`). Each job gets the tier's model
  on Lindley's own AI, else the help if it can do the job, else nothing; the choice of AI for each
  job stays underneath, and changing one makes it your own. Every job done on this computer is
  one that isn't sent anywhere or paid for, so the cost of a page falls, and privacy rises, from
  Low to High:
  - **Low** (any computer, even an old one): no AI on this computer. Tesseract and the rules
    read and sort; the help does the rest
  - **Middle** (8 GB of memory): Gemma 4 E2B checks whether each page carries on from the last
    (`lm_continues`), so sorting needs the help less. Reading handwriting, sorting the rest and
    answering questions go to the help. Whether Middle should read handwriting itself needs
    benches on more computers: on a 2019 desktop processor Gemma 4 E4B takes nearly 3 minutes
    a hard page
  - **High** (16 GB, or a graphics card): Gemma 4 E4B reads handwriting, sorts, answers and
    checks pages, all on this computer (6 seconds a hard page on an 8 GB graphics card)
  - **Who does the rest**: nobody (what this computer doesn't do waits for you), your own AI
    server (Ollama, LM Studio or llama-server on another computer), or a cloud AI with a key
- [x] A local AI that comes with Lindley: llama.cpp's `llama-server` (v0.6.0, the Vulkan build),
  started by Lindley when a job first needs it and stopped with it, offline, on this computer
  only (`localai/`). Choosing a tier downloads its model and the engine, once, from Hugging
  Face and GitHub, each file pinned and checked, with progress and Cancel in Settings and the
  status bar. If the graphics can't load a model it runs on the processor alone. Ollama and LM
  Studio stay supported through the local connection. Benched on a 2019 desktop processor and an
  8 GB graphics card (design/database.md, "Lindley's own AI, measured"); this computer's built-in
  graphics have too old a driver to bench
- [x] Whether a page carries straight on from the last, checked by a small local model
  (`lm_continues`, the `continues` job): asked only about pairs the rules may get wrong, its
  "yes" weighed as evidence. On the 23 documents it lifted the groups proposed a little (57% to
  61% rebuilt exactly, none wrong); its "no" split pages that do run on, so it isn't weighed
- [x] A look at the computer at first run (processor, memory, graphics card, free disk) that
  suggests a tier, with how long 100 pages would take (`localai/computer.py`, `tiers.suggest`).
  Setup starts on the highest tier the memory or graphics card can run, a step lower when its
  download wouldn't fit on the disk, and says why. Each tier gives its time for 100 typed and
  100 handwritten pages, from speeds measured on one 2019 desktop: on it, High takes about 6
  minutes for typed pages on the graphics card, and nearly 2 hours on the processor alone,
  most of it sorting (116 s a question, against 2.8 s on the card). Benches on more computers
  wait for the installer
- [ ] One-click installer (Windows/Mac/Linux), with Tesseract included (on Linux, installed with
  it). Python 3.13 bundled, and the built app found wherever it's installed, not by the repo's
  folders. Linux, Ubuntu to start: a `.deb` built on Ubuntu 22.04 that installs Tesseract,
  Vulkan and its drivers with it, with a menu entry that starts Lindley and opens it in the
  browser. Lindley's own AI gets llama.cpp's Ubuntu Vulkan build (a `.tar.gz`, like the Mac's),
  and stops with Lindley as on Windows (a process group, and a signal if Lindley dies). Keys go
  in the desktop's keyring, with plain words when there isn't one

### After the MVP

- [ ] Ask Lindley, next:
  - related pages by meaning (embeddings) alongside the words searched for, once the `embed`
    job fills the `embeddings` table
  - what an answer stopped part-way used (an AI says only at the end)
  - for a cloud AI that handles them well, tools over the read-only views, so it can look
    further itself
  - "Ask Lindley about this page" in the toolbars and right-click menus, as in the mockup
  - a local model benched for answering questions, for High
- [ ] Searchable text for every alphabet: ship a glyphless font, so text outside Windows-1252
  (Greek, Cyrillic, Hebrew and so on) goes into the PDF as it was read
- [ ] Details view: everything Lindley found about a page or document
- [ ] Advanced settings (hidden from standard users): an interface for creating custom connectors. They're files too, built the same way as the built-in ones
- [ ] Settings lists the models each AI offers (each library can list them), so the defaults
  can't go out of date
- [x] AI spending: a monthly limit, and the tokens and estimated cost of each call, shown in
  Settings. Claude's, OpenAI's and Google's calls are priced from their list prices
  (`providers/prices.py`); a local AI's calls count tokens, at no cost
- [ ] Suggest groups of pages Lindley isn't sure of (typescripts, notes) for a person to confirm
- [ ] Local AI models for intake, tuned on real scans, on more computers. Measured so far
  (design/database.md, "Local models" and "Lindley's own AI, measured"): none makes intake
  quicker, and Tesseract stays for typed pages. Next:
  - laptops, and built-in graphics with a current driver, before the first-run look at the
    computer suggests tiers from them
  - EmbeddingGemma for what pages are about, if it helps build documents (no tier downloads it
    until something uses it)
