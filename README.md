# Lindley

[![CI](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml)
[![Installers](https://github.com/meatbobstrat/Lindley/actions/workflows/installers.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/installers.yml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Last commit](https://img.shields.io/github/last-commit/meatbobstrat/Lindley)](https://github.com/meatbobstrat/Lindley/commits/main)

![Python](https://img.shields.io/python/required-version-toml?tomlFilePath=https://raw.githubusercontent.com/meatbobstrat/Lindley/main/backend/pyproject.toml&logo=python&logoColor=white)
![Platforms: Windows, Ubuntu, macOS](https://img.shields.io/badge/platforms-Windows%20%7C%20Ubuntu%20%7C%20macOS-blue)
![AI: local, no telemetry](https://img.shields.io/badge/AI-local%2C%20no%20telemetry-6f42c1)
![Status: early development](https://img.shields.io/badge/status-early%20development-yellow)
[![License: free for personal use](https://img.shields.io/badge/license-free%20for%20personal%20use-blue)](LICENSING.md)

![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-20232a?logo=react&logoColor=61DAFB)
![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![llama.cpp](https://img.shields.io/badge/llama.cpp-333333)
![Tesseract](https://img.shields.io/badge/Tesseract-4a4a4a)

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
scans. Installers for Windows, Ubuntu and the Mac are built. The first hand test, on an Ubuntu
laptop, went from install to uninstall. It found that the rules sorted a batch of
typescripts right but were too unsure to show it, and the UI needs work before someone new can
find their way. Those come next, then Windows and the Mac (see [ROADMAP.md](ROADMAP.md) and
[design/hand-tests.md](design/hand-tests.md)).

| Part | State |
| --- | --- |
| Project scaffolding, CI | Done |
| UI design | Clickable mockup, close to MVP ([design/mockup](design/mockup/index.html)) |
| Database | Schema v17 designed and tested ([design/database.md](design/database.md)) |
| Assembler (Inbox pages → documents) | Built; scored on synthetic batches and on real assembled typescripts |
| Intake (hash, EXIF, split) and Tesseract reading | Built; try it on your scans with `scripts/intake.py`. Originals are never changed or deleted: move mode keeps them, unchanged, until they're exported |
| Lindley's own AI (llama.cpp's llama-server, with models downloaded when a tier is chosen) | Built: started when a job first needs it, offline, on this computer only; downloads resume and are checked |
| AI connections (Lindley's own, a local AI, Anthropic, OpenAI, Google) | Built: one file per connector, each calling its AI through the company's own library; a connection per job, limits and throttling, keys in Windows Credential Manager. Tried against made-up servers; not yet on real scans with each AI |
| Vision model reading (handwriting) | Built into intake, through any AI connection that can read pages |
| Folder watcher | Built; runs with the backend |
| Image checks (blank pages, rotation, handwriting or print) | Built, and tried on a first sample of real typewritten scans |
| Duplicates (pages and documents scanned more than once) | Detection, decisions, API and the app's screen built |
| Complete, then export | Built: a person marks a document complete, then exports it to the Exported folder they chose: one searchable PDF, from Lindley's own readings with people's corrections, and in move mode the original scans beside it; try it with `scripts/export.py`. after export, Lindley lets go of its own copies of the scans, and reopening brings them back from the originals |
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
4. **Complete, then export.** You mark a finished document complete, and Lindley locks it.
   Exporting it saves a searchable PDF in your Exported folder: each page is its scan, with what
   Lindley read from it as invisible text over the writing. If Lindley has been keeping your
   original scans (move mode), the document gets a folder of its own there, holding the PDF and
   those originals, unchanged. Corrections and
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
- **No AI calls without your OK.** AI calls can cost money, so by default Lindley sends nothing
  to another computer until you say so.
  - Each AI connection says when Lindley may use it: *Ask me first* (the default), or
    *Whenever it's needed*, with a daily and a monthly limit after which it asks again.
  - Lindley's own AI is the exception: it runs on this computer, sending nothing and costing
    nothing, so out of the box it works whenever it's needed. Settings can make it ask first.
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
Open it in a browser; it uses sample data and saves nothing. The app has moved on from it in one
place: the mockup exports a document in one step, while the app marks it complete, then exports
it (to the Exported folder chosen in Setup). The mockup covers:
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

## Installing

The [Installers workflow](.github/workflows/installers.yml) builds Lindley for Windows (an MSI),
Ubuntu 26.04 (a `.deb`) and the Mac (an app), kept as each run's artifacts. None is signed yet:
- **Windows** may warn that it's from an unknown publisher (More info › Run anyway).
- **Ubuntu:** `sudo apt install ./lindley_0.1.0-1~ubuntu-resolute_amd64.deb`, which brings
  Tesseract with it.
- **Mac:** install Tesseract first (`brew install tesseract`), then right-click Lindley ›
  Open the first time, past the warning that it's from an unidentified developer.

## Setup for development (Windows)

Prerequisites: Python 3.13 and Node 22+. Reading scans needs
[Tesseract](https://github.com/UB-Mannheim/tesseract/wiki)
(`winget install UB-Mannheim.TesseractOCR`). The Windows installer includes it, so it's only a
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
never changed or deleted: with `move_files` each is moved, unchanged, into the library's
`originals` folder once Lindley's own copy is checked, and only if it's still the file that was
read in (one a scanner is still writing stays where it is).

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

`scripts/export.py` completes documents and exports them as searchable PDFs, into the output
folder (`output_dir`):

```powershell
cd backend
python scripts\export.py --settings D:\scratch\settings.json            # list documents, and what each still needs
python scripts\export.py --doc 3 7 --settings D:\scratch\settings.json  # export these
python scripts\export.py --ready --settings D:\scratch\settings.json    # every document Lindley thinks is complete
python scripts\export.py --all --settings D:\scratch\settings.json      # every document
```

Each PDF holds the document's pages in order, named after the document. A document whose
original scans Lindley holds (move mode) gets a folder of its own, `<output_dir>/<name>/`, with
the PDF and those originals; an original with pages in another document too stays with Lindley,
and the export says so. In the app it's two steps: Mark complete, then Export, in a document's
toolbar (`POST /api/documents/<id>/complete`, then `/export`), or Export for every completed
document at once under Completed. Exporting a document again (reopen it first) replaces its
PDF.
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
| `watch_folders` | Folders to watch for new scans (`Documents/Lindley/Inbox`, made the first time it's watched). Setup won't start with a folder it can't find; Settings and the status bar mark one that isn't there (a path typed wrong, a drive unplugged), and it's watched once it's back |
| `processing_dir` | Working area for files being processed (`processing` in the data folder) |
| `quarantine_dir` | Where files that fail processing are put (`quarantine` in the data folder) |
| `library_dir` | Lindley's library (`Documents/Lindley/Library`): its working copies of scans (in `scans`), each page as an image (in `pages`), and the originals it took in move mode, unchanged (in `originals`), until they're exported. Once a scan is exported, Lindley lets go of its copies of it |
| `output_dir` | Where documents go when they're exported: each PDF, or in move mode a folder per document with its PDF and original scans. A person chooses it in Setup (which offers `Documents/Lindley/Exported`) or Settings; until then (`null`) nothing is exported |
| `db_path` | SQLite database location (`lindley.db` in the data folder). It's kept outside the library, which is often in a synced folder, where SQLite isn't safe. Back up both. To move it, copy it to the new place first (with its `-wal` file, if there is one): Settings refuses a place that doesn't hold Lindley's database |
| `move_files` | `true` moves scans out of watched folders into Lindley's keeping (the library's `originals`), unchanged; `false` copies them and leaves the originals where they are |
| `add_mode` | Files added with Add scans… in the Inbox: `ask` each time (the default), or always `copy` or `move` them. Moved, Lindley keeps the file and gives it back beside the PDF; a browser can't move files, so the person's own stays on their computer too |
| `ocr` | Reading engine (`hybrid`, `tesseract` or `vision`), languages, and `confidence_threshold`: below this (20 to 95; 70), a page needs the vision model. `review_below`: a page whose reading falls below this (50 to 99; 80) waits for a person's review. `vision_max_side`: pages are reduced to this many pixels on their longer side before sending (2000). `workers`: scans read at once; Tesseract uses one core a page, so a few side by side finish sooner (`null`: one fewer than the computer's cores, at most 3) |
| `assembler` | `group_at`: confidence needed to create a document (75). `hint_at`: confidence needed for an "Add to …?" or "Do these go together?" hint (45). `offer_at`: a group the AI checked, at or above this but below `group_at`, is offered for one-click accept (60). `ai_band`: which uncertain breaks may be sent to the AI. `ask_ai_after_days`: when the sorting AI may run on its own, how long pages wait for a person first (0: at once) |
| `ai.providers` | Named AI connections. `type` is a connector (`builtin`, Lindley's own AI; `local`, `anthropic`, `openai`, `google`, `openai_compat`), with `base_url` and `model` where needed. `allow` is `ask` (the default: background work waits for your OK) or `auto` (sent as soon as there is some; the default for `builtin`). `daily_limit` and `monthly_limit` cap the calls it makes on its own. `per_minute` and `at_once` throttle every call |
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
and you can ask it about them. All of it from a one-click install. The whole roadmap, item by
item, is in [ROADMAP.md](ROADMAP.md).

- **Done:** reading (Tesseract, and an AI for handwriting), sorting pages into documents,
  duplicates, search, searchable PDFs that leave the originals untouched, Ask Lindley, Lindley's
  own AI in three tiers, cloud AIs with limits and costs, the real UI (zoom, touch, the same
  controls everywhere), and installers for Windows, Ubuntu and the Mac
- **Left for the MVP:** hand tests on Ubuntu again, Windows and the Mac; the last decisions on
  keeping originals; tests for the frontend and a quicker first load
- **Before it's offered to anyone else:** a security pass: who can reach the app, headers, files
  added, dependencies, what's sent to a cloud AI, and signed installers
- **After the MVP:** a first run that shows someone new around, a desktop app of its own,
  installing for everyone on a Windows computer, more for Ask Lindley, every alphabet in PDFs,
  spending shown plainly with limits in dollars, and local models tried on more computers

## License

Lindley is free for a person to use for themselves. Organisations, and anyone using it to make
money, need a licence: see [LICENSING.md](LICENSING.md) for what that means, and [LICENSE](LICENSE)
for the terms.
