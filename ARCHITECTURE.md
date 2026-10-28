# Architecture

How Lindley works inside, the decisions behind it, and where its code lives. For what Lindley is
and how to install it, see the [README](README.md).

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
| `design/` | UI mockup, database design, benches and hand tests |
| `docs/` | Guides: the settings file, the command-line tools, adding an AI connector |
| `scripts/dev.ps1` | Runs both dev servers |
