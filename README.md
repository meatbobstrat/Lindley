# Lindley

[![CI](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/ci.yml)
[![Installers](https://github.com/meatbobstrat/Lindley/actions/workflows/installers.yml/badge.svg)](https://github.com/meatbobstrat/Lindley/actions/workflows/installers.yml)
![Platforms: Windows, Ubuntu, macOS](https://img.shields.io/badge/platforms-Windows%20%7C%20Ubuntu%20%7C%20macOS-blue)
![AI: local, no telemetry](https://img.shields.io/badge/AI-local%2C%20no%20telemetry-6f42c1)
![Status: early development](https://img.shields.io/badge/status-early%20development-yellow)
[![License: free for personal use](https://img.shields.io/badge/license-free%20for%20personal%20use-blue)](LICENSING.md)

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

## Installing

The [Installers workflow](.github/workflows/installers.yml) builds Lindley for Windows (an MSI),
Ubuntu 26.04 (a `.deb`) and the Mac (an app), kept as each run's artifacts. None is signed yet:
- **Windows** may warn that it's from an unknown publisher (More info › Run anyway).
- **Ubuntu:** `sudo apt install ./lindley_0.1.0-1~ubuntu-resolute_amd64.deb`, which brings
  Tesseract with it.
- **Mac:** install Tesseract first (`brew install tesseract`), then right-click Lindley ›
  Open the first time, past the warning that it's from an unidentified developer.

## Getting started

1. Start Lindley from the menu. It opens in your browser and puts its icon in the tray; Quit
   Lindley is in the app's header.
2. First-run Setup asks which folders to watch (Lindley's own Inbox, in Documents/Lindley, to
   start with), whether to copy new scans or move them into Lindley's keeping, where finished
   documents go, how much AI this computer can run (Low, Middle or High, with a suggestion),
   and who does the rest (nobody, your own AI server, or a cloud AI).
3. Save or copy scans into a watched folder, or use Add scans… in the Inbox. Lindley reads
   them and sorts the pages into documents, and asks you about the pages it isn't sure of.
4. Check what it read in Needs your review, then name the documents and put them in folders.
5. Mark a document complete, then Export it: a searchable PDF in the folder you chose.
6. Search everything, or ask Lindley a question about it.

Settings changes any of these later. The settings file underneath is in
[docs/configuration.md](docs/configuration.md).

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

1. **Intake.** Each new scan is split into pages and read: Tesseract for printed text, a
   vision model for handwriting and pages Tesseract struggles with.
2. **Assembler.** Rules, weighed as evidence, group the pages into documents. Pages they aren't
   sure of wait in the Inbox with a hint, and an AI is asked only about the hard parts.
3. **Review.** A page read with low confidence waits for a person to check it before it's
   trusted for search and chat.
4. **Complete, then export.** A finished document becomes a searchable PDF: each page is its
   scan, with what Lindley read as invisible text over the writing.

More in [ARCHITECTURE.md](ARCHITECTURE.md).

## Privacy and your scans

- **Your originals are never changed or deleted.** Lindley works on its own copy. In move mode
  it keeps the originals, unchanged, and gives them back beside the PDF.
- **Lindley suggests; you decide.** Every change can be undone, and it never changes a document
  you've named, edited or completed.
- **Private by default.** Lindley's own AI runs on your computer, offline, and sends nothing.
  It also works with an AI server of your own (Ollama, LM Studio).
- **Nothing sent without your OK.** A cloud AI (Anthropic, OpenAI, Google) is used only with a
  key you add, after a plain warning that your scans will leave your computer. Each connection
  asks first, or works on its own within daily and monthly limits, and every call is recorded.
- **Runs on an ordinary laptop.** No graphics card needed. With no AI at all, the rules and
  Tesseract still read and sort.

The full list is in [ARCHITECTURE.md](ARCHITECTURE.md#design-decisions).

## Status

Lindley is in early development. The app works end to end, from scans dropped in a folder to
searchable PDFs and questions answered about them, and installers for Windows, Ubuntu and the Mac
are built. What the first hand test, on an Ubuntu laptop, found has been fixed; next is trying it
there again, then on Windows and the Mac ([design/hand-tests.md](design/hand-tests.md)).

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

## Documentation

- [ROADMAP.md](ROADMAP.md): what's done and what's next, item by item
- [CONTRIBUTING.md](CONTRIBUTING.md): setting up for development, tests, and the rules the code
  keeps
- [ARCHITECTURE.md](ARCHITECTURE.md): how Lindley works inside, its design decisions, and where
  the code lives
- [docs/configuration.md](docs/configuration.md): the settings file
- [docs/scripts.md](docs/scripts.md): reading scans, sorting pages and making PDFs from the
  command line
- [docs/connectors.md](docs/connectors.md): adding an AI connector
- [design/](design/): the UI mockup, the database design, benches and hand tests

## License

Lindley is free for a person to use for themselves. Organisations, and anyone using it to make
money, need a licence: see [LICENSING.md](LICENSING.md) for what that means, and [LICENSE](LICENSE)
for the terms.
