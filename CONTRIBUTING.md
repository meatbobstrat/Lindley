# Contributing

How to set up Lindley for development, run its tests, and the rules its code keeps. How it works
inside is in [ARCHITECTURE.md](ARCHITECTURE.md).

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

## Rules the code keeps

- A person's original scans are never altered or deleted. Lindley works on its own copy.
- The repo is public: never commit `settings.json`, `.env`, databases or scanned data. API keys
  live in the system's credential store, never in a file.
- Tests use a fake AI provider and temporary folders, and never touch real providers or the
  network.
- Never call an AI while a database write is open: a call can take minutes and holds SQLite's
  lock.
- Lindley's own AI and ML run on an ordinary laptop: no graphics card needed, no heavy ML
  packages.

## More

- [docs/scripts.md](docs/scripts.md): reading scans, sorting pages and making PDFs from the
  command line, and benching the assembler
- [docs/configuration.md](docs/configuration.md): the settings file
- [docs/connectors.md](docs/connectors.md): adding an AI connector
- [design/](design/): the UI mockup, the database design, benches and hand tests
