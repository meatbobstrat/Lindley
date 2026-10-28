# Configuration

First-run Setup and Settings in the app cover what most people need. This is the settings file
underneath, for advanced use.

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
