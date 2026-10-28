# Command-line tools

Scripts in `backend/scripts/` that do what the app does, for trying Lindley on real scans and
benching it. Set up for development first ([CONTRIBUTING.md](../CONTRIBUTING.md)).

## Try the assembler

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

Results so far are in [design/database.md](../design/database.md#the-assembler-from-inbox-pages-to-documents).

## Read your own scans

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

## Make PDFs

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
