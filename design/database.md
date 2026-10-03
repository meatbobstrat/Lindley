# Lindley database design

Schema version 2. The source of truth is [backend/src/lindley/db/schema.sql](../backend/src/lindley/db/schema.sql).
This document explains why the schema is shaped the way it is.

## Goals

1. **Intake keeps everything that could help match pages.** Each incoming file is squeezed for every clue it holds: what the file says about itself, what the image looks like, what the text says. All of it is kept with its source and confidence.
2. **Records fill in over time.** A document starts as a few pages and a guessed name. It gets closer to complete as pages are read, reviewed, dated and named. Completeness is worked out from the data, so it can never drift out of step.
3. **Nothing is destroyed.** Originals are never altered, and pages and readings are never deleted. A person's correction is added as a new reading.
4. **Every page is in one place.** A page is in the Inbox, in a document, or Set aside, and the database enforces it.
5. **Lindley suggests and people decide.** Anything Lindley proposes is stored as a suggestion, with its reasons, until a person accepts or dismisses it.

## Entities

```mermaid
erDiagram
    scans ||--|{ pages : "split into"
    pages ||--o{ transcriptions : "read as"
    pages ||--o{ facts : "found on"
    pages ||--o{ embeddings : "embedded as"
    pages ||--o{ page_links : "evidence (a,b)"
    documents |o--o{ pages : "holds, in order"
    documents ||--o{ facts : "about"
    folders |o--o{ documents : "files"
    folders |o--o{ folders : "contains"
    suggestions }o--o| pages : "about"
    suggestions }o--o| documents : "about"
    documents ||--o{ exports : "exported as"
    scans ||--o{ intake_steps : "processed by"
```

| Table | One row per | Why it exists |
|---|---|---|
| `scans` | incoming file | Where the file came from, its hash (to catch duplicates), EXIF data and import mode. A multi-page PDF or TIFF is one scan. |
| `pages` | page image | The unit people move around. Holds the image measurements and where the page lives (a document and position, Set aside, or neither, which means the Inbox). |
| `transcriptions` | reading of a page | Every reading from Tesseract, the vision model, or a person's correction. Exactly one is current. `confirmed_at` records that a person checked it. |
| `facts` | thing found | Dates, names, places, letterheads and so on, each with a source, a confidence, and its position on the page. Belongs to a page or to a document. |
| `embeddings` | page and model | A text vector for "related pages" and similarity matching. |
| `page_links` | page pair and relation | Evidence that two pages belong together, such as "continues", "same paper", or "adjacent file". |
| `documents` | document | Name (and whether a person chose it), status, folder, type, date, grouping confidence, summary. |
| `folders` | folder | The user's folder tree. |
| `suggestions` | proposal | "Add to document", "reorder", "name", "complete", and so on, with reasons shown in the UI. |
| `exports` | PDF made | When a document was exported, where to, and which pages were in it. |
| `intake_steps` | step run | Progress, errors, and the extractor version for each step, so a step can be re-run later. |
| `history` | action | Who did what (a person or Lindley), with before and after. Used for undo and the audit trail. |

Settings stay in `settings.json`. API keys stay in Windows Credential Manager and never go in the database.

## What intake extracts

Each step writes an `intake_steps` row, so the UI can show "Reading…" and a failed step can be retried alone.

| Step | Reads | Writes |
|---|---|---|
| `hash` | the file | `scans.sha256`. A known hash is flagged as a duplicate rather than imported twice. |
| `exif` | file system, EXIF | file created and modified times, `scanner_make` and `scanner_model`, `scanned_at`, `exif_json`, and a `file` fact from the file name (`scan_0042` is a sequence number) |
| `split` | PDF or TIFF | one `pages` row per page, with `page_index` |
| `image` | the page image | size, DPI, colour mode, `phash`, `paper_color`, `blank_score`, `detected_rotation`, `script` (handwritten, printed, typed, mixed or none) |
| `ocr` | Tesseract | a `transcriptions` row with word boxes and confidence, and `language` |
| `vision` | vision model | a `transcriptions` row for handwriting and low-confidence pages. It becomes current if it's the better reading, but never replaces a person's text. With `vision_mode = ask` (the default), the step is first recorded as `queued` ("Waiting for you to OK the vision model") and runs only when a person sends the waiting pages (`Pipeline.read_waiting`). A failed call is never repeated on its own. |
| `facts` | current text, image | `facts` rows (see below) |
| `embed` | current text | an `embeddings` row |
| `match` | everything above | `page_links` evidence, then `suggestions` |

### The image step

`lindley.worker.image` looks at a reduced copy of each page with its edges cropped off, so dark
scanner borders and shadows don't count as writing. The thresholds are a first guess, to be tuned
on real scans.

- `blank_score` is how little ink there is, after dust specks are filtered out: 0 means a full
  page of writing and 1 means blank. At 0.97 or above, a page counts as blank. A blank page
  is still read by Tesseract, so it has a current reading, but it isn't sent to the vision model
  and the assembler only suggests setting it aside.
- `paper_color` is the mean colour of the background pixels.
- `phash` is a 64-bit difference hash. Hashes within 4 bits of each other are the same picture
  (a `duplicate` link). Near-empty pages all hash alike, so they're never compared.
- `detected_rotation` comes from Tesseract's orientation check: the degrees clockwise that turn the
  page upright. It's used only when Tesseract is confident; otherwise it's 0. Pages with
  `detected_rotation + user_rotation` (or an EXIF orientation) are read from a turned copy in the
  processing folder, which is deleted afterwards. **Word boxes are therefore in upright
  coordinates.** `width_px` and `height_px` are the image as a viewer shows it (EXIF applied),
  before either rotation.
- `script` is set after reading, from Tesseract's confidence line by line. Mostly confident lines
  are `printed` and mostly poor ones are `handwritten`; a real share of both is `mixed`; a blank
  page is `none`. If Tesseract can't tell, it stays NULL. `typed` isn't told apart from
  `printed` yet.

The step runs once per page. A scan that's read again after a failure doesn't check its pages again.

### Kinds of fact

These are the starting kinds. New extractors add new kinds without a schema change.

| Kind | Example | Helps with |
|---|---|---|
| `date` | "March 4th 1892", normalized to `1892-03-04` | document date, ordering, matching |
| `person`, `place`, `organization` | "John Branson", "Xenia, O." | shared-name matching and search |
| `amount` | "3.50" | receipts and deeds |
| `salutation`, `closing`, `signature_name` | "Dear Sister,", "Your loving son", "Will" | where a letter starts and ends, and who wrote it |
| `letterhead`, `heading` | "XENIA FEED & SEED CO." | same-letterhead matching and document type |
| `page_marker` | "- 2 -" | page order |
| `first_line`, `last_line` | the opening and closing text | joining a sentence that runs from one page onto the next |
| `ink_color`, `stamp_seal` | "brown ink", "county seal" | same writer or sitting, and document type |
| `reference_number` | "Book 12, p. 340" | deeds and legal records |
| `document_type_hint` | "letter" | document type |

Each fact records `source` (file, exif, image, ocr, vision, rule or user), `source_detail` (the extractor and its version), `confidence`, `bbox` (so the UI can highlight it on the scan), and `status` (proposed, accepted or rejected). When a person corrects a fact, a new `user` fact is added and the old one is rejected. Nothing is overwritten.

## How matching works

```
facts, embeddings, image data  ->  page_links (evidence, one row per signal)
                               ->  suggestions (one proposal, with reasons)
                               ->  a person accepts or dismisses it  ->  history
```

- **Evidence is stored before any decision is made.** Every signal is stored in `page_links` separately, with a score and a readable `evidence` note, for example: "Page 2 ends mid-sentence ('…the auction in'); page 3 begins 'May, and Father…'".
- **The matcher combines the signals into one suggestion.** Its `reasons` are the plain-language list shown in the UI, for example "Same handwriting as page 2", "Mentions Mary and the auction in May".
- **Lindley's own groupings are ordinary rows.** When Lindley groups pages on its own (the italic, suggested names in the mockup), it creates a document with `origin = 'lindley'` and `name_source = 'lindley'`. Everything stays visible and can be undone through `history`.

## The assembler: from Inbox pages to documents

`lindley.assembler.assemble(conn, settings.assembler, chat)` runs once new scans have settled. It's safe to run as often as you like; a second run with nothing new changes nothing.

1. **Clues** (`clues.py`, rules only). A number standing alone at the top or bottom of a page is a page number ("- 2 -", "Page 2 of 3", "ii"; with word positions, the top or bottom 12% of the page). The rules also find:
   - greetings ("Dear Sister,"), letterheads and headings, which start a document;
   - closings and signatures ("Your loving son / John"), "Paid" and "Notary Public", which end one;
   - sentences cut off at the bottom of a page and picked up at the top of the next;
   - dates in old spellings, people, places, amounts, and file sequence numbers (`scan_0042`);
   - whether a page looks like a letter, receipt, deed, diary, stray note or blank.

   All of these are saved as `facts` with `source = 'rule'`.
2. **Evidence** (`evidence.py`). Each pair of neighbouring pages gets a same-document score from 0 to 1:
   - Scanned one after another: 0.62 to start with.
   - A greeting on the second page, or a signature on the first: minus 0.45 or 0.4.
   - Different kinds of page: minus 0.35.
   - Page numbers running n → n+1: at least 0.95.
   - A sentence carried over the break: plus 0.3.
   - Shared names or the same letterhead add a little more.

   Each signal is saved in `page_links` with a readable note.
3. **Grouping** (`segment.py`). Pages are put in scan order and cut wherever the score falls below 0.5.
   - **Order within a group:** page numbers first, then greeting first and signature last, then scan order.
   - **Rejoining parts scanned apart:** a group that lacks its end is joined to a group that lacks its start when one clearly continues the other. This catches two pages fed through the scanner swapped.
   - **Confidence:** each group gets one (0–100), based on how sure the breaks inside and around it are. It drops when the group has no clear start or end (a page may be missing), except for diaries.
4. **AI** (`ai.py`, only when a chat AI is set in Settings). The AI is asked only about breaks scoring 35–75 and groups whose order isn't settled. It gets page text and clues, never images.
   - Its reply must use every page given exactly once, and no others. Anything else is rejected and the rules' answer stands.
   - It also suggests names where the rules could only guess.
5. **Saving** (`apply.py`), in one transaction:

   | Situation | What Lindley does |
   |---|---|
   | The group clearly continues an open document that Lindley made and no one has touched | Adds the pages to it (`history`: `add_pages`) |
   | The same, but a person has named, changed or worked on the document | Suggests it (`add_to_document`); the page stays in the Inbox |
   | The group is confident (at or above `group_at`, default 75) | Creates a Lindley document with an italic name, its type, date, confidence and `reasons` (`history`: `group_pages`) |
   | A likely match (at or above `hint_at`, default 45) | Suggests it (`add_to_document`); the page stays in the Inbox |
   | A blank page or stray note | Suggests Set aside; never moves it |
   | A completed document | Never touches it |
   | A suggestion a person dismissed | Never makes it again |

**Test bench.** `lindley.assembler.bench` makes seeded batches of believable pages with known right answers:
- letters of 1–4 pages, some with page numbers and some broken mid-sentence;
- receipts, a 3-page deed, diary pages, notes and blanks;
- two pages swapped, and a last page that arrives in a later drop.

`scripts/bench_assembler.py` scores the assembler on them, and `scripts/demo_assembler.py` shows the Inbox shrinking round by round. Results at version 1, over 30 batches:

| | Pair F1 | Documents made | Wrong | Rebuilt exactly | Left in Inbox |
|---|---|---|---|---|---|
| Rules only | 0.94 | 240 | 0 | 96% | 4.5% |
| Rules + stand-in AI that is always right | 0.97 | 249 | 0 | 96% | 0.3% |

The rules were written knowing what the bench generates, so these numbers are a ceiling, not a forecast. Real scans carry OCR errors, odd layouts and messier scanning order. The bench's job is to catch regressions and to measure a real model (`--ai settings`) once OCR is running.

## Completeness

`lindley.db.progress.document_progress(conn, doc_id, review_below)` works out a checklist every time it's asked. Nothing is stored.

| Check | Done when |
|---|---|
| Has pages | the document holds at least one page |
| Every page read | every page's scan is read and has a current reading |
| Nothing waiting for review | no current reading is below the review threshold without being corrected or confirmed by a person |
| Named | `name_source = 'user'`, meaning a person chose or accepted the name |
| Date known | `doc_date` is set (partial dates like `1892-03` count) |
| Type known | `doc_type` is set |
| Page order settled | no open `reorder` suggestion |
| Exported | an `exports` row exists |

`ready` means everything except the export is done. That drives the mockup's banner, "Lindley thinks this document is complete." The review threshold comes from settings, so it's passed in rather than stored. Changing the threshold changes the checklist straight away.

## The Details tab (future UI round)

The Details tab reads directly from this data:

- **Page:** file and scanner information (`scans`), image measurements (`pages`), every reading with its source and confidence (`transcriptions`), and facts grouped by kind, each with a "found by" label and a highlight on the scan.
- **Document:** the completeness checklist, document facts, the evidence that holds the pages together (`page_links` between its pages), open suggestions with their reasons, and export history.

## Rules the schema enforces

- A page can't be both in a document and Set aside. A page in a document must have a position.
- A document that still holds pages can't be deleted, and neither can pages, readings or scans. Foreign keys have no cascade.
- Each page has at most one current reading (a partial unique index).
- A fact belongs to exactly one page or one document.
- A page link is stored once per pair, relation and source, with the smaller page id first.
- Searches go through `transcriptions_fts` joined on `is_current = 1`, so old readings aren't matched but are kept.

## Versioning

`PRAGMA user_version` holds the schema version (`SCHEMA_VERSION` in `database.py`). New databases are built from `schema.sql`. Existing ones run the numbered `MIGRATIONS` in order. A database from before version 1 (the placeholder tables) is rebuilt if it's empty, and refused with an explanation if it has data.

## Ask Lindley's view of the data

The AI gets read-only access through views, currently `v_page_location` and `v_current_text`, plus tools built on them. It never writes. Anything it wants to change becomes a suggestion for a person to decide.
