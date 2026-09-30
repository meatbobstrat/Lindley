-- Lindley schema, version 1 (database.SCHEMA_VERSION; init_db sets PRAGMA user_version).
-- Applied idempotently at startup by lindley.db.database.init_db.
-- Design notes: design/database.md.
--
-- Rules the schema enforces:
--   * Nothing is destroyed. Pages and readings are never deleted; documents can only be
--     removed once they have no pages (foreign keys have no ON DELETE action).
--   * Every page lives in exactly one place: a document, Set aside, or the Inbox (neither).
--   * Whatever Lindley works out is recorded with its source and confidence, so the UI can
--     say why.

-- ---------------------------------------------------------------- Files and pages

-- One row per incoming file. A multi-page PDF or TIFF is one scan with many pages.
CREATE TABLE IF NOT EXISTS scans (
    id               INTEGER PRIMARY KEY,
    sha256           TEXT NOT NULL UNIQUE,          -- the same file twice is caught here
    original_name    TEXT NOT NULL,
    source_path      TEXT NOT NULL,                 -- where it was found
    origin           TEXT NOT NULL CHECK (origin IN ('watched', 'added')),
    import_mode      TEXT NOT NULL CHECK (import_mode IN ('copy', 'move')),
    library_path     TEXT,                          -- Lindley's own copy, never altered
    mime_type        TEXT,
    file_size        INTEGER,
    page_count       INTEGER,
    file_created_at  TEXT,
    file_modified_at TEXT,
    scanner_make     TEXT,
    scanner_model    TEXT,
    scanned_at       TEXT,                          -- EXIF DateTimeOriginal, when present
    exif_json        TEXT,                          -- everything EXIF had, as JSON
    imported_at      TEXT NOT NULL DEFAULT (datetime('now')),
    status           TEXT NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'reading', 'read', 'failed')),
    error            TEXT
);

-- One row per page image: the unit people move between the Inbox, documents and Set aside.
CREATE TABLE IF NOT EXISTS pages (
    id                INTEGER PRIMARY KEY,
    scan_id           INTEGER NOT NULL REFERENCES scans(id),
    page_index        INTEGER NOT NULL DEFAULT 0,   -- position inside the scan file
    image_path        TEXT,                         -- page image extracted into the library
    width_px          INTEGER,
    height_px         INTEGER,
    dpi               INTEGER,
    color_mode        TEXT,                         -- rgb, gray, bilevel
    phash             TEXT,                         -- perceptual hash: near-duplicates, same paper
    paper_color       TEXT,                         -- mean background colour, #rrggbb
    blank_score       REAL,                         -- 0 = full page of writing, 1 = blank
    detected_rotation INTEGER NOT NULL DEFAULT 0 CHECK (detected_rotation IN (0, 90, 180, 270)),
    user_rotation     INTEGER NOT NULL DEFAULT 0 CHECK (user_rotation IN (0, 90, 180, 270)),
    script            TEXT CHECK (script IN ('handwritten', 'printed', 'typed', 'mixed', 'none')),
    language          TEXT,                         -- ISO 639 code, e.g. eng
    document_id       INTEGER REFERENCES documents(id),
    position          INTEGER,                      -- order within the document
    set_aside_at      TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (scan_id, page_index),
    CHECK (document_id IS NULL OR set_aside_at IS NULL),      -- one place only
    CHECK ((document_id IS NULL) = (position IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_pages_document ON pages(document_id, position);

-- Every reading of a page, kept forever. One is current; a person's correction becomes current.
CREATE TABLE IF NOT EXISTS transcriptions (
    id            INTEGER PRIMARY KEY,
    page_id       INTEGER NOT NULL REFERENCES pages(id),
    source        TEXT NOT NULL CHECK (source IN ('tesseract', 'vision', 'user')),
    engine_model  TEXT,                             -- e.g. tesseract 5.4 eng, llama3.2-vision
    text          TEXT NOT NULL,
    confidence    REAL,                             -- 0-100; NULL for a person's own text
    unsure_spans  TEXT,                             -- JSON [[start, end], ...] of doubtful words
    words         TEXT,                             -- JSON [{text, conf, bbox}], when available
    is_current    INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1)),
    confirmed_at  TEXT,                             -- a person checked this text is right
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_transcriptions_current
    ON transcriptions(page_id) WHERE is_current = 1;

-- ---------------------------------------------------------------- What intake finds

-- Anything found on a page or about a document. New extractors add new kinds, not columns.
CREATE TABLE IF NOT EXISTS facts (
    id            INTEGER PRIMARY KEY,
    page_id       INTEGER REFERENCES pages(id),
    document_id   INTEGER REFERENCES documents(id),
    kind          TEXT NOT NULL,                    -- see design/database.md for the list
    value         TEXT NOT NULL,                    -- as written: "March 4th 1892"
    norm_value    TEXT,                             -- normalised: "1892-03-04"
    confidence    REAL,                             -- 0-100
    source        TEXT NOT NULL
                  CHECK (source IN ('file', 'exif', 'image', 'ocr', 'vision', 'rule', 'user')),
    source_detail TEXT,                             -- extractor name and version
    bbox          TEXT,                             -- JSON [x, y, w, h] on the page image
    status        TEXT NOT NULL DEFAULT 'proposed'
                  CHECK (status IN ('proposed', 'accepted', 'rejected')),
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK ((page_id IS NULL) <> (document_id IS NULL))        -- belongs to exactly one
);
CREATE INDEX IF NOT EXISTS idx_facts_page ON facts(page_id, kind);
CREATE INDEX IF NOT EXISTS idx_facts_document ON facts(document_id, kind);
CREATE INDEX IF NOT EXISTS idx_facts_lookup ON facts(kind, norm_value);

CREATE TABLE IF NOT EXISTS embeddings (
    page_id     INTEGER NOT NULL REFERENCES pages(id),
    model       TEXT NOT NULL,
    dim         INTEGER NOT NULL,
    vector      BLOB NOT NULL,                      -- little-endian float32 x dim
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (page_id, model)
);

-- Evidence that two pages belong together. The matcher turns this into suggestions.
CREATE TABLE IF NOT EXISTS page_links (
    id          INTEGER PRIMARY KEY,
    page_a      INTEGER NOT NULL REFERENCES pages(id),
    page_b      INTEGER NOT NULL REFERENCES pages(id),
    relation    TEXT NOT NULL CHECK (relation IN (
                    'continues', 'same_writer', 'same_paper', 'same_letterhead',
                    'adjacent_file', 'duplicate', 'shared_names', 'similar_text')),
    score       REAL NOT NULL,                      -- 0-1
    evidence    TEXT,                               -- JSON, readable by a person
    source      TEXT NOT NULL,                      -- extractor name and version
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK (page_a < page_b),
    UNIQUE (page_a, page_b, relation, source)
);
CREATE INDEX IF NOT EXISTS idx_page_links_b ON page_links(page_b);

-- ---------------------------------------------------------------- Organising

CREATE TABLE IF NOT EXISTS folders (
    id          INTEGER PRIMARY KEY,
    parent_id   INTEGER REFERENCES folders(id),
    name        TEXT NOT NULL,
    position    INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS documents (
    id                  INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    name_source         TEXT NOT NULL DEFAULT 'lindley' CHECK (name_source IN ('lindley', 'user')),
    origin              TEXT NOT NULL DEFAULT 'lindley' CHECK (origin IN ('lindley', 'user')),
    status              TEXT NOT NULL DEFAULT 'progress' CHECK (status IN ('progress', 'complete')),
    folder_id           INTEGER REFERENCES folders(id),   -- NULL: In progress / Completed
    doc_type            TEXT,                       -- letter, deed, receipt, diary, ...
    doc_date            TEXT,                       -- partial ISO 8601: 1892, 1892-03, 1892-03-04
    date_source         TEXT CHECK (date_source IN ('lindley', 'user')),
    grouping_confidence REAL,                       -- 0-100: do these pages belong together
    summary             TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Everything Lindley proposes and a person decides: the UI's banners and "Add to" hints.
CREATE TABLE IF NOT EXISTS suggestions (
    id           INTEGER PRIMARY KEY,
    kind         TEXT NOT NULL CHECK (kind IN (
                     'add_to_document', 'group_pages', 'reorder', 'set_aside',
                     'name', 'date', 'type', 'complete')),
    page_id      INTEGER REFERENCES pages(id),
    document_id  INTEGER REFERENCES documents(id),
    payload      TEXT,                              -- JSON: what would change
    confidence   REAL,                              -- 0-100
    reasons      TEXT,                              -- JSON list of plain-language reasons
    status       TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'accepted', 'dismissed')),
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at  TEXT,
    CHECK (page_id IS NOT NULL OR document_id IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_suggestions_open ON suggestions(status, kind);

CREATE TABLE IF NOT EXISTS exports (
    id           INTEGER PRIMARY KEY,
    document_id  INTEGER NOT NULL REFERENCES documents(id),
    pdf_path     TEXT NOT NULL,
    page_ids     TEXT NOT NULL,                     -- JSON snapshot of the pages, in order
    exported_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ---------------------------------------------------------------- Bookkeeping

-- Each intake step for a scan (or one of its pages). Steps can be re-run when an extractor improves.
CREATE TABLE IF NOT EXISTS intake_steps (
    id              INTEGER PRIMARY KEY,
    scan_id         INTEGER NOT NULL REFERENCES scans(id),
    page_id         INTEGER REFERENCES pages(id),
    step            TEXT NOT NULL CHECK (step IN (
                        'hash', 'exif', 'split', 'image', 'ocr', 'vision', 'facts', 'embed', 'match')),
    status          TEXT NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),
    engine_version  TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS idx_intake_steps_status ON intake_steps(status, step);
CREATE INDEX IF NOT EXISTS idx_intake_steps_scan ON intake_steps(scan_id);

-- Who did what, for undo and so Lindley's actions are always visible.
CREATE TABLE IF NOT EXISTS history (
    id           INTEGER PRIMARY KEY,
    at           TEXT NOT NULL DEFAULT (datetime('now')),
    actor        TEXT NOT NULL CHECK (actor IN ('user', 'lindley')),
    action       TEXT NOT NULL,                     -- e.g. move_pages, rename, confirm_text
    target_type  TEXT NOT NULL,                     -- page, document, folder, ...
    target_id    INTEGER,
    before       TEXT,                              -- JSON
    after        TEXT                               -- JSON
);
CREATE INDEX IF NOT EXISTS idx_history_target ON history(target_type, target_id);

-- ---------------------------------------------------------------- Search

-- Full-text index over every reading. Searches join on is_current = 1 for today's text.
CREATE VIRTUAL TABLE IF NOT EXISTS transcriptions_fts USING fts5(
    text,
    content='transcriptions',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS transcriptions_ai AFTER INSERT ON transcriptions BEGIN
    INSERT INTO transcriptions_fts(rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER IF NOT EXISTS transcriptions_ad AFTER DELETE ON transcriptions BEGIN
    INSERT INTO transcriptions_fts(transcriptions_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;

CREATE TRIGGER IF NOT EXISTS transcriptions_au AFTER UPDATE OF text ON transcriptions BEGIN
    INSERT INTO transcriptions_fts(transcriptions_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO transcriptions_fts(rowid, text) VALUES (new.id, new.text);
END;

-- ---------------------------------------------------------------- Views
-- These are also the read-only surface Ask Lindley's AI queries. It never writes.

CREATE VIEW IF NOT EXISTS v_page_location AS
SELECT
    p.id AS page_id,
    CASE
        WHEN p.document_id IS NOT NULL THEN 'document'
        WHEN p.set_aside_at IS NOT NULL THEN 'aside'
        ELSE 'inbox'
    END AS location,
    p.document_id,
    p.position
FROM pages p;

CREATE VIEW IF NOT EXISTS v_current_text AS
SELECT
    t.page_id,
    t.id AS transcription_id,
    t.text,
    t.source,
    t.confidence,
    (t.source = 'user' OR t.confirmed_at IS NOT NULL) AS reviewed
FROM transcriptions t
WHERE t.is_current = 1;
