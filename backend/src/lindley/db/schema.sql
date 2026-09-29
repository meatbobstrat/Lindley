-- Lindley schema. Applied idempotently at startup by lindley.db.database.init_db.

CREATE TABLE IF NOT EXISTS documents (
    id            INTEGER PRIMARY KEY,
    title         TEXT,
    doc_type      TEXT,              -- letter, deed, book, manuscript, ...
    doc_date      TEXT,              -- best-known date of the document itself (ISO 8601, may be partial)
    pdf_path      TEXT,              -- searchable PDF in the library
    source_path   TEXT,              -- original scan location
    sha256        TEXT UNIQUE,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pages (
    id              INTEGER PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    page_number     INTEGER NOT NULL,
    text            TEXT,
    ocr_engine      TEXT,            -- tesseract | vision
    ocr_confidence  REAL,
    UNIQUE (document_id, page_number)
);

CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY,
    source_path   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'queued',  -- queued | ocr | pdf | indexed | quarantined
    error         TEXT,
    document_id   INTEGER REFERENCES documents(id) ON DELETE SET NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- Full-text index over page text, kept in sync with `pages` by triggers.
CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
    text,
    content='pages',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
    INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
END;

CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.id, old.text);
END;

CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON pages BEGIN
    INSERT INTO pages_fts(pages_fts, rowid, text) VALUES ('delete', old.id, old.text);
    INSERT INTO pages_fts(rowid, text) VALUES (new.id, new.text);
END;
