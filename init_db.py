import os
import sqlite3

def init_db(db_path="./data/watcher.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Full schema in one shot — no versioning, no migrations
    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        path TEXT NOT NULL,                -- actual FS path
        location TEXT DEFAULT 'inbox',     -- inbox/{folder}, singles, completed/{folder}
        status TEXT DEFAULT 'queued',      -- queued | ready | filed | completed
        ocr_text TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        page_count INTEGER,
        file_size INTEGER,
        doc_created TEXT,
        doc_modified TEXT,
        word_count INTEGER,
        lang TEXT,
        ocr_confidence REAL,
        metadata TEXT
    )
    """)
    conn.commit()
    conn.close()

    print(f"[InitDB] Database initialized fresh at {db_path}")
    return "v-clean"

if __name__ == "__main__":
    init_db()
