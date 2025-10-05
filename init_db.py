import os
import sqlite3

def init_db(db_path="./data/watcher.db"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

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
        ai_suggestion TEXT,                -- folder/tag AI suggested
        tags TEXT,                         -- JSON array string (quick tags)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Helpful indexes for speed on common queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sha256 ON files(sha256)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_status ON files(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_location ON files(location)")

    conn.commit()
    conn.close()

    print(f"[InitDB] Database initialized at {db_path}")

if __name__ == "__main__":
    init_db()
