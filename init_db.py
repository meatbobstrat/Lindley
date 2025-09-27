import os
import sqlite3

DB_PATH = "/db/watcher.db"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS file_hashes (
    hash TEXT PRIMARY KEY,
    filename TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
conn.close()

print(f"[InitDB] Database initialized at {DB_PATH}")
