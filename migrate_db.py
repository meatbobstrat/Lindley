import sqlite3

DB_PATH = "./data/watcher.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

try:
    cur.execute("ALTER TABLE files ADD COLUMN ocr_text TEXT;")
    print("Added ocr_text column.")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("ocr_text already exists, nothing to do.")
    else:
        raise

conn.commit()
conn.close()
