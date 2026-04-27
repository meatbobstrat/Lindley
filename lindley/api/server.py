"""
Database utility tools for testing and development.
Usage: python db_tools.py generate_test_data
"""

import random
import sqlite3
from datetime import datetime, timedelta

DB_PATH = "./data/watcher.db"


def generate_test_data():
    """Generate realistic test data for development."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Clear existing data
    cur.execute("DELETE FROM files")

    print("[DB Tools] Generating test data...")

    # Helper to insert file
    def add_file(name, location, status, pages=1, words=100, confidence=85, lang="en"):
        sha256 = f"test_hash_{random.randint(1000, 9999)}"
        path = f"./data/inbox/{location}/{name}" if location != "inbox" else f"./data/inbox/{name}"
        size = random.randint(50000, 500000)
        created = (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()

        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status,
                             page_count, file_size, word_count, lang,
                             ocr_confidence, created_at, ocr_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                name,
                size,
                sha256,
                path,
                location,
                status,
                pages,
                size,
                words,
                lang,
                confidence,
                created,
                f"Sample OCR text for {name}",
            ),
        )

    # Unprocessed files in inbox
    for i in range(8):
        add_file(f"scan_{i + 1:03d}.jpg", "inbox", "queued", words=0, confidence=0)

    # Processing files
    for i in range(3):
        add_file(f"processing_{i + 1:03d}.jpg", "inbox", "ready", words=0, confidence=0)

    # Document: John Branson Letters
    letters = [
        ("letter_1892_01_15.jpg", 1, 234, 92),
        ("letter_1892_03_22.jpg", 1, 198, 89),
        ("letter_1892_06_08.jpg", 2, 445, 94),
        ("letter_1892_09_30.jpg", 1, 321, 87),
        ("letter_1893_01_12.jpg", 1, 267, 91),
    ]
    for name, pages, words, conf in letters:
        add_file(name, "inbox/John_Branson_Letters", "filed", pages, words, conf)

    # Document: Property Deeds 1890s
    deeds = [
        ("deed_1891_property.jpg", 3, 567, 78),
        ("deed_1893_transfer.jpg", 2, 423, 82),
        ("deed_1895_sale.jpg", 4, 789, 76),
    ]
    for name, pages, words, conf in deeds:
        add_file(name, "inbox/Property_Deeds_1890s", "filed", pages, words, conf)

    # Document: Family Photos
    photos = [
        ("photo_1880s_family.jpg", 1, 0, 45),
        ("photo_1890_wedding.jpg", 1, 12, 67),
        ("photo_1895_children.jpg", 1, 8, 55),
    ]
    for name, pages, words, conf in photos:
        add_file(name, "inbox/Family_Photos", "filed", pages, words, conf)

    # Singles (unmatched)
    singles = [
        ("receipt_store.jpg", 1, 45, 72),
        ("map_fragment.jpg", 1, 23, 58),
        ("postcard_1891.jpg", 1, 67, 88),
    ]
    for name, pages, words, conf in singles:
        add_file(name, "inbox/singles", "filed", pages, words, conf)

    # Completed documents
    completed_docs = [
        (
            "Miller_Family_Bible",
            [
                ("bible_page_001.jpg", 1, 234, 95),
                ("bible_page_002.jpg", 1, 198, 93),
                ("bible_page_003.jpg", 1, 267, 94),
            ],
        ),
        (
            "WWI_Correspondence",
            [
                ("wwi_letter_1917_01.jpg", 2, 445, 89),
                ("wwi_letter_1917_05.jpg", 2, 387, 91),
            ],
        ),
    ]

    for folder, files in completed_docs:
        for name, pages, words, conf in files:
            add_file(name, f"completed/{folder}", "completed", pages, words, conf)

    conn.commit()

    # Print stats
    cur.execute("SELECT COUNT(*) as count FROM files")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT location) as count FROM files WHERE location LIKE 'inbox/%'")
    doc_count = cur.fetchone()[0]

    print(f"[DB Tools] Generated {total} test files")
    print(f"[DB Tools] Created {doc_count} document folders")

    conn.close()


def clear_database():
    """Clear all data from database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM files")
    conn.commit()
    conn.close()
    print("[DB Tools] Database cleared")


def show_stats():
    """Show database statistics."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) as count FROM files")
    total = cur.fetchone()[0]

    cur.execute("SELECT status, COUNT(*) as count FROM files GROUP BY status")
    status_counts = cur.fetchall()

    cur.execute("SELECT location, COUNT(*) as count FROM files GROUP BY location")
    location_counts = cur.fetchall()

    print(f"\n[DB Stats] Total files: {total}")
    print("\nBy Status:")
    for row in status_counts:
        print(f"  {row[0]}: {row[1]}")
    print("\nBy Location:")
    for row in location_counts:
        print(f"  {row[0]}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python db_tools.py [generate_test_data|clear|stats]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "generate_test_data":
        generate_test_data()
    elif command == "clear":
        clear_database()
    elif command == "stats":
        show_stats()
    else:
        print(f"Unknown command: {command}")
        print("Available: generate_test_data, clear, stats")
