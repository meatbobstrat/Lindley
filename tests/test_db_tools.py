"""Tests for database utilities and operations."""

import sqlite3


def test_database_creation(temp_db):
    """Test that database is created with correct schema."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Check that files table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
    assert cur.fetchone() is not None

    # Check that required columns exist
    cur.execute("PRAGMA table_info(files)")
    columns = {row[1] for row in cur.fetchall()}

    required_columns = {
        "id",
        "name",
        "size",
        "sha256",
        "path",
        "location",
        "status",
        "ocr_text",
        "created_at",
        "page_count",
        "file_size",
        "word_count",
        "lang",
        "ocr_confidence",
    }
    assert required_columns.issubset(columns)

    conn.close()


def test_insert_file_record(temp_db):
    """Test inserting a file record into the database."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert a test file record
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("test.jpg", 1024, "abc123def456", "/path/to/test.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Verify the record was inserted
    cur.execute("SELECT * FROM files WHERE name = ?", ("test.jpg",))
    row = cur.fetchone()
    assert row is not None
    assert row[1] == "test.jpg"  # name
    assert row[3] == "abc123def456"  # sha256
    assert row[6] == "queued"  # status

    conn.close()


def test_update_file_status(temp_db):
    """Test updating file status in the database."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert a test file
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("test.jpg", 1024, "abc123", "/path/to/test.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Update the status
    cur.execute("UPDATE files SET status = ? WHERE name = ?", ("processed", "test.jpg"))
    conn.commit()

    # Verify the update
    cur.execute("SELECT status FROM files WHERE name = ?", ("test.jpg",))
    status = cur.fetchone()[0]
    assert status == "processed"

    conn.close()


def test_query_files_by_status(temp_db):
    """Test querying files by status."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert multiple test files with different statuses
    test_files = [
        ("file1.jpg", 1024, "hash1", "/path/file1.jpg", "inbox", "queued"),
        ("file2.jpg", 2048, "hash2", "/path/file2.jpg", "inbox", "queued"),
        ("file3.jpg", 4096, "hash3", "/path/file3.jpg", "inbox", "processed"),
    ]

    for file_data in test_files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            file_data,
        )
    conn.commit()

    # Query for queued files
    cur.execute("SELECT COUNT(*) FROM files WHERE status = ?", ("queued",))
    queued_count = cur.fetchone()[0]
    assert queued_count == 2

    # Query for processed files
    cur.execute("SELECT COUNT(*) FROM files WHERE status = ?", ("processed",))
    processed_count = cur.fetchone()[0]
    assert processed_count == 1

    conn.close()


def test_file_deduplication_by_hash(temp_db):
    """Test checking for duplicate files by SHA256 hash."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert a file
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("original.jpg", 1024, "same_hash", "/path/original.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Check if a file with the same hash exists
    cur.execute("SELECT COUNT(*) FROM files WHERE sha256 = ?", ("same_hash",))
    count = cur.fetchone()[0]
    assert count == 1

    # Try to insert a duplicate (same hash)
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("duplicate.jpg", 1024, "same_hash", "/path/duplicate.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Verify both records exist (deduplication logic would be in application)
    cur.execute("SELECT COUNT(*) FROM files WHERE sha256 = ?", ("same_hash",))
    count = cur.fetchone()[0]
    assert count == 2

    conn.close()
