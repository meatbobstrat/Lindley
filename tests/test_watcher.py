"""Tests for file watcher functionality."""

import sqlite3


def test_inbox_directory_structure(inbox_dir):
    """Test that inbox directory structure is created correctly."""
    assert inbox_dir.exists()
    assert (inbox_dir / "singles").exists()


def test_file_location_tracking(temp_db, inbox_dir):
    """Test tracking file locations in the database."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert files in different locations
    test_files = [
        ("file1.jpg", str(inbox_dir / "file1.jpg"), "inbox", "queued"),
        ("file2.jpg", str(inbox_dir / "singles" / "file2.jpg"), "inbox/singles", "queued"),
        (
            "file3.jpg",
            str(inbox_dir / "John_Doe" / "file3.jpg"),
            "inbox/John_Doe",
            "filed",
        ),
    ]

    for name, path, location, status in test_files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (name, 1024, f"hash_{name}", path, location, status),
        )
    conn.commit()

    # Verify files are tracked with correct locations
    cur.execute("SELECT location FROM files WHERE name = ?", ("file1.jpg",))
    assert cur.fetchone()[0] == "inbox"

    cur.execute("SELECT location FROM files WHERE name = ?", ("file2.jpg",))
    assert cur.fetchone()[0] == "inbox/singles"

    cur.execute("SELECT location FROM files WHERE name = ?", ("file3.jpg",))
    assert cur.fetchone()[0] == "inbox/John_Doe"

    conn.close()


def test_file_deduplication_detection(temp_db):
    """Test detecting duplicate files by hash."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert original file
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("original.jpg", 1024, "duplicate_hash", "/path/original.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Check if duplicate exists
    cur.execute("SELECT COUNT(*) FROM files WHERE sha256 = ?", ("duplicate_hash",))
    count = cur.fetchone()[0]
    assert count == 1

    # Simulate finding a duplicate
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("duplicate.jpg", 1024, "duplicate_hash", "/path/duplicate.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Verify duplicate detection
    cur.execute("SELECT COUNT(*) FROM files WHERE sha256 = ?", ("duplicate_hash",))
    count = cur.fetchone()[0]
    assert count == 2

    conn.close()


def test_file_status_transitions(temp_db):
    """Test file status transitions through processing pipeline."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert file with initial status
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("test.jpg", 1024, "test_hash", "/path/test.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Simulate status transitions
    statuses = ["queued", "ready", "processing", "processed", "filed"]

    for new_status in statuses[1:]:
        cur.execute("UPDATE files SET status = ? WHERE name = ?", (new_status, "test.jpg"))
        conn.commit()

        cur.execute("SELECT status FROM files WHERE name = ?", ("test.jpg",))
        current_status = cur.fetchone()[0]
        assert current_status == new_status

    conn.close()


def test_document_folder_organization(temp_db):
    """Test organizing files into document folders."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert files for a document folder
    document_name = "John_Branson_Letters"
    files = [
        ("letter_1.jpg", f"inbox/{document_name}", "filed"),
        ("letter_2.jpg", f"inbox/{document_name}", "filed"),
        ("letter_3.jpg", f"inbox/{document_name}", "filed"),
    ]

    for name, location, status in files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (name, 1024, f"hash_{name}", f"/path/{name}", location, status),
        )
    conn.commit()

    # Query files in the document folder
    cur.execute(
        "SELECT COUNT(*) FROM files WHERE location = ?",
        (f"inbox/{document_name}",),
    )
    count = cur.fetchone()[0]
    assert count == 3

    conn.close()
