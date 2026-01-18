"""Tests for OCR worker functionality."""

import json
import sqlite3


def test_file_hash_calculation(temp_db, test_image_file):
    """Test that file hashes are calculated and stored."""
    import hashlib

    # Calculate hash of test file
    with open(test_image_file, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()

    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert file with hash
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        (
            test_image_file.name,
            test_image_file.stat().st_size,
            file_hash,
            str(test_image_file),
            "inbox",
            "queued",
        ),
    )
    conn.commit()

    # Verify hash is stored
    cur.execute("SELECT sha256 FROM files WHERE name = ?", (test_image_file.name,))
    stored_hash = cur.fetchone()[0]
    assert stored_hash == file_hash

    conn.close()


def test_ocr_metadata_storage(temp_db):
    """Test storing OCR and metadata in database."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert file with OCR data
    ocr_text = "This is sample OCR text from a document"
    metadata = {"author": "John Doe", "date": "1892-01-15"}

    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status,
                          ocr_text, word_count, lang, ocr_confidence, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            "document.jpg",
            1024,
            "test_hash",
            "/path/document.jpg",
            "inbox",
            "processed",
            ocr_text,
            len(ocr_text.split()),
            "en",
            0.92,
            json.dumps(metadata),
        ),
    )
    conn.commit()

    # Verify OCR data is stored
    cur.execute(
        "SELECT ocr_text, word_count, lang, ocr_confidence FROM files WHERE name = ?",
        ("document.jpg",),
    )
    row = cur.fetchone()
    assert row[0] == ocr_text
    assert row[1] == len(ocr_text.split())
    assert row[2] == "en"
    assert row[3] == 0.92

    # Verify metadata is stored
    cur.execute("SELECT metadata FROM files WHERE name = ?", ("document.jpg",))
    stored_metadata = json.loads(cur.fetchone()[0])
    assert stored_metadata == metadata

    conn.close()


def test_page_count_tracking(temp_db):
    """Test tracking page count for multi-page documents."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert files with different page counts
    test_files = [
        ("single_page.jpg", 1, "hash1"),
        ("multi_page.pdf", 5, "hash2"),
        ("large_document.pdf", 42, "hash3"),
    ]

    for name, page_count, file_hash in test_files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status, page_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (name, 1024, file_hash, f"/path/{name}", "inbox", "processed", page_count),
        )
    conn.commit()

    # Verify page counts are stored correctly
    for name, expected_pages, _ in test_files:
        cur.execute("SELECT page_count FROM files WHERE name = ?", (name,))
        actual_pages = cur.fetchone()[0]
        assert actual_pages == expected_pages

    conn.close()


def test_language_detection_storage(temp_db):
    """Test storing detected language for documents."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert files with different languages
    test_files = [
        ("english_doc.jpg", "en", "This is English text"),
        ("french_doc.jpg", "fr", "Ceci est un texte français"),
        ("spanish_doc.jpg", "es", "Este es un texto en español"),
    ]

    for name, lang, ocr_text in test_files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status, lang, ocr_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                name,
                1024,
                f"hash_{name}",
                f"/path/{name}",
                "inbox",
                "processed",
                lang,
                ocr_text,
            ),
        )
    conn.commit()

    # Verify languages are stored
    for name, expected_lang, _ in test_files:
        cur.execute("SELECT lang FROM files WHERE name = ?", (name,))
        actual_lang = cur.fetchone()[0]
        assert actual_lang == expected_lang

    conn.close()


def test_ocr_confidence_scoring(temp_db):
    """Test storing OCR confidence scores."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert files with different confidence scores
    test_files = [
        ("high_quality.jpg", 0.95),
        ("medium_quality.jpg", 0.75),
        ("low_quality.jpg", 0.45),
    ]

    for name, confidence in test_files:
        cur.execute(
            """
            INSERT INTO files (name, size, sha256, path, location, status, ocr_confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                name,
                1024,
                f"hash_{name}",
                f"/path/{name}",
                "inbox",
                "processed",
                confidence,
            ),
        )
    conn.commit()

    # Verify confidence scores are stored
    for name, expected_confidence in test_files:
        cur.execute("SELECT ocr_confidence FROM files WHERE name = ?", (name,))
        actual_confidence = cur.fetchone()[0]
        assert actual_confidence == expected_confidence

    conn.close()


def test_file_processing_status(temp_db):
    """Test tracking file processing status."""
    conn = sqlite3.connect(temp_db)
    cur = conn.cursor()

    # Insert file with processing status
    cur.execute(
        """
        INSERT INTO files (name, size, sha256, path, location, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """,
        ("test.jpg", 1024, "test_hash", "/path/test.jpg", "inbox", "queued"),
    )
    conn.commit()

    # Simulate processing workflow
    statuses = ["queued", "processing", "processed"]

    for new_status in statuses[1:]:
        cur.execute("UPDATE files SET status = ? WHERE name = ?", (new_status, "test.jpg"))
        conn.commit()

        cur.execute("SELECT status FROM files WHERE name = ?", ("test.jpg",))
        current_status = cur.fetchone()[0]
        assert current_status == new_status

    conn.close()
