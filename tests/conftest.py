"""Pytest configuration and shared fixtures for Lindley tests."""

import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def temp_db(temp_dir):
    """Create a temporary SQLite database for testing."""
    db_path = temp_dir / "test.db"

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Create the files table schema
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        path TEXT NOT NULL,
        location TEXT DEFAULT 'inbox',
        status TEXT DEFAULT 'queued',
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
    """
    )
    conn.commit()
    conn.close()

    yield str(db_path)


@pytest.fixture
def inbox_dir(temp_dir):
    """Create a temporary inbox directory structure."""
    inbox = temp_dir / "inbox"
    inbox.mkdir()
    (inbox / "singles").mkdir()
    return inbox


@pytest.fixture
def test_image_file(temp_dir):
    """Create a minimal test image file."""
    from PIL import Image

    img_path = temp_dir / "test_image.jpg"
    img = Image.new("RGB", (100, 100), color="red")
    img.save(str(img_path))
    return img_path


@pytest.fixture
def test_pdf_file(temp_dir):
    """Create a minimal test PDF file."""
    try:
        from reportlab.pdfgen import canvas
    except ImportError:
        pytest.skip("reportlab not installed")

    pdf_path = temp_dir / "test_document.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Test PDF Document")
    c.save()
    return pdf_path
