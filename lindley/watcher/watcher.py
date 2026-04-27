import os
import sqlite3
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS

from lindley.watcher.config import load_settings, save_settings
from lindley.watcher.observer import WatcherObserver

app = Flask(__name__)
CORS(app)  # Allow Electron frontend to connect

# Config
DB_PATH = os.path.abspath("./data/watcher.db")
INBOX_DIR = os.path.abspath("./data/inbox")


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dicts
    return conn


def get_documents_from_location():
    """Extract unique document folders from file locations."""
    conn = get_db()
    cur = conn.cursor()

    # Get all unique folders from location field
    cur.execute("""
        SELECT DISTINCT 
            CASE 
                WHEN location LIKE 'inbox/%' AND location != 'inbox/singles' 
                    THEN SUBSTR(location, 7)  -- Strip 'inbox/' prefix
                WHEN location LIKE 'completed/%' 
                    THEN SUBSTR(location, 11)  -- Strip 'completed/' prefix
                ELSE NULL
            END as folder_name,
            CASE 
                WHEN location LIKE 'completed/%' THEN 'completed'
                ELSE 'active'
            END as status
        FROM files 
        WHERE location != 'inbox' 
            AND location != 'inbox/singles'
            AND location IS NOT NULL
    """)

    docs = {}
    for row in cur.fetchall():
        if row["folder_name"]:
            folder = row["folder_name"]
            if folder not in docs:
                docs[folder] = {"name": folder, "status": row["status"]}

    conn.close()
    return list(docs.values())


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Get overall statistics."""
    conn = get_db()
    cur = conn.cursor()

    # Count files by location/status
    cur.execute(
        "SELECT COUNT(*) as count FROM files WHERE location = 'inbox' AND status = 'queued'"
    )
    unprocessed = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) as count FROM files WHERE status = 'ready'")
    processing = cur.fetchone()["count"]

    cur.execute(
        "SELECT COUNT(*) as count FROM files WHERE location LIKE 'inbox/%' AND location != 'inbox/singles'"
    )
    in_documents = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) as count FROM files WHERE location = 'inbox/singles'")
    singles = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) as count FROM files WHERE location LIKE 'completed/%'")
    completed = cur.fetchone()["count"]

    # Count unique document folders
    documents = get_documents_from_location()
    doc_count = len([d for d in documents if d["status"] == "active"])
    completed_doc_count = len([d for d in documents if d["status"] == "completed"])

    conn.close()

    return jsonify(
        {
            "unprocessed": unprocessed,
            "processing": processing,
            "singles": singles,
            "documents": doc_count,
            "completed_documents": completed_doc_count,
            "total_files": unprocessed + processing + in_documents + singles + completed,
        }
    )


@app.route("/api/inbox", methods=["GET"])
def get_inbox():
    """Get files in inbox root (unprocessed)."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, size, sha256, path, location, status, 
               created_at, page_count, file_size, word_count, 
               lang, ocr_confidence
        FROM files 
        WHERE location = 'inbox'
        ORDER BY created_at DESC
    """)

    files = [dict(row) for row in cur.fetchall()]
    conn.close()

    return jsonify({"files": files})


@app.route("/api/documents", methods=["GET"])
def get_documents():
    """Get list of document folders with file counts and metadata."""
    conn = get_db()
    cur = conn.cursor()

    documents = get_documents_from_location()

    # Enrich each document with file counts and metadata
    for doc in documents:
        if doc["status"] == "completed":
            location_pattern = f"completed/{doc['name']}"
        else:
            location_pattern = f"inbox/{doc['name']}"

        cur.execute(
            """
            SELECT COUNT(*) as count,
                   AVG(ocr_confidence) as avg_confidence,
                   SUM(page_count) as total_pages
            FROM files 
            WHERE location = ?
        """,
            (location_pattern,),
        )

        stats = cur.fetchone()
        doc["file_count"] = stats["count"]
        doc["avg_confidence"] = round(stats["avg_confidence"], 1) if stats["avg_confidence"] else 0
        doc["total_pages"] = stats["total_pages"] or 0

    conn.close()

    return jsonify({"documents": documents})


@app.route("/api/documents/<path:folder_name>/files", methods=["GET"])
def get_document_files(folder_name):
    """Get all files in a specific document folder."""
    # Determine if it's completed or active
    conn = get_db()
    cur = conn.cursor()

    # Try both inbox and completed locations
    cur.execute(
        """
        SELECT id, name, size, sha256, path, location, status,
               created_at, page_count, file_size, word_count,
               lang, ocr_confidence, ocr_text, metadata
        FROM files
        WHERE location IN (?, ?)
        ORDER BY name ASC
    """,
        (f"inbox/{folder_name}", f"completed/{folder_name}"),
    )

    files = [dict(row) for row in cur.fetchall()]
    conn.close()

    if not files:
        return jsonify({"error": "Document not found"}), 404

    return jsonify({"folder_name": folder_name, "files": files})


@app.route("/api/singles", methods=["GET"])
def get_singles():
    """Get files in the singles folder (unmatched)."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, size, sha256, path, location, status,
               created_at, page_count, file_size, word_count,
               lang, ocr_confidence
        FROM files
        WHERE location = 'inbox/singles'
        ORDER BY created_at DESC
    """)

    files = [dict(row) for row in cur.fetchall()]
    conn.close()

    return jsonify({"files": files})


@app.route("/api/file/<int:file_id>", methods=["GET"])
def get_file(file_id):
    """Get detailed information about a specific file."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM files
        WHERE id = ?
    """,
        (file_id,),
    )

    file = cur.fetchone()
    conn.close()

    if not file:
        return jsonify({"error": "File not found"}), 404

    return jsonify({"file": dict(file)})


@app.route("/api/file/<int:file_id>/text", methods=["GET"])
def get_file_text(file_id):
    """Get OCR text for a specific file."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, name, ocr_text FROM files WHERE id = ?", (file_id,))
    file = cur.fetchone()
    conn.close()

    if not file:
        return jsonify({"error": "File not found"}), 404

    return jsonify({"id": file["id"], "name": file["name"], "text": file["ocr_text"]})


@app.route("/api/search", methods=["GET"])
def search():
    """Search files by text content or metadata."""
    query = request.args.get("q", "").strip()

    if not query:
        return jsonify({"error": "Query parameter required"}), 400

    conn = get_db()
    cur = conn.cursor()

    # Simple text search in OCR content and filename
    cur.execute(
        """
        SELECT id, name, path, location, status, page_count,
               ocr_confidence, created_at,
               snippet(files, 'ocr_text', '', '', '...', 10) as snippet
        FROM files
        WHERE ocr_text LIKE ? OR name LIKE ?
        ORDER BY ocr_confidence DESC
        LIMIT 50
    """,
        (f"%{query}%", f"%{query}%"),
    )

    results = [dict(row) for row in cur.fetchall()]
    conn.close()

    return jsonify({"query": query, "results": results, "count": len(results)})


@app.route("/api/quarantine", methods=["GET"])
def get_quarantine():
    """Get list of files in quarantine folders."""
    import glob

    quarantine_dirs = [
        os.path.abspath("./data/quarantine"),
        os.path.abspath("./data/ocr_quarantine"),
    ]

    quarantine_files = []

    for qdir in quarantine_dirs:
        if os.path.exists(qdir):
            reason = (
                "duplicate"
                if "quarantine" in os.path.basename(qdir) and "ocr" not in qdir
                else "processing_error"
            )
            for fpath in glob.glob(os.path.join(qdir, "*")):
                if os.path.isfile(fpath):
                    quarantine_files.append(
                        {
                            "name": os.path.basename(fpath),
                            "path": fpath,
                            "size": os.path.getsize(fpath),
                            "reason": reason,
                            "modified": datetime.fromtimestamp(os.path.getmtime(fpath)).isoformat(),
                        }
                    )

    return jsonify({"files": quarantine_files, "count": len(quarantine_files)})


@app.route("/api/settings", methods=["GET"])
def get_settings():
    """Get current settings including watch folders."""
    settings = load_settings()
    return jsonify(settings)


@app.route("/api/settings/watch-folders", methods=["POST"])
def add_watch_folder():
    """Add a new watch folder."""
    data = request.get_json()
    path = data.get("path", "").strip()
    move_files = data.get("move_files", True)
    name = data.get("name", os.path.basename(path))

    if not path or not os.path.isdir(path):
        return jsonify({"error": "Invalid folder path"}), 400

    settings = load_settings()

    # Check for duplicates
    for folder in settings["watch_folders"]:
        if os.path.abspath(folder["path"]) == os.path.abspath(path):
            return jsonify({"error": "Folder already being watched"}), 409

    new_folder = {
        "id": f"folder-{len(settings['watch_folders'])}",
        "path": path,
        "move_files": move_files,
        "enabled": True,
        "name": name,
    }

    settings["watch_folders"].append(new_folder)
    save_settings(settings)

    # Add to live observer if it exists
    if hasattr(app, "watcher_observer"):
        app.watcher_observer.add_watch(new_folder)

    return jsonify(new_folder), 201


@app.route("/api/settings/watch-folders/<folder_id>", methods=["PUT"])
def update_watch_folder(folder_id):
    """Update a watch folder's settings."""
    data = request.get_json()
    settings = load_settings()

    folder = None
    for f in settings["watch_folders"]:
        if f["id"] == folder_id:
            folder = f
            break

    if not folder:
        return jsonify({"error": "Folder not found"}), 404

    if "move_files" in data:
        folder["move_files"] = data["move_files"]
    if "enabled" in data:
        folder["enabled"] = data["enabled"]
    if "name" in data:
        folder["name"] = data["name"]

    save_settings(settings)
    return jsonify(folder), 200


@app.route("/api/settings/watch-folders/<folder_id>", methods=["DELETE"])
def delete_watch_folder(folder_id):
    """Remove a watch folder."""
    settings = load_settings()
    settings["watch_folders"] = [f for f in settings["watch_folders"] if f["id"] != folder_id]
    save_settings(settings)
    return jsonify({"status": "deleted"}), 200


def main():
    """Main entry point for the watcher."""
    print("[Watcher] Starting Lindley file watcher...")

    settings = load_settings()
    print(f"[Watcher] Loaded settings: {len(settings['watch_folders'])} folders configured")

    # Initialize database
    from init_db import init_db
    init_db(settings["db_path"])

    # Start observer
    watcher_observer = WatcherObserver(
        db_path=settings["db_path"],
        inbox_dir=os.path.join(os.path.dirname(settings["db_path"]), "inbox"),
        quarantine_dir=settings["quarantine_dir"],
    )

    # Add configured folders and scan for existing files
    for folder in settings["watch_folders"]:
        if folder.get("enabled", True):
            watcher_observer.add_watch(folder)
            watcher_observer.scan_existing_files(folder)

    watcher_observer.start()
    app.watcher_observer = watcher_observer  # Store for API access

    # Start Flask API
    print("[Watcher] Starting Flask API on http://127.0.0.1:5000")
    try:
        app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
    finally:
        watcher_observer.stop()


if __name__ == "__main__":
    main()

