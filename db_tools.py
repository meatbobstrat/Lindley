#!/usr/bin/env python3
import argparse
import sqlite3
import os
import sys

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "watcher.db"))

def connect_db():
    return sqlite3.connect(DB_PATH)

# ---- Commands ----
def cmd_dump(args):
    conn = connect_db(); c = conn.cursor()
    for row in c.execute("SELECT path, status, word_count, ocr_confidence, lang FROM files;"):
        print(row)
    conn.close()

def cmd_errors(args):
    conn = connect_db(); c = conn.cursor()
    for row in c.execute("SELECT path, status FROM files WHERE status='error';"):
        print(row)
    conn.close()

def cmd_stats(args):
    conn = connect_db(); c = conn.cursor()
    total = c.execute("SELECT COUNT(*) FROM files;").fetchone()[0]
    processed = c.execute("SELECT COUNT(*) FROM files WHERE status='processed';").fetchone()[0]
    errors = c.execute("SELECT COUNT(*) FROM files WHERE status='error';").fetchone()[0]
    print(f"Total: {total}, Processed: {processed}, Errors: {errors}")
    conn.close()

# Future scaffolding
def cmd_export(args):
    print("TODO: implement export (CSV/JSON)")

def cmd_recent(args):
    print("TODO: implement recent files filter")

def cmd_shell(args):
    print("TODO: interactive SQL shell")

# ---- Command registry ----
COMMANDS = {
    "dump": cmd_dump,
    "errors": cmd_errors,
    "stats": cmd_stats,
    "export": cmd_export,
    "recent": cmd_recent,
    "shell": cmd_shell,
}

def main():
    parser = argparse.ArgumentParser(description="Lindley DB Tools")
    parser.add_argument("command", choices=COMMANDS.keys(), help="command to run")
    args = parser.parse_args()

    COMMANDS[args.command](args)

if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print(f"[ERROR] DB not found at {DB_PATH}")
        sys.exit(1)
    main()
