# Lindley

Lindley is your research partner for taming thousands of scanned pages — letters, deeds, books, manuscripts, and more.  
It automatically turns raw images into sorted, searchable PDFs, and gives you an AI you can talk to about your documents.

## Why Lindley?
- Organize massive archives without manual sorting  
- Search across handwritten or printed text instantly  
- Ask questions like *“show me every letter mentioning John Branson in 1892”*  
- Build connections between scattered documents into a story  

## Current Status
**Watcher MVP**: watches folders, deduplicates files, moves/copies them into a processing queue, and tracks everything in SQLite + Redis.

## Config
Edit `settings.json` to set:
- `watch_folders`: list of folders to monitor  
- `processing_dir`, `quarantine_dir`, `db_path`  
- `move_files`: `true` to move, `false` to copy  

## Roadmap
- Watcher (✔)  
- Worker (OCR → searchable PDFs)  
- AI (chat with your docs)  
- One-click installer (Windows/Mac)
