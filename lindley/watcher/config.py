import json
import os
from typing import Any, Dict


SETTINGS_PATH = "./settings.json"
DEFAULTS = {
    "watch_folders": [],
    "processing_dir": "./data/tmp",
    "quarantine_dir": "./data/quarantine",
    "db_path": "./data/watcher.db",
}


def _ensure_absolute(path: str) -> str:
    """Convert relative paths to absolute, leave network paths alone."""
    if path.startswith(("\\\\", "//")):  # network path
        return path
    return os.path.abspath(path)


def load_settings() -> Dict[str, Any]:
    """Load and normalize settings from settings.json."""
    if not os.path.exists(SETTINGS_PATH):
        return DEFAULTS.copy()

    try:
        with open(SETTINGS_PATH, "r") as f:
            settings = json.load(f)
    except (json.JSONDecodeError, IOError):
        print(f"[Config] Failed to load {SETTINGS_PATH}, using defaults")
        return DEFAULTS.copy()

    # Normalize: ensure absolute paths, ensure required keys
    settings = {**DEFAULTS, **settings}
    settings["processing_dir"] = _ensure_absolute(settings["processing_dir"])
    settings["quarantine_dir"] = _ensure_absolute(settings["quarantine_dir"])
    settings["db_path"] = _ensure_absolute(settings["db_path"])

    # Normalize watch folders
    normalized_folders = []
    for i, folder in enumerate(settings.get("watch_folders", [])):
        if isinstance(folder, str):
            # Backward compat: old flat string list -> convert to object
            folder = {"path": folder, "move_files": True, "enabled": True}

        folder = {
            "id": folder.get("id", f"folder-{i}"),
            "path": _ensure_absolute(folder.get("path", "")),
            "move_files": folder.get("move_files", True),
            "enabled": folder.get("enabled", True),
            "name": folder.get("name", os.path.basename(folder.get("path", ""))),
        }
        if folder["path"]:
            normalized_folders.append(folder)

    settings["watch_folders"] = normalized_folders
    return settings


def save_settings(settings: Dict[str, Any]) -> None:
    """Persist settings to settings.json."""
    os.makedirs(os.path.dirname(SETTINGS_PATH) or ".", exist_ok=True)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)
    print(f"[Config] Settings saved to {SETTINGS_PATH}")
