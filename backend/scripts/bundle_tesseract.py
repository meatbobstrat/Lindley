"""Copy what Tesseract needs for reading into the Windows app, from an installed Tesseract
(UB-Mannheim's build). The installer then brings it, where find_tesseract looks first.

    python scripts/bundle_tesseract.py "C:\\Program Files\\Tesseract-OCR" ^
        build\\lindley\\windows\\app\\src\\tesseract

What goes: tesseract.exe, the DLLs it loads from its own folder (each one's imports followed in
turn: the build's training tools need far more), English, the orientation check (osd) and the
configs Lindley reads with (tsv, hocr), and Tesseract's license. Needs pefile (pip install
pefile), which the installers' workflow installs.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pefile

DATA = ("eng.traineddata", "osd.traineddata", "configs", "tessconfigs")
NOTICES = ("LICENSE", "AUTHORS")


def needed(folder: Path, program: str) -> list[str]:
    """The program and the DLLs it loads from the folder, theirs included."""
    here = {p.name.lower(): p.name for p in folder.glob("*.dll")}
    found, todo = [], [program]
    while todo:
        name = todo.pop()
        if name in found:
            continue
        found.append(name)
        pe = pefile.PE(str(folder / name), fast_load=True)
        pe.parse_data_directories(
            directories=[pefile.DIRECTORY_ENTRY["IMAGE_DIRECTORY_ENTRY_IMPORT"]]
        )
        for entry in getattr(pe, "DIRECTORY_ENTRY_IMPORT", []):
            dll = entry.dll.decode().lower()
            if dll in here:  # the system's own (kernel32, msvcrt...) aren't copied
                todo.append(here[dll])
        pe.close()
    return found


def bundle(source: Path, into: Path) -> int:
    shutil.rmtree(into, ignore_errors=True)
    (into / "tessdata").mkdir(parents=True)
    files = needed(source, "tesseract.exe")
    for name in files:
        shutil.copy2(source / name, into / name)
    for name in DATA:
        src = source / "tessdata" / name
        if src.is_dir():
            shutil.copytree(src, into / "tessdata" / name)
        else:
            shutil.copy2(src, into / "tessdata" / name)
    for name in NOTICES:
        if (source / name).exists():
            shutil.copy2(source / name, into / name)
    return sum(f.stat().st_size for f in into.rglob("*") if f.is_file())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("source", type=Path, help="the installed Tesseract's folder")
    parser.add_argument("into", type=Path, help="the app's tesseract folder")
    args = parser.parse_args()
    size = bundle(args.source, args.into)
    print(f"Copied Tesseract into {args.into} ({size / 1e6:.0f} MB)")


if __name__ == "__main__":
    main()
