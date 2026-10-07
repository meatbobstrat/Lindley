"""Draw Lindley's icon at each size the installers and the tray need, from the shape of the
app's favicon (frontend/public/favicon.svg): a brown page with three lines of writing.

    python scripts/make_icons.py

Writes icons/lindley-<size>.png (Ubuntu), icons/lindley.ico (Windows), icons/lindley.icns (the
Mac), and src/lindley/icon.png, the tray's. Run it again if the favicon changes.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

BACKEND = Path(__file__).resolve().parents[1]
PAGE, INK = "#7a5c3e", "#f4ecdf"
BIG = 1024  # drawn once at this size, then made smaller
SIZES = (16, 32, 64, 128, 256, 512)


def draw() -> Image.Image:
    k = BIG / 32  # the favicon's viewBox is 32 across
    im = Image.new("RGBA", (BIG, BIG), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    d.rounded_rectangle((6 * k, 3 * k, 26 * k, 29 * k), radius=2 * k, fill=PAGE)
    for y, x2 in ((10, 22), (15, 22), (20, 18)):  # stroke 2, round ends
        d.line((10 * k, y * k, x2 * k, y * k), fill=INK, width=round(2 * k))
        for x in (10, x2):
            d.ellipse(((x - 1) * k, (y - 1) * k, (x + 1) * k, (y + 1) * k), fill=INK)
    return im


def main() -> None:
    big = draw()
    out = BACKEND / "icons"
    out.mkdir(exist_ok=True)
    for size in SIZES:
        big.resize((size, size), Image.Resampling.LANCZOS).save(out / f"lindley-{size}.png")
    big.save(out / "lindley.ico", sizes=[(s, s) for s in (16, 24, 32, 48, 64, 128, 256)])
    big.save(out / "lindley.icns")
    big.resize((64, 64), Image.Resampling.LANCZOS).save(BACKEND / "src" / "lindley" / "icon.png")
    print(f"Wrote {out} and src/lindley/icon.png")


if __name__ == "__main__":
    main()
