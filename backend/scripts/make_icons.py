"""Draw Lindley's icon at each size the installers and the tray need: the mark the app shows
(frontend/src/ui/icons.tsx, MarkDefs, and frontend/public/favicon.svg), a speech bubble with an
"L" in it.

    python scripts/make_icons.py

Writes icons/lindley-<size>.png (Ubuntu), icons/lindley.ico (Windows), icons/lindley.icns (the
Mac), and src/lindley/icon.png, the tray's. Run it again if the mark changes.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

BACKEND = Path(__file__).resolve().parents[1]
BUBBLE, INK = "#8a5a00", "#f4ecdf"  # the app's --ai colour; the L in the page's cream
BIG = 1024  # drawn once at this size, then made smaller
SIZES = (16, 32, 64, 128, 256, 512)
PAD = 1  # the mark's viewBox is 24 across; a little room round it


def draw() -> Image.Image:
    k = BIG / (24 + 2 * PAD)

    def at(x: float, y: float) -> tuple[float, float]:
        return ((x + PAD) * k, (y + PAD) * k)

    def dot(x: float, y: float, r: float, fill: str) -> None:
        (cx, cy) = at(x, y)
        d.ellipse((cx - r * k, cy - r * k, cx + r * k, cy + r * k), fill=fill)

    im = Image.new("RGBA", (BIG, BIG), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    # The bubble: a circle round (12, 12), and its tail down to the left with a rounded tip
    dot(12, 12, 10.2, BUBBLE)
    d.polygon([at(7.4, 21.1), at(2.6, 22.4), at(1.9, 21.6), at(3.4, 17.3), at(12, 12)], fill=BUBBLE)
    dot(2.46, 21.82, 0.6, BUBBLE)
    # The L: a stroke 2.6 wide, round at its ends and corner
    for a, b in (((9.4, 6.6), (9.4, 16.2)), ((9.4, 16.2), (15.6, 16.2))):
        d.line((*at(*a), *at(*b)), fill=INK, width=round(2.6 * k))
    for x, y in ((9.4, 6.6), (9.4, 16.2), (15.6, 16.2)):
        dot(x, y, 1.3, INK)
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
