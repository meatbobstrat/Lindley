// Zoom steps for a full scan (components/ScanImage.tsx). 1 is Fit: the whole page in view.

export const ZOOM_MAX = 5
const STEPS = [1, 1.5, 2, 3, 4, 5]

/** The zoom step after z, in (1) or out (-1): what the zoom buttons do. */
export function stepZoom(z: number, dir: 1 | -1): number {
  return dir > 0 ? (STEPS.find((s) => s > z + 0.01) ?? ZOOM_MAX) : ([...STEPS].reverse().find((s) => s < z - 0.01) ?? 1)
}
