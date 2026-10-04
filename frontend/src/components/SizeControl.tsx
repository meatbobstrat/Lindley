import { useThumb } from '../lib/view'

/** The slider that makes page thumbnails bigger or smaller, remembered in this browser. */
export function SizeControl() {
  const [thumb, setThumb] = useThumb()
  return (
    <label className="desktop-only" data-tip="Make the page thumbnails bigger or smaller">
      Size{' '}
      <input type="range" min={110} max={260} step={10} value={thumb} onChange={(e) => setThumb(Number(e.target.value))} aria-label="Thumbnail size" />
    </label>
  )
}
