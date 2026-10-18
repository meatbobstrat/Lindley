// A full scan, to read or to check text against. It starts fitted to its pane. It zooms with its
// buttons, the + − 0 keys, Ctrl+wheel (a trackpad pinch is one), a double click or double tap,
// and a pinch on a touch screen. Zoomed in, it pans by scrolling, by dragging with the mouse, or
// with a finger. The pane's scroll position is the pan, so scrollbars, the wheel and the arrow
// keys work as they do everywhere else, and panes can scroll together (Duplicates).
//
// Touch: the pane is `touch-action: pan-x pan-y`, so one finger pans natively (with momentum) and
// the browser never zooms the whole page. The pinch is Touch Events, not Pointer Events: once the
// browser starts a one-finger pan it cancels that pointer, and a pinch would lose its first finger.

import { type CSSProperties, useEffect, useLayoutEffect, useRef, useState } from 'react'
import { imageAt } from '../api/client'
import { stepZoom, ZOOM_MAX } from '../lib/zoom'
import { Icon } from '../ui/icons'

const SHARP = 4000 // the most the server sends: asked for once the zoomed scan needs more than `size`
const TAP_MS = 300
const TAP_PX = 10

const clamp = (z: number) => Math.min(ZOOM_MAX, Math.max(1, z))

interface Props {
  /** The page's image address, as the API gives it */
  src: string
  alt: string
  /** The size to ask for first, in pixels on the longest side */
  size?: number
  /** Zoom (1 is Fit), when the parent keeps it: Duplicates zooms every copy at once */
  zoom?: number
  setZoom?: (z: number) => void
  /** Its own zoom buttons, in the corner of the scan */
  bar?: boolean
  className?: string
}

interface Size {
  w: number
  h: number
}
/** A point of the scan (as fractions of it) to keep under a point of the pane, while zooming */
interface Anchor {
  fx: number
  fy: number
  px: number
  py: number
}
interface Press {
  x: number
  y: number
  left: number
  top: number
  at: number
  moved: boolean
  drag: boolean
}

export function ScanImage({ src, alt, size = 2000, zoom, setZoom, bar = true, className }: Props) {
  const [own, setOwn] = useState(1)
  const [sharp, setSharp] = useState(false)
  const [shown, setShown] = useState(src)
  if (shown !== src) {
    // Another page: back to Fit, unless the parent keeps the zoom
    setShown(src)
    setOwn(1)
    setSharp(false)
  }
  const z = zoom ?? own
  const set = setZoom ?? setOwn

  const pane = useRef<HTMLDivElement>(null)
  const img = useRef<HTMLImageElement>(null)
  const [nat, setNat] = useState<Size | null>(null)
  const [box, setBox] = useState<Size | null>(null)
  const [dragging, setDragging] = useState(false)
  const anchor = useRef<Anchor | null>(null)
  const zNow = useRef(z) // ahead of a render, so wheel and pinch events in one frame add up
  const zWas = useRef(z)
  const press = useRef<Press | null>(null)
  const fingers = useRef(new Set<number>()) // touch pointers down: two or more is a pinch, not a tap
  const lastTap = useRef<{ x: number; y: number; at: number } | null>(null)

  const fit = nat && box && box.w > 0 && box.h > 0 ? Math.min(box.w / nat.w, box.h / nat.h) : null
  const style: CSSProperties = fit && nat ? { width: nat.w * fit * z, height: nat.h * fit * z } : { maxWidth: '100%', maxHeight: '100%' }

  const zoomTo = (next: number, at?: { x: number; y: number }) => {
    next = clamp(next)
    zNow.current = next
    // Zoomed past what was sent: ask for the sharpest the server has
    const side = nat ? Math.max(nat.w, nat.h) : 0
    if (!sharp && fit && side >= size && side * fit * next * devicePixelRatio > size * 1.1) setSharp(true)
    const p = pane.current
    const i = img.current
    if (p && i) {
      const pr = p.getBoundingClientRect()
      const ir = i.getBoundingClientRect()
      const px = at ? at.x - pr.left : p.clientWidth / 2
      const py = at ? at.y - pr.top : p.clientHeight / 2
      if (ir.width && ir.height) anchor.current = { fx: (px - (ir.left - pr.left)) / ir.width, fy: (py - (ir.top - pr.top)) / ir.height, px, py }
    }
    set(next)
  }
  const toggle = (at: { x: number; y: number }) => zoomTo(zNow.current > 1 ? 1 : 2, at)
  const live = useRef({ zoomTo, toggle })

  // After a zoom, scroll so the anchored point is where it was. Zoomed from outside (the
  // Duplicates buttons), keep the middle of the pane in the middle.
  useLayoutEffect(() => {
    live.current = { zoomTo, toggle }
    const p = pane.current
    const i = img.current
    const a = anchor.current
    const was = zWas.current
    zWas.current = z
    zNow.current = z
    anchor.current = null
    if (!p || !i || was === z) return
    if (a) {
      const pr = p.getBoundingClientRect()
      const ir = i.getBoundingClientRect()
      p.scrollLeft += ir.left - pr.left + a.fx * ir.width - a.px
      p.scrollTop += ir.top - pr.top + a.fy * ir.height - a.py
    } else {
      const r = z / was
      p.scrollLeft = (p.scrollLeft + p.clientWidth / 2) * r - p.clientWidth / 2
      p.scrollTop = (p.scrollTop + p.clientHeight / 2) * r - p.clientHeight / 2
    }
  })

  // The pane's size, for Fit
  useEffect(() => {
    const p = pane.current
    if (!p) return
    const ro = new ResizeObserver(([e]) => setBox({ w: e.contentRect.width, h: e.contentRect.height }))
    ro.observe(p)
    return () => ro.disconnect()
  }, [])

  // Ctrl+wheel and pinches: listeners that may prevent the browser zooming the page
  useEffect(() => {
    const p = pane.current
    if (!p) return
    let pinch: { d: number; z: number } | null = null
    const dist = (t: TouchList) => Math.hypot(t[0].clientX - t[1].clientX, t[0].clientY - t[1].clientY)
    const mid = (t: TouchList) => ({ x: (t[0].clientX + t[1].clientX) / 2, y: (t[0].clientY + t[1].clientY) / 2 })
    const onWheel = (e: WheelEvent) => {
      if (!e.ctrlKey) return
      e.preventDefault()
      const dy = e.deltaY * (e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? 400 : 1)
      live.current.zoomTo(zNow.current * Math.exp(-dy * 0.002), { x: e.clientX, y: e.clientY })
    }
    const onStart = (e: TouchEvent) => {
      if (e.touches.length !== 2) return
      pinch = { d: dist(e.touches) || 1, z: zNow.current }
      if (e.cancelable) e.preventDefault()
    }
    const onMove = (e: TouchEvent) => {
      if (!pinch || e.touches.length !== 2) return
      if (e.cancelable) e.preventDefault()
      live.current.zoomTo((pinch.z * dist(e.touches)) / pinch.d, mid(e.touches))
    }
    const onEnd = (e: TouchEvent) => {
      if (e.touches.length < 2) pinch = null
    }
    const onGesture = (e: Event) => e.preventDefault() // Safari's own pinch, which would zoom the page
    p.addEventListener('wheel', onWheel, { passive: false })
    p.addEventListener('touchstart', onStart, { passive: false })
    p.addEventListener('touchmove', onMove, { passive: false })
    p.addEventListener('touchend', onEnd)
    p.addEventListener('touchcancel', onEnd)
    p.addEventListener('gesturestart', onGesture)
    return () => {
      p.removeEventListener('wheel', onWheel)
      p.removeEventListener('touchstart', onStart)
      p.removeEventListener('touchmove', onMove)
      p.removeEventListener('touchend', onEnd)
      p.removeEventListener('touchcancel', onEnd)
      p.removeEventListener('gesturestart', onGesture)
    }
  }, [])

  const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    if (e.pointerType === 'touch') fingers.current.add(e.pointerId)
    if (e.button !== 0 || fingers.current.size > 1) {
      press.current = null
      lastTap.current = null
      return
    }
    const p = e.currentTarget
    const drag = e.pointerType === 'mouse' && z > 1
    press.current = { x: e.clientX, y: e.clientY, left: p.scrollLeft, top: p.scrollTop, at: e.timeStamp, moved: false, drag }
    if (drag) p.setPointerCapture(e.pointerId)
  }
  const onPointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    const s = press.current
    if (!s) return
    const dx = e.clientX - s.x
    const dy = e.clientY - s.y
    if (!s.moved && Math.hypot(dx, dy) > TAP_PX) {
      s.moved = true
      if (s.drag) setDragging(true)
    }
    if (s.drag && s.moved) {
      e.currentTarget.scrollLeft = s.left - dx
      e.currentTarget.scrollTop = s.top - dy
    }
  }
  const onPointerUp = (e: React.PointerEvent<HTMLDivElement>) => {
    fingers.current.delete(e.pointerId)
    const s = press.current
    press.current = null
    setDragging(false)
    if (!s || s.moved || e.timeStamp - s.at > TAP_MS) return
    const t = lastTap.current
    if (t && e.timeStamp - t.at < TAP_MS && Math.hypot(e.clientX - t.x, e.clientY - t.y) < TAP_PX * 3) {
      lastTap.current = null
      live.current.toggle({ x: e.clientX, y: e.clientY })
    } else lastTap.current = { x: e.clientX, y: e.clientY, at: e.timeStamp }
  }
  const onPointerCancel = (e: React.PointerEvent<HTMLDivElement>) => {
    fingers.current.delete(e.pointerId)
    press.current = null
    setDragging(false)
  }
  const onKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.ctrlKey || e.metaKey || e.altKey) return
    if (e.key === '+' || e.key === '=') zoomTo(stepZoom(z, 1))
    else if (e.key === '-' || e.key === '_') zoomTo(stepZoom(z, -1))
    else if (e.key === '0') zoomTo(1)
    else {
      // Zoomed in, the arrow keys scroll the scan rather than turn the page
      if (z > 1 && e.key.startsWith('Arrow')) e.stopPropagation()
      return
    }
    e.preventDefault()
  }

  return (
    <div className={`scan-img${className ? ` ${className}` : ''}`}>
      <div
        ref={pane}
        className={`scan-pane${z > 1 ? ' zoomed' : ''}${dragging ? ' dragging' : ''}`}
        role="region"
        tabIndex={0}
        aria-label={`${alt}. Zoom with + and −, 0 to fit.`}
        onKeyDown={onKeyDown}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerCancel}
      >
        <img
          ref={img}
          src={imageAt(src, sharp ? SHARP : size)}
          alt={alt}
          style={style}
          draggable={false}
          onLoad={(e) => setNat({ w: e.currentTarget.naturalWidth, h: e.currentTarget.naturalHeight })}
        />
      </div>
      {bar && (
        <div className="scan-bar" role="group" aria-label="Zoom">
          <button className="icon-btn" onClick={() => zoomTo(stepZoom(z, -1))} disabled={z <= 1} aria-label="Zoom out" data-tip="Zoom out (−)">
            <Icon name="zout" />
          </button>
          <span className="scan-pct" aria-live="polite">
            {z === 1 ? 'Fit' : `${Math.round(z * 100)}%`}
          </span>
          <button
            className="icon-btn"
            onClick={() => zoomTo(stepZoom(z, 1))}
            disabled={z >= ZOOM_MAX}
            aria-label="Zoom in"
            data-tip="Zoom in (+). Ctrl+scroll, a double click or tap, or a pinch zooms too; drag to look around."
          >
            <Icon name="zin" />
          </button>
          <button className="btn ghost" onClick={() => zoomTo(1)} disabled={z === 1} data-tip="Fit the whole page in view (0)">
            Fit
          </button>
        </div>
      )}
    </div>
  )
}
