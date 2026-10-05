// The floating toolbar: every action for what you're looking at, close to where you're looking.
// Drag it by its grip (or move it with the arrow keys there; Home puts it back). When it doesn't
// fit, secondary buttons drop their text, keeping their tooltip and accessible name.

import {
  type CSSProperties,
  type KeyboardEvent as KeyboardEvent_,
  type MouseEvent as MouseEvent_,
  type PointerEvent as PointerEvent_,
  type ReactNode,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
} from 'react'
import { useDockPos } from '../lib/view'
import { Icon, type IconName } from './icons'

export function Dock({ label, children }: { label: string; children: ReactNode }) {
  const [pos, setPos] = useDockPos()
  const wrap = useRef<HTMLDivElement>(null)
  const bar = useRef<HTMLDivElement>(null)
  const narrow = () => window.matchMedia('(max-width: 860px)').matches

  const fit = useCallback(() => {
    const d = bar.current
    const w = wrap.current
    if (!d || !w) return
    d.classList.remove('compact', 'tiny')
    if (d.scrollWidth > d.clientWidth + 1) d.classList.add('compact')
    if (d.scrollWidth > d.clientWidth + 1) d.classList.add('tiny')
    const main = w.parentElement
    if (pos && main && !narrow()) {
      const m = main.getBoundingClientRect()
      const r = d.getBoundingClientRect()
      w.style.left = `${Math.max(0, Math.min(pos.x, m.width - r.width))}px`
      w.style.top = `${Math.max(0, Math.min(pos.y, m.height - r.height))}px`
    } else {
      w.style.left = ''
      w.style.top = ''
    }
  }, [pos])

  useLayoutEffect(fit)
  useEffect(() => {
    const main = wrap.current?.parentElement
    if (!main) return
    main.classList.add('has-dock')
    const ro = new ResizeObserver(fit)
    ro.observe(main)
    return () => {
      ro.disconnect()
      main.classList.remove('has-dock')
    }
  }, [fit])

  const moveTo = (x: number, y: number) => {
    const main = wrap.current?.parentElement
    const d = bar.current
    if (!main || !d) return
    const m = main.getBoundingClientRect()
    const r = d.getBoundingClientRect()
    setPos({ x: Math.round(Math.max(0, Math.min(x, m.width - r.width))), y: Math.round(Math.max(0, Math.min(y, m.height - r.height))) })
  }

  const onGripDown = (e: PointerEvent_<HTMLButtonElement>) => {
    const main = wrap.current?.parentElement
    const d = bar.current
    if (!main || !d || narrow()) return
    e.preventDefault()
    const m = main.getBoundingClientRect()
    const r = d.getBoundingClientRect()
    const ox = e.clientX - r.left
    const oy = e.clientY - r.top
    const g = e.currentTarget
    g.setPointerCapture(e.pointerId)
    d.classList.add('dragging')
    const mv = (ev: PointerEvent) => moveTo(ev.clientX - m.left - ox, ev.clientY - m.top - oy)
    const up = () => {
      g.removeEventListener('pointermove', mv)
      g.removeEventListener('pointerup', up)
      d.classList.remove('dragging')
    }
    g.addEventListener('pointermove', mv)
    g.addEventListener('pointerup', up)
  }

  const onKey = (e: KeyboardEvent_) => {
    const t = e.target as HTMLElement
    if (t.matches('[data-grip]') && /^(Arrow(Up|Down|Left|Right)|Home)$/.test(e.key)) {
      e.preventDefault()
      e.stopPropagation() // the toolbar's own keys: not the view's (← → turn the page)
      if (e.key === 'Home') {
        setPos(null)
        return
      }
      const main = wrap.current?.parentElement
      const d = bar.current
      if (!main || !d) return
      const m = main.getBoundingClientRect()
      const r = d.getBoundingClientRect()
      const step = e.shiftKey ? 96 : 24
      const dx = e.key === 'ArrowLeft' ? -step : e.key === 'ArrowRight' ? step : 0
      const dy = e.key === 'ArrowUp' ? -step : e.key === 'ArrowDown' ? step : 0
      moveTo(r.left - m.left + dx, r.top - m.top + dy)
      return
    }
    // Arrow keys move along the toolbar, as in any toolbar.
    if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
      e.preventDefault()
      e.stopPropagation()
      const bs = [...(bar.current?.querySelectorAll<HTMLButtonElement>('button:not(:disabled)') ?? [])]
      const i = bs.indexOf(t.closest('button') as HTMLButtonElement)
      bs[(i + (e.key === 'ArrowRight' ? 1 : -1) + bs.length) % bs.length]?.focus()
    }
  }

  return (
    <div className={`dock-wrap${pos && !narrow() ? ' free' : ''}`} ref={wrap}>
      <div className="dock" role="toolbar" aria-label={label} ref={bar} onKeyDown={onKey}>
        <button
          className="dock-grip"
          data-grip
          aria-label="Move toolbar. Arrow keys move it, Home puts it back."
          data-tip="Drag to move the toolbar. Double-click to put it back at the bottom."
          onPointerDown={onGripDown}
          onDoubleClick={() => setPos(null)}
        >
          <Icon name="grip" />
        </button>
        {children}
      </div>
    </div>
  )
}

export function DBtn({
  icon,
  label,
  tip,
  onClick,
  disabled,
  kind = '',
  iconOnly,
}: {
  icon: IconName
  label: string
  tip?: string
  onClick: (e: MouseEvent_<HTMLButtonElement>) => void
  disabled?: boolean
  kind?: '' | 'primary' | 'ai' | 'keep' | 'next'
  iconOnly?: boolean
}) {
  return (
    <button
      className={`btn ${iconOnly ? 'ico' : ''} ${kind}`}
      disabled={disabled}
      onClick={onClick}
      aria-label={iconOnly ? label : undefined}
      data-tip={tip ?? label}
    >
      <Icon name={icon} />
      {!iconOnly && <span className="lbl">{label}</span>}
    </button>
  )
}

export const Sep = () => <span className="sep" aria-hidden="true" />

export function DockText({ children }: { children: ReactNode }) {
  return <span className="dock-sel">{children}</span>
}

export function DockProgress({ text, at, of }: { text: string; at: number; of: number }) {
  return (
    <span className="dock-prog">
      <span>{text}</span>
      <b style={{ '--v': `${Math.round((at / Math.max(1, of)) * 100)}%` } as CSSProperties} aria-hidden="true" />
    </span>
  )
}
