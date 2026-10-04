// Toasts (with Undo), popup menus and dialogs, shared by every view.

import {
  type CSSProperties,
  type ReactNode,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react'
import { api, type Change } from '../api/client'
import { invalidate } from '../api/store'
import { FeedbackCtx, type MenuItem } from './feedbackContext'
import { Icon } from './icons'

export function FeedbackProvider({ children }: { children: ReactNode }) {
  const [toastState, setToast] = useState<{ msg: string; undo?: number | null; n: number } | null>(null)
  const [menu, setMenu] = useState<{ items: MenuItem[]; x: number; y: number; up: boolean; back: Element | null } | null>(null)
  const timer = useRef<number>(undefined)

  const toast = useCallback((msg: string, undo?: number | null) => {
    setToast((t) => ({ msg, undo, n: (t?.n ?? 0) + 1 }))
    window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => setToast(null), undo ? 7000 : 3500)
  }, [])

  const undo = useCallback(
    (batch?: number | null) => {
      api.undo(batch).then(
        () => {
          invalidate()
          toast('Undone.')
        },
        (e: Error) => toast(e.message),
      )
    },
    [toast],
  )

  const run = useCallback(
    async <T,>(p: Promise<T>, msg: string | ((r: T) => string)) => {
      try {
        const r = await p
        invalidate()
        const c = r as Partial<Change>
        toast(typeof msg === 'function' ? msg(r) : msg, c && typeof c === 'object' ? c.undo : null)
        return r
      } catch (e) {
        toast((e as Error).message)
        invalidate()
        return undefined
      }
    },
    [toast],
  )

  const openMenu = useCallback((items: MenuItem[], anchor: Element, opts?: { at?: { x: number; y: number } }) => {
    const r = anchor.getBoundingClientRect()
    // Menus from the toolbar open upward, so they stay next to the button.
    const up = !!anchor.closest('.dock') && r.top > window.innerHeight / 2
    setMenu({
      items,
      x: opts?.at?.x ?? r.left,
      y: opts?.at?.y ?? (up ? r.top : r.bottom + 4),
      up: up && !opts?.at,
      back: document.activeElement,
    })
  }, [])

  const value = useMemo(() => ({ toast, run, undo, openMenu }), [toast, run, undo, openMenu])

  return (
    <FeedbackCtx.Provider value={value}>
      {children}
      {menu && <Menu {...menu} close={() => setMenu(null)} />}
      {toastState && (
        <div
          className="toast"
          role="status"
          key={toastState.n}
          onPointerEnter={() => window.clearTimeout(timer.current)}
          onFocus={() => window.clearTimeout(timer.current)}
          onPointerLeave={() => {
            timer.current = window.setTimeout(() => setToast(null), 4000)
          }}
        >
          <span>{toastState.msg}</span>
          {toastState.undo ? (
            <button
              data-tip="Put things back the way they were before this change"
              onClick={() => {
                setToast(null)
                undo(toastState.undo)
              }}
            >
              Undo
            </button>
          ) : null}
        </div>
      )}
    </FeedbackCtx.Provider>
  )
}

function Menu({
  items,
  x,
  y,
  up,
  back,
  close,
}: {
  items: MenuItem[]
  x: number
  y: number
  up: boolean
  back: Element | null
  close: () => void
}) {
  const ref = useRef<HTMLDivElement>(null)
  const done = useCallback(
    (refocus: boolean) => {
      close()
      if (refocus && back instanceof HTMLElement) back.focus()
    },
    [close, back],
  )

  useLayoutEffect(() => {
    const m = ref.current
    if (!m) return
    const r = m.getBoundingClientRect()
    m.style.left = `${Math.max(8, Math.min(x, window.innerWidth - r.width - 8))}px`
    m.style.top = `${Math.max(8, Math.min(up ? y - r.height - 6 : y, window.innerHeight - r.height - 8))}px`
    m.querySelector<HTMLButtonElement>('button:not(:disabled)')?.focus()
  }, [x, y, up])

  useEffect(() => {
    const onDown = (e: PointerEvent) => {
      if (!ref.current?.contains(e.target as Node)) done(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopPropagation()
        done(true)
        return
      }
      if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(e.key)) return
      e.preventDefault()
      const bs = [...(ref.current?.querySelectorAll<HTMLButtonElement>('button:not(:disabled)') ?? [])]
      const i = bs.indexOf(document.activeElement as HTMLButtonElement)
      const j =
        e.key === 'Home' ? 0 : e.key === 'End' ? bs.length - 1 : (i + (e.key === 'ArrowDown' ? 1 : -1) + bs.length) % bs.length
      bs[j]?.focus()
    }
    document.addEventListener('pointerdown', onDown, true)
    document.addEventListener('keydown', onKey, true)
    return () => {
      document.removeEventListener('pointerdown', onDown, true)
      document.removeEventListener('keydown', onKey, true)
    }
  }, [done])

  return (
    <div className="menu" role="menu" ref={ref}>
      {items.map((it, i) => {
        if (it === '-') return <hr key={i} />
        if ('head' in it)
          return (
            <div className="mh" key={i}>
              {it.head}
            </div>
          )
        return (
          <button
            key={i}
            role="menuitem"
            disabled={it.disabled}
            data-depth={it.depth != null ? '' : undefined}
            style={it.depth != null ? ({ '--d': it.depth } as CSSProperties) : undefined}
            data-tip={it.tip}
            onClick={() => {
              done(false)
              it.onSelect()
            }}
          >
            {it.icon && <Icon name={it.icon} />}
            {it.label}
          </button>
        )
      })}
    </div>
  )
}

/** A dialog: native <dialog>, so focus stays inside it and Escape closes it. */
export function Modal({
  title,
  onClose,
  children,
  className = 'modal',
}: {
  title: string
  onClose: () => void
  children: ReactNode
  className?: string
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const back = useRef<Element | null>(null)
  useEffect(() => {
    back.current = document.activeElement
    const d = ref.current
    if (d && !d.open) d.showModal()
    return () => {
      if (back.current instanceof HTMLElement) back.current.focus()
    }
  }, [])
  return (
    <dialog
      ref={ref}
      className={className}
      aria-labelledby="modal-h"
      onCancel={(e) => {
        e.preventDefault()
        onClose()
      }}
      onClick={(e) => {
        // A click on the backdrop, outside the dialog's box
        const r = ref.current?.getBoundingClientRect()
        if (e.target === ref.current && r && (e.clientX < r.left || e.clientX > r.right || e.clientY < r.top || e.clientY > r.bottom))
          onClose()
      }}
    >
      <h2 id="modal-h">{title}</h2>
      {children}
    </dialog>
  )
}
