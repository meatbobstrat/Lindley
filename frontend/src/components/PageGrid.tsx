// A grid of page thumbnails. Click selects one; Ctrl or Shift selects more; Enter or Space adds
// to the selection; double-click opens. Pages drag to reorder a document, or onto the folder tree.

import { type CSSProperties, type ReactNode, useRef, useState } from 'react'
import type { Page } from '../api/client'
import { drag } from '../lib/drag'
import { Icon } from '../ui/icons'
import { ConfChip, Thumb } from './bits'

export interface GridProps {
  pages: Page[]
  numbered?: boolean
  sel: Set<number>
  setSel: (s: Set<number>) => void
  onOpen: (p: Page, i: number) => void
  onMenu?: (ids: number[], at: { x: number; y: number }, anchor: Element) => void
  onReorder?: (ids: number[], target: number, after: boolean) => void
  onReview?: (p: Page) => void
  below?: (p: Page) => ReactNode
  empty: ReactNode
  thumb: number
}

export function PageGrid({ pages, numbered, sel, setSel, onOpen, onMenu, onReorder, onReview, below, empty, thumb }: GridProps) {
  const anchor = useRef<number | null>(null)
  const [dropAt, setDropAt] = useState<{ id: number; after: boolean } | null>(null)
  const [dragging, setDragging] = useState<number[]>([])

  if (!pages.length) return <div className="empty">{empty}</div>

  const click = (e: React.MouseEvent, p: Page) => {
    const ids = pages.map((x) => x.id)
    const next = new Set(sel)
    const keyboard = e.detail === 0
    if (e.shiftKey && anchor.current != null && ids.includes(anchor.current)) {
      const [a, b] = [ids.indexOf(anchor.current), ids.indexOf(p.id)].sort((x, y) => x - y)
      ids.slice(a, b + 1).forEach((x) => next.add(x))
    } else if (e.ctrlKey || e.metaKey || keyboard || (e.target as Element).closest('[data-check]')) {
      if (next.has(p.id)) next.delete(p.id)
      else next.add(p.id)
      anchor.current = p.id
    } else {
      const only = sel.size === 1 && sel.has(p.id)
      next.clear()
      if (!only) next.add(p.id)
      anchor.current = p.id
    }
    setSel(next)
  }

  return (
    <div
      className="grid"
      role="list"
      aria-label="Pages. Click to select, Ctrl or Shift to select more, double-click to open."
      style={{ '--thumb': `${thumb}px` } as CSSProperties}
    >
      {pages.map((p, i) => {
        const on = sel.has(p.id)
        const cls = [
          'pg',
          on && 'is-sel',
          dragging.includes(p.id) && 'is-dragging',
          dropAt?.id === p.id && (dropAt.after ? 'drop-after' : 'drop-before'),
        ]
          .filter(Boolean)
          .join(' ')
        return (
          <div
            key={p.id}
            className={cls}
            role="listitem"
            draggable
            onDragStart={(e) => {
              const ids = on ? pages.filter((x) => sel.has(x.id)).map((x) => x.id) : [p.id]
              if (!on) setSel(new Set([p.id]))
              drag.pages = ids
              setDragging(ids)
              e.dataTransfer.effectAllowed = 'move'
              e.dataTransfer.setData('text/plain', ids.join(','))
            }}
            onDragEnd={() => {
              drag.pages = null
              setDragging([])
              setDropAt(null)
            }}
            onDragOver={(e) => {
              if (!onReorder || !drag.pages || drag.pages.includes(p.id)) return
              e.preventDefault()
              const r = e.currentTarget.getBoundingClientRect()
              setDropAt({ id: p.id, after: e.clientX > r.left + r.width / 2 })
            }}
            onDragLeave={() => setDropAt((d) => (d?.id === p.id ? null : d))}
            onDrop={(e) => {
              if (!onReorder || !drag.pages) return
              e.preventDefault()
              const r = e.currentTarget.getBoundingClientRect()
              onReorder(drag.pages, p.id, e.clientX > r.left + r.width / 2)
              drag.pages = null
              setDropAt(null)
            }}
            onContextMenu={(e) => {
              if (!onMenu) return
              e.preventDefault()
              const ids = on ? pages.filter((x) => sel.has(x.id)).map((x) => x.id) : [p.id]
              if (!on) setSel(new Set([p.id]))
              const r = e.currentTarget.getBoundingClientRect()
              // The keyboard's menu key gives no pointer position: open it at the page instead.
              onMenu(ids, { x: e.clientX || r.left + 20, y: e.clientY || r.top + 20 }, e.currentTarget)
            }}
          >
            <button
              className="pg-main"
              aria-pressed={on}
              onClick={(e) => click(e, p)}
              onDoubleClick={() => onOpen(p, i)}
              data-tip={`${numbered ? `Page ${i + 1}, ${p.file}` : p.file}. Click to select, double-click to open${onMenu ? ', right-click for more' : ''}.`}
              data-tip-side="top"
            >
              <Thumb p={p} size={Math.max(300, thumb * 2)} alt="" />
              <span className="pg-check" data-check aria-hidden="true">
                <Icon name="check" />
              </span>
              {p.origin === 'added' && !numbered && (
                <span className="pg-flag" data-tip="You added this scan with Add scans…">
                  <Icon name="upload" />
                  Added by you
                </span>
              )}
              <span className="pg-f">
                <span className={`pg-lbl ${numbered ? '' : 'mono'}`}>{numbered ? `Page ${i + 1}` : p.file}</span>
                <ConfChip p={p} onReview={onReview ? () => onReview(p) : undefined} />
              </span>
              {p.duplicate_of && (
                <span className="pg-dup" data-tip="A duplicate you set aside. The copy you kept is in its place.">
                  <Icon name="dup" />
                  <span>
                    Copy of <span className="mono">{p.duplicate_of.file}</span>
                  </span>
                </span>
              )}
            </button>
            {below?.(p)}
          </div>
        )
      })}
    </div>
  )
}
