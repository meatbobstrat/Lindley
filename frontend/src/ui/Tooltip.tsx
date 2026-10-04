// One tooltip for the whole app. Any element with a `data-tip` attribute gets it:
//   <button data-tip="Turn the selected pages a quarter turn to the left">…</button>
// It follows WCAG 2.2's rule for content on hover or focus (1.4.13):
// - shown on hover (after a short pause) and on keyboard focus, never on a timer;
// - hoverable: it stays while the pointer moves onto it;
// - dismissible: Escape closes it without moving focus or the pointer.
// While it's shown, the element is described by it (aria-describedby), unless the tooltip only
// repeats the element's own name.

import { type ReactNode, useEffect, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

const TIP_ID = 'lindley-tip'
const SHOW_MS = 450 // hover pause before the first tooltip
const HIDE_MS = 120 // grace to move from the element onto the tooltip

interface Shown {
  text: string
  el: HTMLElement
}

function tipFor(t: EventTarget | null): HTMLElement | null {
  return t instanceof Element ? t.closest<HTMLElement>('[data-tip]') : null
}

function repeatsName(el: HTMLElement, text: string): boolean {
  const name = el.getAttribute('aria-label') ?? el.textContent ?? ''
  return name.trim() === text.trim()
}

export function TooltipLayer() {
  const [shown, setShown] = useState<Shown | null>(null)
  const box = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let showT: number | undefined
    let hideT: number | undefined
    let current: HTMLElement | null = null

    const describe = (el: HTMLElement | null, on: boolean) => {
      if (!el) return
      if (on && !el.hasAttribute('aria-describedby') && !repeatsName(el, el.dataset.tip ?? '')) {
        el.setAttribute('aria-describedby', TIP_ID)
        el.dataset.tipDescribed = '1'
      } else if (!on && el.dataset.tipDescribed) {
        el.removeAttribute('aria-describedby')
        delete el.dataset.tipDescribed
      }
    }
    const hideNow = () => {
      window.clearTimeout(showT)
      window.clearTimeout(hideT)
      describe(current, false)
      current = null
      setShown(null)
    }
    const show = (el: HTMLElement, delay: number) => {
      window.clearTimeout(hideT)
      window.clearTimeout(showT)
      if (el === current) return
      showT = window.setTimeout(() => {
        const text = el.dataset.tip
        if (!text || !el.isConnected) return
        describe(current, false)
        current = el
        describe(el, true)
        setShown({ text, el })
      }, delay)
    }
    const hideSoon = () => {
      window.clearTimeout(showT)
      window.clearTimeout(hideT)
      hideT = window.setTimeout(hideNow, HIDE_MS)
    }

    const onOver = (e: PointerEvent) => {
      if (e.pointerType === 'touch') return
      if (box.current?.contains(e.target as Node)) {
        window.clearTimeout(hideT) // hoverable: the pointer is on the tooltip
        return
      }
      const el = tipFor(e.target)
      if (el) show(el, current ? 0 : SHOW_MS)
      else if (current || showT) hideSoon()
    }
    const onFocus = (e: FocusEvent) => {
      const el = tipFor(e.target)
      if (el && el.matches(':focus-visible')) show(el, 0)
    }
    const onBlur = () => hideSoon()
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && current) hideNow()
    }
    const onLeaveWindow = (e: PointerEvent) => {
      if (!e.relatedTarget) hideSoon()
    }

    document.addEventListener('pointerover', onOver)
    document.addEventListener('pointerout', onLeaveWindow)
    document.addEventListener('pointerdown', hideNow, true)
    document.addEventListener('focusin', onFocus)
    document.addEventListener('focusout', onBlur)
    document.addEventListener('keydown', onKey, true)
    document.addEventListener('scroll', hideNow, true)
    window.addEventListener('blur', hideNow)
    return () => {
      document.removeEventListener('pointerover', onOver)
      document.removeEventListener('pointerout', onLeaveWindow)
      document.removeEventListener('pointerdown', hideNow, true)
      document.removeEventListener('focusin', onFocus)
      document.removeEventListener('focusout', onBlur)
      document.removeEventListener('keydown', onKey, true)
      document.removeEventListener('scroll', hideNow, true)
      window.removeEventListener('blur', hideNow)
      window.clearTimeout(showT)
      window.clearTimeout(hideT)
    }
  }, [])

  // Below the element, or above it when there's no room. In a vertical list (the folder tree,
  // Settings' sections, a menu) it goes beside the element instead, so it never covers the next
  // row: being hoverable, it would stop the pointer reaching it. `data-tip-side` chooses a side.
  // Always inside the window.
  useLayoutEffect(() => {
    const t = box.current
    if (!t || !shown) return
    const el = shown.el
    const r = el.getBoundingClientRect()
    const w = t.offsetWidth
    const h = t.offsetHeight
    const gap = 8
    const vw = window.innerWidth
    const vh = window.innerHeight
    const side = el.dataset.tipSide ?? (el.closest('.tree, .set-nav, .menu') ? 'right' : 'bottom')
    let top: number
    let left: number
    if (side === 'right') {
      left = r.right + gap
      if (left + w > vw - 4) left = Math.max(4, r.left - w - gap)
      top = r.top + r.height / 2 - h / 2
    } else {
      top = side === 'top' ? r.top - h - gap : r.bottom + gap
      if (top + h > vh - 4) top = r.top - h - gap
      if (top < 4) top = r.bottom + gap
      left = r.left + r.width / 2 - w / 2
    }
    t.style.top = `${Math.min(Math.max(4, top), vh - h - 4)}px`
    t.style.left = `${Math.min(Math.max(4, left), vw - w - 4)}px`
  }, [shown])

  // A tooltip in a dialog must be in the dialog's top layer to show above it.
  const host = shown?.el.closest('dialog[open]')
  const tip = (
    <div ref={box} id={TIP_ID} role="tooltip" className="tip" hidden={!shown}>
      {shown?.text}
    </div>
  )
  return host ? <DialogTip host={host as HTMLDialogElement}>{tip}</DialogTip> : tip
}

function DialogTip({ host, children }: { host: HTMLDialogElement; children: ReactNode }) {
  return createPortal(children, host)
}
