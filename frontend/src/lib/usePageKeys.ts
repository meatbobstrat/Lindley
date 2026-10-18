// ← → (and Page Up, Page Down) go to the one before or after, wherever a view shows one at a
// time. Not while typing, on a row of tabs (they have arrow keys of their own), with a dialog or
// menu open, or with Alt, Ctrl or ⌘ held (Alt+← is the browser's Back).

import { useEffect } from 'react'
import { typing } from './view'

export function usePageKeys(prev: () => void, next: () => void, on = true) {
  useEffect(() => {
    if (!on) return
    const onKey = (e: KeyboardEvent) => {
      if (e.altKey || e.ctrlKey || e.metaKey || typing(e.target) || (e.target as Element).matches?.('.tab') || document.querySelector('dialog[open], .menu')) return
      if (e.key === 'ArrowRight' || e.key === 'PageDown') next()
      else if (e.key === 'ArrowLeft' || e.key === 'PageUp') prev()
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  })
}
