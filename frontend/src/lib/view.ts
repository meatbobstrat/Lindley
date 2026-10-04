// Per-viewer conveniences kept in this browser, and small view helpers.

import { useState } from 'react'
import { useLocation } from 'react-router'
import { useLocal } from '../api/store'

export type Theme = 'system' | 'light' | 'dark'

export const useTheme = () => useLocal<Theme>('theme', 'system')

export const useThumb = () => useLocal<number>('thumb', 150)

export const useDockPos = () => useLocal<{ x: number; y: number } | null>('dock', null)

/** Selection that starts with the pages another view asked to show (navigate state.select). */
export function useSelection(): [Set<number>, (s: Set<number>) => void] {
  const loc = useLocation()
  const [sel, setSel] = useState<Set<number>>(() => new Set((loc.state as { select?: number[] } | null)?.select ?? []))
  return [sel, setSel]
}

/** The text as the person left it in an editable text box. */
export const textOf = (el: HTMLElement | null) => (el ? el.innerText.replace(/\n{3,}/g, '\n\n').trim() : '')

export const typing = (t: EventTarget | null) => t instanceof Element && !!t.closest('input, textarea, select, [contenteditable="true"]')
