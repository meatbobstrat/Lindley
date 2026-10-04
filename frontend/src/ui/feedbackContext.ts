// Toasts, menus and Undo, shared through context (ui/feedback.tsx provides them).

import { createContext, useContext } from 'react'
import type { IconName } from './icons'

export type MenuItem =
  | '-'
  | { head: string }
  | { label: string; icon?: IconName; onSelect: () => void; disabled?: boolean; depth?: number; tip?: string }

export interface Feedback {
  toast: (msg: string, undo?: number | null) => void
  /** Make a change, then show what happened, with Undo when it can be undone. */
  run: <T>(p: Promise<T>, msg: string | ((r: T) => string)) => Promise<T | undefined>
  undo: (batch?: number | null) => void
  openMenu: (items: MenuItem[], anchor: Element, opts?: { at?: { x: number; y: number } }) => void
}

export const FeedbackCtx = createContext<Feedback | null>(null)

export function useFeedback(): Feedback {
  const f = useContext(FeedbackCtx)
  if (!f) throw new Error('useFeedback outside FeedbackProvider')
  return f
}

