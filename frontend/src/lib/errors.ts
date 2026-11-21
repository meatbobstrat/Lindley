// Errors the page can't recover from, sent to Lindley's own log (POST /api/log), where they sit
// with the rest of what happened: a browser's console is one nobody has open. Reports go to this
// computer only, and each is short, sent once, and at most a few in all, so a page stuck in a
// loop can't fill the log. Reporting never throws: it's the last thing that may go wrong.

import { ApiError } from '../api/client'

const MOST_REPORTS = 20
let sent = 0
const seen = new Set<string>()

/** An error in a line: its kind and what it said. */
export function describe(e: unknown): string {
  if (e instanceof Error) return `${e.name}: ${e.message}`
  if (typeof e === 'string') return e
  try {
    return JSON.stringify(e) ?? String(e)
  } catch {
    return String(e)
  }
}

export function reportProblem(level: 'error' | 'warning', message: string, error?: unknown): void {
  try {
    // The backend not answering is told on the screen, and can't be told to the log anyway
    if (error instanceof ApiError && error.status === 0) return
    const key = `${level}:${message}`
    if (seen.has(key) || sent >= MOST_REPORTS) return
    seen.add(key)
    sent += 1
    const stack = error instanceof Error ? error.stack : undefined
    void fetch('/api/log', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        level,
        message: message.slice(0, 500),
        stack: stack?.slice(0, 4000),
        where: window.location.pathname.slice(0, 200),
      }),
      keepalive: true,
    }).catch(() => undefined)
  } catch {
    // nothing more can be done about it
  }
}

/** What nothing caught: an error thrown outside React, and a promise that failed with nobody
 * waiting on it. */
export function installErrorReporting(): void {
  window.addEventListener('error', (ev) => {
    if (ev.message.startsWith('ResizeObserver loop')) return // the browser's, harmless
    reportProblem('error', describe(ev.error ?? ev.message), ev.error)
  })
  window.addEventListener('unhandledrejection', (ev) => {
    const level = ev.reason instanceof ApiError ? 'warning' : 'error'
    reportProblem(level, `Unhandled: ${describe(ev.reason)}`, ev.reason)
  })
}
