// Words the UI says: plurals, short names, dates and how a page was read.

import type { Page } from '../api/client'

export const plural = (n: number, w: string, ws = `${w}s`) => `${n.toLocaleString()} ${n === 1 ? w : ws}`

// US dollars to the cent; a cost too small to show is "under 1¢"
export const dollars = (n: number) => (n > 0 && n < 0.005 ? 'under 1¢' : `${n.toFixed(2)}`)

// A size on disk: "33 MB", "5.2 GB"
export const size = (bytes: number) => (bytes < 1e9 ? `${Math.max(1, Math.round(bytes / 1e6))} MB` : `${(bytes / 1e9).toFixed(1)} GB`)

// How long something takes, roughly: "about a minute", "about 25 minutes", "about 1½ hours"
export function duration(seconds: number): string {
  const m = seconds / 60
  if (m < 1.5) return 'about a minute'
  if (m < 10) return `about ${Math.round(m)} minutes`
  if (m < 55) return `about ${Math.round(m / 5) * 5} minutes`
  const h = m / 60
  if (h < 1.25) return 'about an hour'
  if (h < 10) {
    const halves = Math.round(h * 2) / 2
    return `about ${Math.floor(halves)}${halves % 1 ? '½' : ''} hours`
  }
  if (h < 36) return `about ${Math.round(h)} hours`
  return `about ${Math.round(h / 24)} days`
}

export const shortName = (n: string, max = 26) => (n.length > max ? `${n.slice(0, max - 1)}…` : n)

/** A name in quotes, unless it has its own: Lindley's (Pages starting “…”) do, as
 * lindley.duplicates.resolve.quoted says. One shortened past its closing quote gets it back. */
export function quoted(n: string): string {
  const opens = n.split('“').length - 1
  if (!opens) return `“${n}”`
  return opens > n.split('”').length - 1 ? `${n}”` : n
}

/** Reasons as sentences, each ending in one full stop: the rules' have none, an AI's often
 * have their own (“…to Shermantown.”), and an ellipsis isn't the end of a sentence. */
export const sentences = (reasons: string[]) =>
  reasons
    .map((r) => r.trim())
    .filter(Boolean)
    .map((r) => (/[.!?]["'”’)\]]*$/.test(r) ? r : `${r}.`))
    .join(' ')

/** "it"/"them" and friends, by count. */
export const them = (n: number) => (n === 1 ? 'it' : 'them')
export const needs = (n: number) => (n === 1 ? 'needs' : 'need')

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

/** A document's date, kept as partial ISO 8601 (1892, 1892-03, 1892-03-14), in words. */
export function docDate(d: string | null | undefined): string {
  if (!d) return ''
  const m = d.match(/^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?/)
  if (!m) return d
  const [, y, mo, da] = m
  if (!mo) return y
  const month = MONTHS[Number(mo) - 1] ?? mo
  return da ? `${Number(da)} ${month} ${y}` : `${month} ${y}`
}

/** A date and time the database keeps in UTC ("2026-09-29 10:14:00"), in local words. */
export function when(ts: string | null | undefined, withTime = true): string {
  if (!ts) return ''
  const d = new Date(ts.includes('T') ? ts : `${ts.replace(' ', 'T')}Z`)
  if (Number.isNaN(d.getTime())) return ts
  return d.toLocaleString(undefined, {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
    ...(withTime ? { hour: '2-digit', minute: '2-digit' } : {}),
  })
}

export const readBy = (p: Pick<Page, 'source'>) =>
  p.source === 'vision' ? 'the AI vision model' : p.source === 'user' ? 'you' : 'Tesseract OCR'

export const readShort = (p: Pick<Page, 'source'>) => (p.source === 'vision' ? 'AI' : p.source === 'user' ? 'Yours' : 'OCR')

/** "Read by Tesseract OCR, 84% confident" */
export function readLong(p: Pick<Page, 'source' | 'confidence'>): string {
  if (p.source === 'user') return 'You corrected this text'
  return `Read by ${readBy(p)}${p.confidence != null ? `, ${p.confidence}% confident` : ''}`
}

export const kindOf = (p: Pick<Page, 'script' | 'blank'>) =>
  p.blank
    ? 'A blank page'
    : ({ handwritten: 'Handwriting', printed: 'Printed text', typed: 'Typewritten text', mixed: 'Print and handwriting' } as Record<string, string>)[
        p.script ?? ''
      ] ?? 'Scan'

export const pageLabel = (p: Page, i?: number) => (i === undefined ? p.file : `Page ${i + 1}`)
