// Where each AI connection runs, and so who else sees what it's sent. Always said in words.

import type { Connector, Job, ProviderConfig, Settings } from '../api/client'

export type Reach = 'this' | 'network' | 'server' | 'cloud'

export interface Conn {
  name: string // its id in ai.providers
  label: string
  cfg: ProviderConfig
  connector: Connector | undefined
  cloud: boolean
  reach: Reach
  company: string
}

export function hostOf(u: string | null | undefined): string {
  try {
    return new URL(u ?? '').hostname
  } catch {
    return ''
  }
}

/** Where a connection sends pages: this computer, your own network, a server elsewhere, or a cloud company. */
export function reachOf(cfg: ProviderConfig, connector: Connector | undefined): Reach {
  if (connector?.where === 'cloud' || (!connector && cfg.type !== 'local')) return 'cloud'
  const h = hostOf(cfg.base_url || connector?.default_base_url)
  if (!h || h === 'localhost' || h === '[::1]' || /^127\./.test(h)) return 'this'
  if (/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(h) || /\.(local|lan|home|internal)$/.test(h) || !h.includes('.')) return 'network'
  return 'server'
}

export function companyOf(cfg: ProviderConfig, connector: Connector | undefined): string {
  if (cfg.type === 'openai_compat') return hostOf(cfg.base_url) || 'that company'
  return connector?.company || connector?.label || cfg.type
}

export function connection(settings: Settings | undefined, connectors: Connector[], name: string | null | undefined): Conn | null {
  if (!settings || !name) return null
  const cfg = settings.ai.providers[name]
  if (!cfg) return null
  const connector = connectors.find((c) => c.id === cfg.type)
  const reach = reachOf(cfg, connector)
  return {
    name,
    label: cfg.label || connector?.short || name,
    cfg,
    connector,
    cloud: reach === 'cloud',
    reach,
    company: companyOf(cfg, connector),
  }
}

export function forJob(settings: Settings | undefined, connectors: Connector[], job: Job): Conn | null {
  return connection(settings, connectors, settings?.ai.jobs[job]?.connection)
}

export const JOB_WORDS: Record<Job, { label: string; what: string; none: string; short: string }> = {
  vision: {
    label: 'Reading handwriting and hard pages',
    what: 'page images',
    none: 'Nothing connected: handwriting and hard pages wait for your review instead.',
    short: 'reading handwriting',
  },
  assemble: {
    label: 'Sorting pages into documents',
    what: 'the text of pages it isn’t sure how to sort',
    none: 'Nothing connected: Lindley groups pages when its rules are sure, and you match the rest by hand.',
    short: 'sorting pages into documents',
  },
  chat: {
    label: 'Answering your questions in Ask Lindley',
    what: 'your questions and the text of your pages',
    none: 'Nothing connected: Ask Lindley only finds words, and what’s waiting for your review.',
    short: 'answering your questions',
  },
  embed: {
    label: 'Finding related pages',
    what: 'the text of your pages',
    none: 'Nothing connected: related pages are found by matching words only.',
    short: 'finding related pages',
  },
}

/** The companies your pages are sent to, given the jobs set up now. */
export function cloudInUse(settings: Settings | undefined, connectors: Connector[]): string[] {
  if (!settings) return []
  const out = new Set<string>()
  ;(Object.keys(JOB_WORDS) as Job[]).forEach((j) => {
    const c = forJob(settings, connectors, j)
    if (c?.cloud) out.add(c.company)
  })
  return [...out]
}

export const anyAi = (settings: Settings | undefined, connectors: Connector[]) =>
  (Object.keys(JOB_WORDS) as Job[]).some((j) => forJob(settings, connectors, j))

/** Who gets it: the company for a cloud AI, else the connection's name. */
export const who = (c: Conn | null) => (c ? (c.cloud ? c.company : c.label) : '')
