// How much AI this computer runs, and who does the rest (backend providers/tiers.py): each job's AI
// set from the two. The choice of AI for each job stays underneath, and changing one makes the
// settings a person's own.

import { type Connector, type HelpKind, JOBS, type Settings, type Tier } from '../api/client'
import { connection } from './ai'
import { cfgOf, newEdit, newId } from './connEdit'

/** What kind of help connection `name` can be: a cloud AI, or a server of your own (any AI that's
 * neither a company's nor Lindley's own). Null: it can't help. */
export function helpKind(settings: Settings, connectors: Connector[], name: string | null | undefined): HelpKind | null {
  const c = connection(settings, connectors, name)
  if (!c || c.cfg.type === 'builtin') return null
  return c.cloud ? 'cloud' : 'server'
}

/** The connections that can be help of this kind, the one helping now first. */
export function helpsOf(settings: Settings, connectors: Connector[], kind: HelpKind): string[] {
  if (kind === 'none') return []
  const names = Object.keys(settings.ai.providers).filter((n) => helpKind(settings, connectors, n) === kind)
  return [...names.filter((n) => n === settings.ai.help), ...names.filter((n) => n !== settings.ai.help)]
}

/** The settings with each job's AI set from the tier and the help: the tier's model on Lindley's
 * own AI (a connection to it is added if there's none), else the help if it can do the job, else
 * nothing. Whether a page carries on from another is never the help's: only Lindley's own AI is
 * sure to say how sure it is. */
export function withTier(settings: Settings, connectors: Connector[], tier: Tier, help: string | null): Settings {
  const next: Settings = structuredClone(settings)
  next.ai.tier = tier.id
  next.ai.help = help
  const names = () => Object.keys(next.ai.providers)
  let own = names().find((n) => next.ai.providers[n].type === 'builtin')
  const builtin = connectors.find((c) => c.id === 'builtin')
  if (!own && tier.downloads.length && builtin) {
    own = newId(builtin.short, names())
    // One question at a time: its server answers one at a time
    next.ai.providers[own] = cfgOf({ ...newEdit(builtin), label: builtin.short, atOnce: 1 }, builtin)
  }
  const can = (help && connectors.find((c) => c.id === next.ai.providers[help]?.type)?.jobs) || []
  JOBS.forEach((j) => {
    const model = tier.local[j]
    if (model && own) next.ai.jobs[j] = { connection: own, model }
    else if (help && can.includes(j) && j !== 'continues') next.ai.jobs[j] = { connection: help, model: null }
    else next.ai.jobs[j] = { connection: null, model: null }
  })
  return next
}
