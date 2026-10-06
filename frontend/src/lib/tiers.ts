// Performance tiers (backend providers/tiers.py): each job's AI set from how much the computer can
// run. The choice of AI for each job stays underneath, and changing one makes it a person's own.

import { type Connector, JOBS, type Settings, type Tier } from '../api/client'
import { connection } from './ai'
import { cfgOf, newEdit, newId } from './connEdit'

/** Whether connection `name` is one the tier can run on. */
export function fits(settings: Settings, connectors: Connector[], name: string, tier: Tier): boolean {
  const c = connection(settings, connectors, name)
  if (!c) return false
  if (tier.runs === 'this') return c.cfg.type === 'local' && c.reach === 'this'
  if (tier.runs === 'network') return !c.cloud && c.reach !== 'this'
  return tier.runs === 'cloud' && c.cloud
}

/** The connections a tier can run on, the ones its jobs use now first. */
export function fitting(settings: Settings, connectors: Connector[], tier: Tier): string[] {
  const used = new Set(JOBS.map((j) => settings.ai.jobs[j]?.connection))
  const names = Object.keys(settings.ai.providers).filter((n) => fits(settings, connectors, n, tier))
  return [...names.filter((n) => used.has(n)), ...names.filter((n) => !used.has(n))]
}

/** The settings with each job's AI set from the tier, on connection `name` (else the first that
 * fits; a tier on this computer adds one if there's none). Null: it needs a connection that isn't
 * there yet, a server or a cloud AI. */
export function withTier(settings: Settings, connectors: Connector[], tier: Tier, name?: string): Settings | null {
  const next: Settings = structuredClone(settings)
  JOBS.forEach((j) => (next.ai.jobs[j] = { connection: null, model: null }))
  next.ai.tier = tier.id
  if (tier.runs === 'none') return next
  let use = name ?? fitting(settings, connectors, tier)[0]
  if (!use) {
    const local = connectors.find((c) => c.id === 'local')
    if (tier.runs !== 'this' || !local) return null
    use = newId(local.short, Object.keys(next.ai.providers))
    next.ai.providers[use] = cfgOf(newEdit(local), local)
  }
  const can = connectors.find((c) => c.id === next.ai.providers[use].type)?.jobs ?? []
  JOBS.forEach((j) => {
    if (!can.includes(j)) return
    if (tier.runs !== 'this') next.ai.jobs[j] = { connection: use, model: null }
    else if (tier.models[j]) next.ai.jobs[j] = { connection: use, model: tier.models[j]! }
  })
  return next
}

/** The models a tier on this computer uses, each once. */
export const tierModels = (tier: Tier) => [...new Set(Object.values(tier.models))]
