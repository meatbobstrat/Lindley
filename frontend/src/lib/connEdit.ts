// A connection being added or changed, in Settings or first-run setup, and what it saves as.

import type { Connector, ProviderConfig } from '../api/client'

export const LANGS: [string, string][] = [
  ['eng', 'English'],
  ['deu', 'German'],
  ['fra', 'French'],
  ['spa', 'Spanish'],
  ['nld', 'Dutch'],
  ['lat', 'Latin'],
]

export interface Edit {
  id: string | null // its name in ai.providers, once added
  type: string
  label: string
  base_url: string
  model: string
  key: string
  keyHint: string | null
  ack: boolean
  show: boolean
  tested: { ok: boolean; message: string } | null
  allow: 'ask' | 'auto'
  dailyOn: boolean
  daily: number
  monthlyOn: boolean
  monthly: number
  perMinOn: boolean
  perMin: number
  atOnce: number
  api_key_env: string | null
  timeout_s: number | null // not shown: kept as settings.json has it
}

export function newEdit(c: Connector | undefined): Edit {
  const local = c?.where !== 'cloud'
  return {
    id: null,
    type: c?.id ?? 'local',
    label: local ? 'AI on this computer' : (c?.short ?? 'Cloud AI'),
    base_url: c?.default_base_url ?? '',
    model: c?.default_models.chat ?? c?.default_models.vision ?? '',
    key: '',
    keyHint: null,
    ack: local,
    show: false,
    tested: null,
    allow: 'ask',
    dailyOn: !local,
    daily: 50,
    monthlyOn: !local,
    monthly: 1000,
    perMinOn: false,
    perMin: 30,
    atOnce: 2,
    api_key_env: null,
    timeout_s: null,
  }
}

export function editOf(id: string, cfg: ProviderConfig, keyHint: string | null): Edit {
  const capsFirst = cfg.allow !== 'auto' // a connection that asks gets the caps on if switched to run on its own
  return {
    id,
    type: cfg.type,
    label: cfg.label ?? id,
    base_url: cfg.base_url ?? '',
    model: cfg.model ?? '',
    key: '',
    keyHint,
    ack: true,
    show: false,
    tested: null,
    allow: cfg.allow,
    dailyOn: cfg.daily_limit != null || capsFirst,
    daily: cfg.daily_limit ?? 50,
    monthlyOn: cfg.monthly_limit != null || capsFirst,
    monthly: cfg.monthly_limit ?? 1000,
    perMinOn: cfg.per_minute != null,
    perMin: cfg.per_minute ?? 30,
    atOnce: cfg.at_once ?? 2,
    api_key_env: cfg.api_key_env ?? null,
    timeout_s: cfg.timeout_s ?? null,
  }
}

const whole = (v: number, lo: number, hi: number, d: number) => Math.min(hi, Math.max(lo, Math.round(v || d)))

export function cfgOf(e: Edit, c: Connector | undefined): ProviderConfig {
  const auto = e.allow === 'auto'
  return {
    type: e.type,
    label: e.label.trim() || (c?.short ?? e.type),
    base_url: e.base_url.trim() || null,
    model: e.model.trim() || null,
    api_key_env: e.api_key_env,
    allow: e.allow,
    daily_limit: auto && e.dailyOn ? whole(e.daily, 1, 100000, 50) : null,
    monthly_limit: auto && e.monthlyOn ? whole(e.monthly, 1, 1000000, 1000) : null,
    per_minute: e.perMinOn ? whole(e.perMin, 1, 1000, 30) : null,
    at_once: whole(e.atOnce, 1, 32, 2),
    timeout_s: e.timeout_s,
  }
}

export const editReady = (e: Edit, c: Connector | undefined) => !c?.needs_key || (e.ack && !!(e.key.trim() || e.keyHint || e.api_key_env))

/** A name for a new connection in ai.providers: from its label, not already taken. */
export function newId(label: string, taken: string[]): string {
  const base = label.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'ai'
  let id = base
  for (let n = 2; taken.includes(id); n++) id = `${base}-${n}`
  return id
}

