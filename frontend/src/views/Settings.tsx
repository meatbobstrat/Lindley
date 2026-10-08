// Settings: AI connections and privacy, folders, reading and review, the library, and how Lindley
// looks. Changes are a draft until you save them. API keys never go in settings.json: they're
// saved in the system's credential store, after the settings that name their connection.

import { useEffect, useMemo, useRef, useState } from 'react'
import { useBlocker, useNavigate, useParams } from 'react-router'
import { api, type AiCalls, type Computer, type Connector, type Help, type HelpKind, JOBS, type Settings, type Tier } from '../api/client'
import { invalidate, useApi } from '../api/store'
import { Banner, LimitsTag, Loading, Meter, PrivTag } from '../components/bits'
import { cloudInUse, companyOf, connection, hostOf, JOB_WORDS, reachOf } from '../lib/ai'
import { useApp, useLooking } from '../lib/appContext'
import { cfgOf, type Edit, editOf, editReady, LANGS, newEdit, newId } from '../lib/connEdit'
import { helpKind, helpsOf, withTier } from '../lib/tiers'
import { useDockPos, useTheme } from '../lib/view'
import { dollars, duration, plural, size } from '../lib/words'
import { DBtn, Dock, DockText } from '../ui/Dock'
import { Modal } from '../ui/feedback'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, type IconName } from '../ui/icons'

const SECTIONS: [string, string, IconName][] = [
  ['ai', 'AI and privacy', 'lock'],
  ['scans', 'Scans', 'inbox'],
  ['reading', 'Reading and review', 'flag'],
  ['library', 'Library', 'folder'],
  ['look', 'Appearance', 'sun'],
]
const clone = <T,>(o: T): T => JSON.parse(JSON.stringify(o))

export function ConnEditor({
  e,
  set,
  connectors,
  inSetup,
  saved,
  onSave,
  onCancel,
}: {
  e: Edit
  set: (e: Edit) => void
  connectors: Connector[]
  inSetup?: boolean
  saved: boolean // the connection is in the saved settings, so its saved key can be tried
  onSave?: () => void
  onCancel?: () => void
}) {
  const ctx = inSetup ? 'su' : 'st'
  const c = connectors.find((x) => x.id === e.type)
  const cloud = c?.where === 'cloud'
  const cfg = cfgOf(e, c)
  const co = companyOf(cfg, c)
  const reach = reachOf(cfg, c)
  const ok = editReady(e, c)
  const [testing, setTesting] = useState(false)
  const up = (p: Partial<Edit>) => set({ ...e, ...p, tested: 'tested' in p ? (p.tested ?? null) : null })
  const who = cloud ? co : 'this AI'
  const own = e.type === 'builtin'

  // A test takes a while: what's typed meanwhile stays, and a result for a connection since
  // changed, or closed, is dropped
  const latest = useRef(e)
  const open = useRef(true)
  useEffect(() => {
    latest.current = e
  })
  useEffect(() => {
    open.current = true
    return () => {
      open.current = false
    }
  }, [])
  const asTried = (x: Edit) => JSON.stringify([x.id, cfgOf(x, connectors.find((k) => k.id === x.type)), x.key])
  const test = async () => {
    setTesting(true)
    const tried = asTried(e)
    const result = (tested: Edit['tested']) => {
      if (open.current && asTried(latest.current) === tried) set({ ...latest.current, tested })
    }
    try {
      const r = await api.testConnection(cfg, saved ? e.id : null, e.key.trim() || null)
      result({ ok: r.ok, message: r.message })
    } catch (err) {
      result({ ok: false, message: (err as Error).message })
    } finally {
      if (open.current) setTesting(false)
    }
  }

  const fld = (id: string, label: string, value: string, onChange: (v: string) => void, extra: Record<string, unknown> = {}) => (
    <>
      <label className="fld" htmlFor={`${ctx}-${id}`}>
        {label}
      </label>
      <input className="fld-in" id={`${ctx}-${id}`} value={value} onChange={(ev) => onChange(ev.target.value)} autoComplete="off" spellCheck={false} {...extra} />
    </>
  )

  return (
    <div className="conn-ed" id={`${ctx}-ed`}>
      {!inSetup && <h3>{e.id ? `Change ${e.label}` : 'Add an AI connection'}</h3>}
      <label className="fld" htmlFor={`${ctx}-vendor`}>
        Service
      </label>
      <select
        className="fld-in"
        id={`${ctx}-vendor`}
        value={e.type}
        aria-describedby={`${ctx}-vendorh`}
        onChange={(ev) => set({ ...newEdit(connectors.find((x) => x.id === ev.target.value)), id: e.id, help: e.help })}
        data-tip="Which AI service to connect to. Each is one connector file in Lindley’s providers folder."
      >
        {/* Lindley's own AI comes with a tier, not from here */}
        {connectors.filter((x) => x.id !== 'builtin' || x.id === e.type).map((x) => (
          <option key={x.id} value={x.id}>
            {x.label}
          </option>
        ))}
      </select>
      <p className="fld-note" id={`${ctx}-vendorh`}>
        {cloud ? (
          <>
            <Icon name="cloud" /> Not private: every page and question this AI handles is sent to {co}.
          </>
        ) : (
          <>
            <Icon name="lock" /> Private: your scans stay on computers you control.
          </>
        )}
      </p>
      {fld('name', 'Name', e.label, (v) => up({ label: v }), { 'data-tip': 'What Lindley calls this connection in Settings and the status bar' })}
      {(!cloud || e.type === 'openai_compat') && (
        <>
          {fld('url', 'Address', e.base_url, (v) => up({ base_url: v }), { 'aria-describedby': `${ctx}-urlh`, placeholder: c?.default_base_url ?? 'https://' })}
          <p className="fld-note" id={`${ctx}-urlh`}>
            {cloud ? 'The service’s OpenAI-compatible address.' : <>For Ollama on this computer it’s usually <span className="mono">http://localhost:11434/v1</span>. LM Studio uses port 1234.</>}
          </p>
          {!cloud && reach === 'server' && (
            <div className="rv-state flag" role="note">
              <Icon name="warn" />
              <div>
                <b>This address isn’t on your own network.</b> Your pages will travel over the internet to {hostOf(e.base_url)}. Use it only if you or your
                organization run that server.
              </div>
            </div>
          )}
        </>
      )}
      {fld('model', 'Model', e.model, (v) => up({ model: v }), {
        placeholder: c?.default_models.chat ?? '',
        'data-tip': 'The model to use for every job but finding related pages. Leave it empty for the service’s usual one.',
      })}
      {cloud && (
        <div className="pwarn" role="note" aria-labelledby={`${ctx}-wh`}>
          <h4 id={`${ctx}-wh`}>
            <Icon name="warn" />
            <span>Warning: your scans will not be private</span>
          </h4>
          <ul>
            <li>Every page you ask this AI to read, and every question you ask about your documents, is sent over the internet to {co}’s computers.</li>
            <li>Once it’s sent, you can’t take it back. {co}’s own policies, not Lindley’s, decide how long it’s kept and who can see it.</li>
            <li>
              Don’t use a cloud AI for scans with private information: medical, legal or financial records, adoption papers, or anything about living people who
              haven’t agreed to it.
            </li>
            <li>To keep your scans private, use an AI on a computer you own instead: Lindley’s own (a Middle or High tier), or Ollama or LM Studio.</li>
          </ul>
          <label className="ack">
            <input type="checkbox" checked={e.ack} onChange={(ev) => up({ ack: ev.target.checked })} />
            <span>I understand my scans will be sent to {co} and are not private.</span>
          </label>
        </div>
      )}
      {(cloud || c?.needs_key || !inSetup) && (
        <>
          <label className="fld" htmlFor={`${ctx}-key`}>
            {cloud ? `API key from ${co}` : 'API key (optional)'}
          </label>
          <div className="keyrow">
            <input
              className="fld-in"
              id={`${ctx}-key`}
              type={e.show ? 'text' : 'password'}
              value={e.key}
              onChange={(ev) => up({ key: ev.target.value })}
              autoComplete="off"
              spellCheck={false}
              disabled={cloud && !e.ack}
              aria-describedby={`${ctx}-keyh`}
              placeholder={e.keyHint ? `Leave empty to keep the saved key ${e.keyHint}` : e.api_key_env ? `From the ${e.api_key_env} environment variable` : 'Paste your key here'}
            />
            <button className="btn" disabled={cloud && !e.ack} aria-pressed={e.show} aria-label="Show key" onClick={() => set({ ...e, show: !e.show })} data-tip={e.show ? 'Hide the key' : 'Show the key you typed'}>
              {e.show ? 'Hide' : 'Show'}
            </button>
          </div>
          <p className="fld-note" id={`${ctx}-keyh`}>
            {cloud && !e.ack
              ? 'Read the warning and check the box above before you enter a key.'
              : 'Lindley keeps your key in this computer’s credential store (Windows Credential Manager, the Keychain on a Mac, the keyring on Linux), never in its settings file, and never shows it again after you save it.'}
            {c?.key_url && (
              <>
                {' '}
                <a href={c.key_url} target="_blank" rel="noreferrer" data-tip={`Opens ${c.company}’s page for API keys in your browser`}>
                  Get a key from {c.company}
                </a>
                .
              </>
            )}
          </p>
        </>
      )}
      <div className="lim-grp">
        <h4 className="lim-h">Limits</h4>
        <fieldset className="choices">
          <legend className="fld">When may Lindley use {cloud ? co : 'it'}?</legend>
          <label className="choice">
            <input type="radio" name={`${ctx}-allow`} checked={e.allow === 'ask'} onChange={() => up({ allow: 'ask' })} />
            <span>
              <b>Ask me first.</b> {own ? '' : 'Recommended. '}Hard pages, and pages Lindley isn’t sure how to sort, wait in Needs AI until you send them. Nothing
              goes to {who} without you.
            </span>
          </label>
          <label className="choice">
            <input type="radio" name={`${ctx}-allow`} checked={e.allow === 'auto'} onChange={() => up({ allow: 'auto' })} />
            <span>
              <b>Whenever it’s needed.</b> {own ? 'Recommended: it runs on this computer, so nothing is sent anywhere or paid for. ' : ''}Hard pages go to {who}{' '}
              as soon as they’re read, and it helps sort pages into documents as they arrive.
            </span>
          </label>
        </fieldset>
        {e.allow === 'auto' && (
          <>
            {(
              [
                ['daily', 'dailyOn', 100000, 'calls a day'],
                ['monthly', 'monthlyOn', 1000000, 'calls a month'],
              ] as const
            ).map(([k, on, max, unit]) => (
              <div className="limit-row" key={k}>
                <label className="limit-check">
                  <input type="checkbox" checked={e[on]} onChange={(ev) => up({ [on]: ev.target.checked } as Partial<Edit>)} />
                  <span>Stop after</span>
                </label>
                <input
                  type="number"
                  className="fld-in limit-in"
                  min={1}
                  max={max}
                  value={e[k]}
                  disabled={!e[on]}
                  aria-label={unit}
                  onChange={(ev) => up({ [k]: Number(ev.target.value) } as Partial<Edit>)}
                  aria-describedby={`${ctx}-capsh`}
                />
                <span aria-hidden="true">{unit}, then ask me</span>
              </div>
            ))}
            <p className="fld-note" id={`${ctx}-capsh`}>
              {cloud
                ? `${co} charges for every call. One call is one page read, or one question about how pages go together.`
                : 'An AI on your own computer costs nothing to call, but a limit keeps a slow computer from being kept busy.'}
            </p>
          </>
        )}
        <p className="fld-note">
          <Icon name="send" /> Questions you type in Ask Lindley are always sent: asking is your OK.
        </p>
        {inSetup ? (
          <p className="fld-note">You can set more limits in Settings: calls a minute, and calls at once.</p>
        ) : (
          <>
            <p className="fld lim-sub">For every call, including the ones you OK</p>
            <div className="limit-row">
              <label className="limit-check">
                <input type="checkbox" checked={e.perMinOn} onChange={(ev) => up({ perMinOn: ev.target.checked })} />
                <span>At most</span>
              </label>
              <input type="number" className="fld-in limit-in" min={1} max={1000} value={e.perMin} disabled={!e.perMinOn} aria-label="Calls a minute" onChange={(ev) => up({ perMin: Number(ev.target.value) })} />
              <span aria-hidden="true">calls a minute</span>
            </div>
            <div className="limit-row">
              <span aria-hidden="true">At most</span>
              <input type="number" className="fld-in limit-in" min={1} max={32} value={e.atOnce} aria-label="Calls at once" onChange={(ev) => up({ atOnce: Number(ev.target.value) })} />
              <span aria-hidden="true">calls at once</span>
            </div>
            <p className="fld-note">
              {cloud ? `Keeps Lindley under ${co}’s rate limits.` : 'Keeps a slow computer usable while it reads.'} If {who} says it’s busy, Lindley waits and tries
              again, a few times.
            </p>
          </>
        )}
      </div>
      <div className="actions conn-acts">
        <button className="btn" disabled={!ok || testing} onClick={test} data-tip={ok ? 'Make one small call to check the address, the key and the model' : 'Fill in the connection first'}>
          {testing ? 'Testing…' : 'Test connection'}
        </button>
        <span className="test-res" role="status">
          {e.tested && (
            <>
              <Icon name={e.tested.ok ? 'checkc' : 'warn'} />
              {e.tested.message}
            </>
          )}
        </span>
        {!inSetup && (
          <>
            <span className="spacer" />
            <button className="btn" onClick={onCancel} data-tip="Close without adding or changing the connection">
              Cancel
            </button>
            <button className="btn primary" disabled={!ok} onClick={onSave} data-tip={ok ? 'Put it in your settings. Save your changes afterwards to keep it.' : 'Fill in the connection first'}>
              {e.id ? 'Update connection' : 'Add connection'}
            </button>
          </>
        )}
      </div>
    </div>
  )
}

/** A folder path as people copy it: File Explorer's Copy as path puts it in quotes. */
const cleanPath = (p: string) => p.trim().replace(/^"(.*)"$/, '$1').trim()

/** The folders Lindley can't find, of these: a path typed wrong, or a drive unplugged. */
function useMissing(folders: string[]): Set<string> {
  const found = useApi(folders.length ? `folders:${JSON.stringify(folders)}` : null, () => api.checkFolders(folders)).data
  return new Set((found?.folders ?? []).filter((f) => !f.found).map((f) => f.path))
}

/** Watched folders. A browser can't open a folder picker that gives a path, so they're typed. */
export function FoldersEditor({ ctx, folders, set }: { ctx: string; folders: string[]; set: (f: string[]) => void }) {
  const [path, setPath] = useState('')
  const missing = useMissing(folders)
  const add = () => {
    const v = cleanPath(path)
    if (v && !folders.includes(v)) set([...folders, v])
    setPath('')
  }
  return (
    <>
      <ul className="setup-folders">
        {folders.length ? (
          folders.map((x, i) => (
            <li key={x}>
              <Icon name="folder" />
              <span className="mono">{x}</span>
              {missing.has(x) && (
                <span className="chip missing" data-tip="Lindley can’t find this folder. Check the path, or plug in the drive it’s on: Lindley watches it once it’s there.">
                  <Icon name="warn" /> Not found
                </span>
              )}
              <button className="btn ghost" aria-label={`Stop watching ${x}`} data-tip="Stop watching this folder. Scans already in Lindley stay." onClick={() => set(folders.filter((_, j) => j !== i))}>
                Remove
              </button>
            </li>
          ))
        ) : (
          <li>No folders yet. Add at least one.</li>
        )}
      </ul>
      <div className="setup-add">
        <label className="sr-only" htmlFor={`${ctx}-path`}>
          Folder path
        </label>
        <input
          className="fld-in"
          id={`${ctx}-path`}
          value={path}
          onChange={(e) => setPath(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              add()
            }
          }}
          placeholder="Type a folder path, for example E:\Scans"
          data-tip="The full path of a folder your scanner saves into. Copy it from File Explorer’s address bar."
        />
        <button className="btn" onClick={add} disabled={!path.trim()} data-tip="Watch this folder for new scans">
          Add
        </button>
      </div>
    </>
  )
}

export function ModeChoices({ ctx, move, set }: { ctx: string; move: boolean; set: (m: boolean) => void }) {
  return (
    <fieldset className="choices">
      <legend className="sr-only">When a scan arrives</legend>
      <label className="choice">
        <input type="radio" name={`${ctx}-mode`} checked={!move} onChange={() => set(false)} />
        <span>
          <b>Copy it into Lindley</b> and leave my folder untouched. Recommended: your scan folder keeps every file.
        </span>
      </label>
      <label className="choice">
        <input type="radio" name={`${ctx}-mode`} checked={move} onChange={() => set(true)} />
        <span>
          <b>Move it into Lindley</b> and empty my scan folder. Handy when the folder is only a drop-off spot for the scanner. A file is removed only once
          Lindley’s copy is checked.
        </span>
      </label>
    </fieldset>
  )
}

/** How much AI this computer runs: Low, Middle or High. */
export function TierChoices({
  ctx,
  tiers,
  value,
  onChoose,
  computer,
}: {
  ctx: string
  tiers: Tier[]
  value: string | null
  onChoose: (t: Tier) => void
  computer?: Computer
}) {
  return (
    <fieldset className="choices">
      <legend className="sr-only">How much AI this computer runs</legend>
      {tiers.map((t) => {
        const time = computer?.times[t.id]
        return (
          <label className="choice" key={t.id}>
            <input type="radio" name={`${ctx}-tier`} checked={value === t.id} onChange={() => onChoose(t)} />
            <span>
              <b>
                {t.label}
                {computer?.suggested === t.id && ' (suggested for this computer)'}.
              </b>{' '}
              {t.needs} {t.does}
              {time && (
                <i>
                  {' '}
                  100 typed pages: {duration(time.typed)} here.{' '}
                  {time.handwritten === null ? 'Handwriting goes to the help, or waits for you.' : `100 handwritten: ${duration(time.handwritten)}.`}
                </i>
              )}
            </span>
          </label>
        )
      })}
    </fieldset>
  )
}

/** What Lindley found this computer has, and the tier it suggests. */
export function ComputerNote({ computer: c, tiers }: { computer: Computer; tiers: Tier[] }) {
  const gb = (n: number) => `${Math.round(n / 2 ** 30)} GB`
  const parts = [
    c.processor ? `${c.processor} (${plural(c.threads, 'thread')})` : plural(c.threads, 'processor thread'),
    c.memory !== null && `${gb(c.memory)} of memory`,
    c.graphics.length ? c.graphics.map((g) => `${g.name} (${gb(g.memory)})`).join(', ') : 'no graphics card',
    c.free !== null && `${size(c.free)} free`,
  ].filter(Boolean)
  const label = tiers.find((t) => t.id === c.suggested)?.label ?? c.suggested
  return (
    <p className="fld-note">
      This computer: {parts.join(', ')}. Lindley suggests <b>{label}</b>. {c.why} Times are estimates, measured on {c.measured_on}; yours may differ.
    </p>
  )
}

/** Who does the jobs the tier leaves: nobody, your own AI server, or a cloud AI. */
export function HelpChoices({ ctx, helps, value, onChoose }: { ctx: string; helps: Help[]; value: HelpKind | null; onChoose: (h: Help) => void }) {
  return (
    <fieldset className="choices">
      <legend className="sr-only">Who does the rest</legend>
      {helps.map((h) => (
        <label className="choice" key={h.id}>
          <input type="radio" name={`${ctx}-help`} checked={value === h.id} onChange={() => onChoose(h)} />
          <span>
            <b>
              {h.id !== 'none' && <Icon name={h.id === 'cloud' ? 'cloud' : 'lock'} />}
              {h.label}.
            </b>{' '}
            {h.does}
          </span>
        </label>
      ))}
    </fieldset>
  )
}

/** What Lindley's own AI needs downloaded for a tier, and downloading it. With `setNow` (first-run
 * setup), a box says whether to download it once setup is saved. */
export function LocalModels({ tier, now, setNow }: { tier: Tier; now?: boolean; setNow?: (v: boolean) => void }) {
  const { toast } = useFeedback()
  const s = useApi(tier.downloads.length ? 'local-ai' : null, api.localAi).data
  if (!tier.downloads.length || !s) return null
  const models = tier.downloads.flatMap((id) => s.models.filter((m) => m.id === id))
  const names = models.map((m) => m.label).join(' and ')
  const left = models.filter((m) => m.state !== 'ready')
  const need = left.reduce((n, m) => n + m.size, 0) + (s.engine.ready ? 0 : s.engine.size)
  if (!need)
    return (
      <p className="fld-note">
        <Icon name="lock" /> {names} {models.length === 1 ? 'is' : 'are'} downloaded, and run on this computer.
        {s.on_processor && ' This computer’s graphics couldn’t run it, so it runs on the processor alone: slower, but the same answers.'}
      </p>
    )
  const room = s.free === null || s.free > need
  const d = s.downloading
  const what = left.map((m) => `${m.label} (${size(m.size)})`).join(' and ') + (s.engine.ready ? '' : ` and Lindley’s AI engine (${size(s.engine.size)})`)
  const get = async () => {
    try {
      await api.downloadModels(tier.downloads)
      invalidate()
      toast(`Downloading ${names}. The status bar shows how far it’s got, and you can keep working.`)
    } catch (e) {
      toast((e as Error).message)
    }
  }
  return (
    <div className="fld-note">
      <p>
        <Icon name="lock" /> {tier.label} runs Lindley’s own AI on this computer. It needs {what}, downloaded once from Hugging Face and GitHub; then it works
        offline.{s.free !== null && ` ${size(s.free)} is free on that disk.`}
      </p>
      {!room && (
        <p>
          <Icon name="warn" /> There isn’t room for it. Free up {size(need - (s.free ?? 0))} first.
        </p>
      )}
      {d ? (
        <p>
          <Meter value={Math.floor((d.done * 100) / Math.max(1, d.of))} label={`Downloading ${d.label}`} tip={`${size(d.done)} of ${size(d.of)}`} />{' '}
          <button
            className="btn ghost"
            onClick={async () => {
              await api.cancelDownload()
              invalidate()
            }}
            data-tip="Stop downloading. What came down so far is kept, and the next download goes on from there"
          >
            Cancel
          </button>
        </p>
      ) : setNow ? (
        <label className="limit-check">
          <input type="checkbox" checked={!!now} onChange={(e) => setNow(e.target.checked)} />
          <span>Download {size(need)} when I start using Lindley</span>
        </label>
      ) : (
        <button className="btn" onClick={get} disabled={!room} data-tip={`Download ${size(need)} now. You can keep working while it does`}>
          <Icon name="plus" /> Download {size(need)}
        </button>
      )}
    </div>
  )
}

// ---------------------------------------------------------------- The Settings screen

export function SettingsView() {
  const { settings } = useApp()
  useLooking('Settings')
  return settings ? <SettingsScreen settings={settings} /> : <Loading />
}

function SettingsScreen({ settings }: { settings: Settings }) {
  const section = useParams().section ?? 'ai'
  const { connectors, overview } = useApp()
  const calls = useApi('ai-calls', api.aiCalls)
  const { toast } = useFeedback()
  const nav = useNavigate()
  const [draft, setDraft] = useState<Settings>(() => clone(settings))
  const [keys, setKeys] = useState<Record<string, string>>({})
  const [edit, setEdit] = useState<Edit | null>(null)
  const [saving, setSaving] = useState(false)

  const changes = useMemo(() => {
    const keysOf = (o: Settings) => Object.keys(o) as (keyof Settings)[]
    return keysOf(draft).filter((k) => JSON.stringify(draft[k]) !== JSON.stringify(settings[k])).length + Object.keys(keys).length
  }, [draft, settings, keys])
  const dirty = changes > 0 || !!edit
  const blocker = useBlocker(({ currentLocation, nextLocation }) => dirty && !nextLocation.pathname.startsWith('/settings') && currentLocation.pathname !== nextLocation.pathname)

  const sec = SECTIONS.find((s) => s[0] === section) ?? SECTIONS[0]

  // `dropEdit`: the connection being added is left out (leaving Settings: "Save and continue")
  const save = async ({ dropEdit = false } = {}): Promise<boolean> => {
    if (edit && !dropEdit) {
      toast('Finish the connection you’re adding first: add it or cancel it.')
      document.getElementById('st-ed')?.scrollIntoView({ block: 'center' })
      return false
    }
    if (!draft.watch_folders.length) {
      toast('Add at least one folder for Lindley to watch.')
      nav('/settings/scans')
      return false
    }
    if (!draft.ocr.languages.length) {
      toast('Choose at least one language for reading printed pages.')
      nav('/settings/reading')
      return false
    }
    setSaving(true)
    const before = cloudInUse(settings, connectors)
    try {
      const saved = await api.saveSettings(draft)
      for (const [name, key] of Object.entries(keys)) if (saved.ai.providers[name]) await api.saveKey(name, key)
      setKeys({})
      setDraft(clone(saved))
      invalidate()
      const now = cloudInUse(saved, connectors).filter((x) => !before.includes(x))
      toast(`Settings saved.${now.length ? ` From now on your scans are sent to ${now.join(' and ')}.` : ''}`)
      return true
    } catch (e) {
      toast((e as Error).message)
      return false
    } finally {
      setSaving(false)
    }
  }
  const discard = () => {
    setDraft(clone(settings))
    setKeys({})
    setEdit(null)
    toast('Changes discarded.')
  }

  const body = (() => {
    switch (sec[0]) {
      case 'ai':
        return <SetAi d={draft} set={setDraft} keys={keys} setKeys={setKeys} edit={edit} setEdit={setEdit} calls={calls.data} saved={settings} />
      case 'scans':
        return <SetScans d={draft} set={setDraft} />
      case 'reading':
        return <SetReading d={draft} set={setDraft} reviewNow={overview?.counts.review} saved={settings} />
      case 'library':
        return <SetLibrary d={draft} set={setDraft} />
      default:
        return <SetLook />
    }
  })()

  return (
    <>
      <div className="head">
        <div className="crumbs">Settings › {sec[1]}</div>
        <div className="title-row">
          <h1 className="title">Settings</h1>
        </div>
        <p className="sub">Changes take effect when you save them with the toolbar below.</p>
        <div className="set-pick">
          <label className="fld" htmlFor="set-sec">
            Section
          </label>
          <select className="fld-in" id="set-sec" value={sec[0]} onChange={(e) => nav(`/settings/${e.target.value}`)}>
            {SECTIONS.map(([k, l]) => (
              <option key={k} value={k}>
                {l}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="set">
        <nav className="set-nav" aria-label="Settings sections">
          {SECTIONS.map(([k, l, ic]) => (
            <button
              key={k}
              className={k === sec[0] ? 'is-active' : undefined}
              aria-current={k === sec[0] ? 'page' : undefined}
              onClick={() => nav(`/settings/${k}`)}
              data-tip={
                {
                  ai: 'Which AIs Lindley may use, for what, and who sees your scans',
                  scans: 'The folders Lindley watches, and what happens to new files',
                  reading: 'When pages wait for your review, and how printed and handwritten pages are read',
                  library: 'Where Lindley keeps its copies, page images, PDFs and database',
                  look: 'Theme and toolbar',
                }[k]
              }
            >
              <Icon name={ic} />
              {l}
            </button>
          ))}
        </nav>
        <div className="scroll set-body">
          <section className="set-sec" aria-labelledby="set-h">
            <h2 id="set-h">{sec[1]}</h2>
            {body}
          </section>
        </div>
      </div>
      <Dock label="Settings actions">
        <DockText>{changes ? plural(changes, 'unsaved change') : 'No unsaved changes'}</DockText>
        <DBtn icon="back" label="Discard changes" disabled={!dirty} tip={dirty ? 'Put the settings back as they were saved' : 'Nothing to discard'} onClick={discard} />
        <DBtn icon="check" label={saving ? 'Saving…' : 'Save changes'} kind="primary" disabled={!changes || saving} tip={changes ? 'Save your changes. They take effect at once.' : 'Nothing to save'} onClick={() => save()} />
      </Dock>
      {blocker.state === 'blocked' && (
        <Modal title="Save your settings?" onClose={() => blocker.reset()}>
          <p>
            {changes ? `You have ${plural(changes, 'unsaved change')}. Changes only take effect once you save them.` : ''}
            {edit ? ` ${changes ? 'You are also' : 'You’re'} partway through adding an AI connection, which will be dropped.` : ''}
          </p>
          <div className="actions">
            <button className="btn" onClick={() => blocker.reset()} data-tip="Stay in Settings">
              Keep editing
            </button>
            <button
              className="btn"
              onClick={() => {
                setDraft(clone(settings))
                setKeys({})
                setEdit(null)
                blocker.proceed()
              }}
              data-tip="Leave without saving"
            >
              Discard changes
            </button>
            {changes > 0 && (
              <button
                className="btn primary"
                autoFocus
                onClick={async () => {
                  if (await save({ dropEdit: true })) {
                    setEdit(null)
                    blocker.proceed()
                  } else blocker.reset()
                }}
                data-tip="Save, then go where you were going"
              >
                Save and continue
              </button>
            )}
          </div>
        </Modal>
      )}
    </>
  )
}

type SetProps = { d: Settings; set: (s: Settings) => void }

function SetAi({
  d,
  set,
  keys,
  setKeys,
  edit,
  setEdit,
  calls,
  saved,
}: SetProps & {
  keys: Record<string, string>
  setKeys: (k: Record<string, string>) => void
  edit: Edit | null
  setEdit: (e: Edit | null) => void
  calls: AiCalls | undefined
  saved: Settings
}) {
  const { connectors, tiers, helps } = useApp()
  const { toast } = useFeedback()
  const cloud = cloudInUse(d, connectors)
  const names = Object.keys(d.ai.providers)
  const conns = names.map((n) => connection(d, connectors, n)!).filter(Boolean)
  const jobsOf = (n: string) => JOBS.filter((j) => d.ai.jobs[j]?.connection === n).map((j) => JOB_WORDS[j].short)
  const any = JOBS.some((j) => connection(d, connectors, d.ai.jobs[j]?.connection))
  // Lindley's own AI's models: their names, and which can do each job
  const own = useApi('local-ai', api.localAi).data
  const computer = useApi('computer', api.computer).data
  const ownModel = (id: string) => own?.models.find((m) => m.id === id)
  const modelsOf = (n: string) =>
    [...new Set(JOBS.filter((j) => d.ai.jobs[j]?.connection === n).map((j) => d.ai.jobs[j]?.model || connection(d, connectors, n)?.connector?.default_models[j] || ''))]
      .filter(Boolean)
      .map((id) => ownModel(id)?.label ?? id)
      .join(' and ')

  // The two choices shown chosen: the tier saved (with no AI at all, that's Low), and the help:
  // one waiting for its connection to be added, else the saved one. Neither once a person has
  // changed a job's AI.
  const tierNow = tiers.find((t) => t.id === (d.ai.tier ?? (any ? null : 'low')))
  const helpNow: HelpKind | null = edit?.help ?? (d.ai.help ? helpKind(d, connectors, d.ai.help) : d.ai.tier || !any ? 'none' : null)
  const helpers = helpNow && helpNow !== 'none' ? helpsOf(d, connectors, helpNow) : []
  const base = tierNow ?? tiers[0] // a help chosen before a tier goes with Low
  const chooseTier = (t: Tier) => {
    set(withTier(d, connectors, t, d.ai.help))
    toast(`${t.label}: each job’s AI is set below. Save your changes to use it.`)
  }
  const chooseHelp = (h: Help) => {
    if (edit && !edit.help) {
      toast('Finish the connection you’re adding first: add it or cancel it.')
      document.getElementById('st-ed')?.scrollIntoView({ block: 'center' })
      return
    }
    const fit = h.id === 'none' ? [] : helpsOf(d, connectors, h.id)
    if (h.id === 'none' || fit.length) {
      set(withTier(d, connectors, base, fit[0] ?? null))
      setEdit(null)
      toast(`${h.id === 'none' ? 'Nobody else does the rest' : `${connection(d, connectors, fit[0])?.label} does the rest`}: each job’s AI is set below. Save your changes to use it.`)
      return
    }
    // A server or a cloud AI must be added first: it's the help once it is
    const c = h.id === 'cloud' ? connectors.find((x) => x.where === 'cloud') : connectors.find((x) => x.id === 'local')
    setEdit({ ...newEdit(c), ...(h.id === 'server' ? { label: 'Your AI server', base_url: '' } : {}), help: h.id })
    toast(h.id === 'cloud' ? 'Add a cloud AI below, with its key, to use it.' : 'Add your AI server below: type its address.')
  }

  const commitEdit = () => {
    if (!edit) return
    if (edit.help === 'server' && !edit.base_url.trim()) {
      toast('Type your AI server’s address, for example http://192.168.1.20:11434/v1.')
      document.getElementById('st-url')?.focus()
      return
    }
    const c = connectors.find((x) => x.id === edit.type)
    const id = edit.id ?? newId(edit.label || c?.short || 'ai', names)
    let next = clone(d)
    next.ai.providers[id] = cfgOf(edit, c)
    if (edit.help) next = withTier(next, connectors, base, id)
    else if (!edit.id)
      // A first connection takes on every job that had nothing
      JOBS.forEach((j) => {
        if (!next.ai.jobs[j]?.connection && c?.jobs.includes(j) && j !== 'continues') {
          next.ai.jobs[j] = { connection: id, model: null }
          next.ai.tier = null
        }
      })
    set(next)
    if (edit.key.trim()) setKeys({ ...keys, [id]: edit.key.trim() })
    setEdit(null)
    toast(
      edit.help
        ? `Added ${edit.label}, to do the rest. Save your changes to use it.`
        : `${edit.id ? 'Updated' : 'Added'} ${edit.label}. Choose which jobs it does, then save your changes.`,
    )
  }
  const remove = (n: string) => {
    const next = clone(d)
    delete next.ai.providers[n]
    if (next.ai.help === n) next.ai.help = null
    JOBS.forEach((j) => {
      if (next.ai.jobs[j]?.connection !== n) return
      next.ai.jobs[j] = { connection: null, model: null }
      next.ai.tier = null
    })
    const k = { ...keys }
    delete k[n]
    setKeys(k)
    set(next)
    if (edit?.id === n) setEdit(null)
    toast(`Removed ${d.ai.providers[n].label ?? n}. Its key is deleted from the credential store when you save. Save your changes to confirm.`)
  }

  return (
    <>
      {cloud.length ? (
        <Banner kind="danger" role="note">
          <b>Not private. Your scans are sent to {cloud.join(' and ')}.</b> That happens for{' '}
          {JOBS.filter((j) => connection(d, connectors, d.ai.jobs[j]?.connection)?.cloud)
            .map((j) => JOB_WORDS[j].short)
            .join(', ')}
          . Anything you’d rather keep to yourself shouldn’t go to a cloud AI.
        </Banner>
      ) : any ? (
        <Banner kind="ok" icon="lock" role="note">
          <b>Private. Your scans stay on computers you control.</b> No page or question is sent to an AI company.
        </Banner>
      ) : (
        <Banner kind="warn" role="note">
          <b>No AI connected.</b> Tesseract still reads printed pages on this computer, and Lindley groups pages into documents when its rules are sure. You match
          the other scans to documents by hand, handwriting waits for your review, and Ask Lindley only finds words.
        </Banner>
      )}
      <p className="set-p">
        Lindley uses an AI to read handwriting, sort pages into documents and answer your questions. Where that AI runs decides who else sees your scans, and what
        each page costs. An AI on this computer, or on another computer you own, keeps them private and costs nothing. A cloud AI company sees every page it’s
        asked to read, and charges for it.
      </p>
      <h3>How much AI this computer runs</h3>
      <p className="set-p">The more this computer does itself, the less goes to the help below. You can still change any job further down.</p>
      {computer && <ComputerNote computer={computer} tiers={tiers} />}
      <TierChoices ctx="st" tiers={tiers} value={tierNow?.id ?? null} onChoose={chooseTier} computer={computer} />
      {tierNow ? (
        <LocalModels tier={tierNow} />
      ) : (
        any && <p className="fld-note">You’ve chosen the AI for each job yourself, below. Choose one of these to set them all again.</p>
      )}
      <h3>Who does the rest</h3>
      <HelpChoices ctx="st" helps={helps} value={helpNow} onChoose={chooseHelp} />
      {edit?.help ? (
        <p className="fld-note">
          <Icon name="plus" /> Add {edit.help === 'cloud' ? 'the cloud AI' : 'your server'} below to finish.
        </p>
      ) : (
        helpers.length > 1 && (
          <>
            <label className="fld" htmlFor="st-help-on">
              {helpNow === 'cloud' ? 'Cloud AI' : 'AI server'}
            </label>
            <select
              className="fld-in"
              id="st-help-on"
              value={helpers[0]}
              onChange={(e) => set(withTier(d, connectors, base, e.target.value))}
              data-tip="Which connection does the jobs this computer doesn’t"
            >
              {helpers.map((n) => (
                <option key={n} value={n}>
                  {connection(d, connectors, n)?.label ?? n}
                </option>
              ))}
            </select>
          </>
        )
      )}
      <h3>AI connections</h3>
      {conns.length ? (
        <ul className="conns">
          {conns.map((c) => {
            const use = calls?.providers[c.name]
            const jobs = jobsOf(c.name)
            const hint = keys[c.name] ? `••••${keys[c.name].slice(-4)} (not saved yet)` : use?.key_hint
            return (
              <li className="conn" key={c.name}>
                <div className="conn-main">
                  <b>{c.label}</b>
                  <PrivTag c={c} />
                  <span className="conn-where">
                    {c.cfg.type === 'builtin' ? (
                      <>
                        On this computer · {modelsOf(c.name) || 'no models in use'}
                      </>
                    ) : (
                      <>
                        {c.cloud ? `${c.company}’s cloud` : <span className="mono">{c.cfg.base_url || c.connector?.default_base_url}</span>} · model{' '}
                        <span className="mono">{c.cfg.model || c.connector?.default_models.chat || 'the usual one'}</span>
                      </>
                    )}
                    {(c.cloud || hint) && ` · ${hint ? `key saved ${hint}` : c.cfg.api_key_env ? `key from ${c.cfg.api_key_env}` : 'no key'}`}
                  </span>
                  <span className="conn-uses">
                    {jobs.length ? `Used for ${jobs.join(', ')}` : 'Not used for anything yet'} <LimitsTag c={c} />
                  </span>
                  {use && (
                    <span className="conn-usage" data-tip="Calls recorded today and this calendar month">
                      {c.cfg.allow === 'auto'
                        ? `Calls on its own: ${use.automatic_today}${use.daily_limit ? ` of ${use.daily_limit.toLocaleString()}` : ''} today · ${use.automatic_month}${use.monthly_limit ? ` of ${use.monthly_limit.toLocaleString()}` : ''} this month`
                        : `Calls you OKed: ${use.oked_today} today · ${use.oked_month} this month`}
                    </span>
                  )}
                  {use && (c.cloud || use.spent_month > 0) && (
                    <span className="conn-usage" data-tip="Worked out from the tokens each call used, at the company’s list prices. Your bill from them is the real figure.">
                      Cost, estimated: {dollars(use.spent_today)} today · {dollars(use.spent_month)} this month
                    </span>
                  )}
                </div>
                <div className="conn-acts">
                  <button className="btn" aria-label={`Change ${c.label}`} data-tip="Change its address, model, key or limits" onClick={() => setEdit(editOf(c.name, c.cfg, saved.ai.providers[c.name] ? (use?.key_hint ?? null) : null))}>
                    Change…
                  </button>
                  <button className="btn ghost" aria-label={`Remove ${c.label}`} data-tip="Remove this connection, and its key when you save" onClick={() => remove(c.name)}>
                    Remove
                  </button>
                </div>
              </li>
            )
          })}
        </ul>
      ) : (
        <p className="set-p">No AI connections yet.</p>
      )}
      {edit ? (
        <ConnEditor e={edit} set={setEdit} connectors={connectors} saved={!!edit.id && !!saved.ai.providers[edit.id]} onSave={commitEdit} onCancel={() => setEdit(null)} />
      ) : (
        <button className="btn set-add" onClick={() => setEdit(newEdit(connectors[0]))} data-tip="Connect an AI on this computer, on your network, or a cloud AI">
          <Icon name="plus" /> Add an AI connection
        </button>
      )}
      <h3>Which AI does what</h3>
      <p className="set-p">Each job can use a different connection, and its own model. For example, keep reading on this computer and use a cloud AI only for questions.</p>
      <div className="tasks">
        {JOBS.map((j) => {
          const w = JOB_WORDS[j]
          const c = connection(d, connectors, d.ai.jobs[j]?.connection)
          const usual = (j !== 'embed' && c?.cfg.model) || c?.connector?.default_models[j] || ''
          return (
            <div className="task" key={j}>
              <label className="fld" htmlFor={`st-use-${j}`}>
                {w.label}
              </label>
              <select
                className="fld-in"
                id={`st-use-${j}`}
                value={d.ai.jobs[j]?.connection ?? ''}
                onChange={(e) => {
                  const next = clone(d)
                  next.ai.jobs[j] = { connection: e.target.value || null, model: null }
                  next.ai.tier = null
                  set(next)
                }}
                data-tip={`Which connection does ${w.short}`}
              >
                <option value="">None</option>
                {conns.map((x) => {
                  const no = !x.connector?.jobs.includes(j)
                  return (
                    <option key={x.name} value={x.name} disabled={no}>
                      {x.label} ({x.cloud ? 'cloud, not private' : x.reach === 'server' ? 'your server' : 'private'}){no ? `: ${x.company} doesn’t offer this` : ''}
                    </option>
                  )
                })}
              </select>
              {!c ? (
                <span className="ptag none">{w.none}</span>
              ) : c.cloud ? (
                <span className="ptag cloud" data-tip="This job sends what it works on over the internet">
                  <Icon name="cloud" />
                  Sends {w.what} to {c.company} · not private
                </span>
              ) : c.reach === 'server' ? (
                <span className="ptag server">
                  <Icon name="warn" />
                  Sends {w.what} to {hostOf(c.cfg.base_url)}
                </span>
              ) : (
                <span className="ptag priv">
                  <Icon name="lock" />
                  Stays on {c.reach === 'this' ? 'this computer' : 'your network'}
                </span>
              )}
              {c && (
                <div className="task-model">
                  <label className="fld" htmlFor={`st-model-${j}`}>
                    Model for this job
                  </label>
                  {c.cfg.type === 'builtin' && own ? (
                    <select
                      className="fld-in"
                      id={`st-model-${j}`}
                      value={d.ai.jobs[j]?.model || usual}
                      onChange={(e) => {
                        const next = clone(d)
                        next.ai.jobs[j] = { connection: next.ai.jobs[j]?.connection ?? null, model: e.target.value }
                        next.ai.tier = null
                        set(next)
                      }}
                      data-tip="Which of Lindley’s own models does this job. One not downloaded yet is downloaded from the tier above, or with Download."
                    >
                      {own.models
                        .filter((m) => m.jobs.includes(j))
                        .map((m) => (
                          <option key={m.id} value={m.id}>
                            {m.label} ({size(m.size)}){m.state === 'ready' ? '' : ', not downloaded'}
                          </option>
                        ))}
                    </select>
                  ) : (
                    <>
                      <input
                        className="fld-in"
                        id={`st-model-${j}`}
                        value={d.ai.jobs[j]?.model ?? ''}
                        placeholder={usual}
                        autoComplete="off"
                        spellCheck={false}
                        aria-describedby={`st-modelh-${j}`}
                        onChange={(e) => {
                          const next = clone(d)
                          next.ai.jobs[j] = { connection: next.ai.jobs[j]?.connection ?? null, model: e.target.value || null }
                          next.ai.tier = null
                          set(next)
                        }}
                      />
                      <p className="fld-note" id={`st-modelh-${j}`}>
                        Leave it empty to use {usual ? <span className="mono">{usual}</span> : 'the connection’s model'}.
                      </p>
                    </>
                  )}
                </div>
              )}
            </div>
          )
        })}
      </div>
      <p className="fld-note">
        <Icon name="lock" /> API keys are kept in this computer’s credential store (Windows Credential Manager, the Keychain on a Mac, the keyring on Linux). They are never written to Lindley’s settings file or your library, and never shown
        again once saved.
      </p>
    </>
  )
}

function SetScans({ d, set }: SetProps) {
  const am: ['ask' | 'copy' | 'move', React.ReactNode][] = [
    [
      'ask',
      <>
        <b>Ask me each time.</b> Recommended.
      </>,
    ],
    [
      'copy',
      <>
        <b>Always copy them</b> and leave the originals where they are.
      </>,
    ],
    [
      'move',
      <>
        <b>Always move them</b> into Lindley, where that’s possible.
      </>,
    ],
  ]
  return (
    <>
      <p className="set-p">Lindley watches these folders. When a scan arrives it reads it and puts it in your Inbox.</p>
      <h3>Folders to watch</h3>
      <FoldersEditor ctx="st" folders={d.watch_folders} set={(f) => set({ ...d, watch_folders: f })} />
      <h3>When a scan arrives in a watched folder</h3>
      <ModeChoices ctx="st" move={d.move_files} set={(m) => set({ ...d, move_files: m })} />
      <h3>When you use Add scans… in the Inbox</h3>
      <fieldset className="choices">
        <legend className="sr-only">When you use Add scans</legend>
        {am.map(([v, l]) => (
          <label className="choice" key={v}>
            <input type="radio" name="st-add" checked={d.add_mode === v} onChange={() => set({ ...d, add_mode: v })} />
            <span>{l}</span>
          </label>
        ))}
      </fieldset>
      <p className="fld-note">Files added from this browser are always copied: a browser can’t remove the originals. Either way, Lindley never changes the scans themselves.</p>
    </>
  )
}

function SetReading({ d, set, reviewNow, saved }: SetProps & { reviewNow: number | undefined; saved: Settings }) {
  const { connectors } = useApp()
  const v = connection(d, connectors, d.ai.jobs.vision?.connection)
  const ocr = (p: Partial<Settings['ocr']>) => set({ ...d, ocr: { ...d.ocr, ...p } })
  const langs = [...LANGS, ...d.ocr.languages.filter((l) => !LANGS.some(([k]) => k === l)).map((l) => [l, l] as [string, string])]
  const slider = (id: string, label: string, value: number, lo: number, hi: number, on: (n: number) => void, note: React.ReactNode) => (
    <>
      <label className="fld" htmlFor={id}>
        {label}
      </label>
      <div className="thr-row">
        <input type="range" id={id} min={lo} max={hi} step={1} value={value} onChange={(e) => on(Number(e.target.value))} aria-describedby={`${id}-p`} />
        <input
          type="number"
          className="fld-in"
          min={lo}
          max={hi}
          value={value}
          aria-label={`${label}, in percent`}
          aria-describedby={`${id}-p`}
          onChange={(e) => {
            const n = Number(e.target.value)
            if (n >= lo && n <= hi) on(Math.round(n))
          }}
        />
        <span aria-hidden="true">% confidence</span>
      </div>
      <p className="fld-note" id={`${id}-p`} aria-live="polite">
        {note}
      </p>
    </>
  )
  return (
    <>
      <h3>When Lindley asks for your review</h3>
      <p className="set-p">
        Lindley flags any page it read with less confidence than this, so a person checks it before it’s trusted for search and chat. A higher number means more
        pages to check, and fewer mistakes slipping through.
      </p>
      {slider(
        'st-thr',
        'Flag pages read with less than',
        d.ocr.review_below,
        50,
        99,
        (n) => ocr({ review_below: n }),
        <>
          Right now {plural(reviewNow ?? 0, 'page')} {reviewNow === 1 ? 'needs' : 'need'} your review, at {saved.ocr.review_below}%.
          {d.ocr.review_below !== saved.ocr.review_below && ' The count changes when you save.'}
        </>,
      )}
      <h3>Languages in your scans</h3>
      <fieldset className="langs">
        <legend className="sr-only">Languages in your scans</legend>
        {langs.map(([k, l]) => (
          <label key={k} data-tip={`Tesseract reads printed ${l}. Its language pack must be installed.`}>
            <input
              type="checkbox"
              checked={d.ocr.languages.includes(k)}
              onChange={(e) => ocr({ languages: e.target.checked ? [...d.ocr.languages, k] : d.ocr.languages.filter((x) => x !== k) })}
            />
            {l}
          </label>
        ))}
      </fieldset>
      <p className="fld-note">The printed-text reader uses these. Choose only the ones you need, since each one adds a little time.</p>
      <h3>Printed text</h3>
      <label className="fld" htmlFor="st-tess">
        Tesseract program
      </label>
      <input
        className="fld-in"
        id="st-tess"
        value={d.ocr.tesseract_path ?? ''}
        placeholder="Found by itself (for example C:\Program Files\Tesseract-OCR\tesseract.exe)"
        onChange={(e) => ocr({ tesseract_path: e.target.value || null })}
        spellCheck={false}
        data-tip="Leave empty unless Lindley can’t find Tesseract by itself"
      />
      <h3>Handwriting and hard pages</h3>
      <div className="kv">
        {v ? (
          <>
            Read by {v.label} <PrivTag c={v} /> <LimitsTag c={v} />
          </>
        ) : (
          'No AI connected, so these pages wait for your review.'
        )}
      </div>
      {v &&
        slider(
          'st-aib',
          'Send a page to the AI when Tesseract reads it with less than',
          d.ocr.confidence_threshold,
          20,
          95,
          (n) => ocr({ confidence_threshold: n }),
          <>
            {v.cfg.allow === 'auto' ? `${v.label} runs on its own, so these pages go to it as soon as they’re read.` : `${v.label} asks first, so these pages wait in Needs AI until you send them.`}{' '}
            Tesseract’s rough reading is used meanwhile.
          </>,
        )}
    </>
  )
}

function SetLibrary({ d, set }: SetProps) {
  const sep = d.library_dir.includes('/') && !d.library_dir.includes('\\') ? '/' : '\\'
  return (
    <>
      <p className="set-p">
        Lindley’s copies of your scans and the PDFs you export live in its library folder. Its database, of everything it read and every change you made, is a file
        of its own. Back up both and you’ve backed up everything.
      </p>
      <label className="fld" htmlFor="st-lib">
        Library folder
      </label>
      <input className="fld-in" id="st-lib" value={d.library_dir} onChange={(e) => set({ ...d, library_dir: e.target.value })} spellCheck={false} data-tip="Lindley’s own folder. New scans are copied here." />
      <ul className="kv-list">
        {(
          [
            ['scans', 'Lindley’s copies of your scans'],
            ['pages', 'Each page as an image'],
            ['Exports', 'PDFs you export'],
          ] as const
        ).map(([x, l]) => (
          <li key={x}>
            {l}: <span className="mono">{`${d.library_dir}${sep}${x}`}</span>
          </li>
        ))}
      </ul>
      <label className="fld" htmlFor="st-db">
        Database
      </label>
      <input className="fld-in" id="st-db" value={d.db_path} onChange={(e) => set({ ...d, db_path: e.target.value })} spellCheck={false} aria-describedby="st-dbh" />
      <p className="fld-note" id="st-dbh">
        The database of everything Lindley read and every change you made: names, page order, corrections. To move it, copy the file to the new place first, with
        the file beside it whose name ends in -wal if there is one, then save. Lindley checks the file is its database before it uses it.
      </p>
      <h3>Keeping your originals safe</h3>
      <ul className="safe">
        <li>
          <Icon name="check" />
          <span>Lindley never changes a scan. Turning, correcting text and putting pages in order are saved alongside it.</span>
        </li>
        <li>
          <Icon name="check" />
          <span>Nothing is deleted. Set aside keeps scans you don’t need, and you can bring them back at any time.</span>
        </li>
        <li>
          <Icon name="check" />
          <span>API keys are never stored in the library or the database, so both are safe to back up or copy to another computer.</span>
        </li>
      </ul>
    </>
  )
}

function SetLook() {
  const [theme, setTheme] = useTheme()
  const [pos, setPos] = useDockPos()
  const th = [
    ['system', 'Match my computer'],
    ['light', 'Light'],
    ['dark', 'Dark'],
  ] as const
  return (
    <>
      <h3>Theme</h3>
      <p className="set-p">Kept in this browser, and used at once.</p>
      <fieldset className="choices">
        <legend className="sr-only">Theme</legend>
        {th.map(([v, l]) => (
          <label className="choice" key={v}>
            <input type="radio" name="st-theme" checked={theme === v} onChange={() => setTheme(v)} />
            <span>
              <b>{l}</b>
            </span>
          </label>
        ))}
      </fieldset>
      <h3>Toolbar</h3>
      <p className="set-p">Drag the toolbar anywhere by the handle at its left end. This puts it back at the bottom, in the middle.</p>
      <button className="btn set-add" disabled={!pos} onClick={() => setPos(null)} data-tip={pos ? 'Put the toolbar back at the bottom' : 'The toolbar is where it started'}>
        Reset toolbar position
      </button>
    </>
  )
}
