// First-run setup: where scans come from, whether they're copied or moved, and how much AI this
// computer runs (a performance tier). Shown until there's a settings file.

import { useEffect, useRef, useState } from 'react'
import { api, type Connector, JOBS, type Settings, type Tier } from '../api/client'
import { invalidate } from '../api/store'
import { cloudInUse } from '../lib/ai'
import { plural } from '../lib/words'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'
import { cfgOf, type Edit, editReady, newEdit, newId } from '../lib/connEdit'
import { withTier } from '../lib/tiers'
import { ConnEditor, FoldersEditor, ModeChoices, TierChoices, TierModels } from './Settings'

/** The connection a tier elsewhere starts from: a server on your network, or a cloud AI. */
const editFor = (t: Tier, connectors: Connector[]): Edit =>
  t.runs === 'cloud'
    ? newEdit(connectors.find((c) => c.where === 'cloud'))
    : { ...newEdit(connectors.find((c) => c.id === 'local')), label: 'Your AI server', base_url: '' }

export function Setup({ settings, connectors, tiers }: { settings: Settings; connectors: Connector[]; tiers: Tier[] }) {
  const { toast } = useFeedback()
  const ref = useRef<HTMLDialogElement>(null)
  const [folders, setFolders] = useState<string[]>([])
  const [move, setMove] = useState(settings.move_files)
  // Until Lindley can look at the computer itself, an AI on it, as before tiers
  const [tier, setTier] = useState<Tier>(() => tiers.find((t) => t.id === 'full') ?? tiers[0])
  const [edit, setEdit] = useState<Edit>(() => editFor(tier, connectors))
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    const d = ref.current
    if (d && !d.open) d.showModal()
  }, [])
  const c = connectors.find((x) => x.id === edit.type)
  const elsewhere = tier.runs === 'network' || tier.runs === 'cloud'
  const choose = (t: Tier) => {
    if (t.runs !== tier.runs && (t.runs === 'network' || t.runs === 'cloud')) setEdit(editFor(t, connectors))
    setTier(t)
  }

  const save = async () => {
    if (!folders.length) {
      toast('Add at least one folder for Lindley to watch.')
      document.getElementById('su-path')?.focus()
      return
    }
    const next: Settings = JSON.parse(JSON.stringify(settings))
    next.watch_folders = folders
    next.move_files = move
    next.ai.providers = {}
    JOBS.forEach((j) => (next.ai.jobs[j] = { connection: null, model: null }))
    let key: [string, string] | null = null
    let ready: Settings | null
    if (elsewhere) {
      if (c?.needs_key && !edit.ack) {
        toast('Read the warning and check the box to use a cloud AI, or choose an AI on a computer you own.')
        return
      }
      if (!editReady(edit, c)) {
        toast('Paste your API key, or choose another tier.')
        document.getElementById('su-key')?.focus()
        return
      }
      if (tier.runs === 'network' && !edit.base_url.trim()) {
        toast('Type your AI server’s address, for example http://192.168.1.20:11434/v1.')
        document.getElementById('su-url')?.focus()
        return
      }
      const id = newId(edit.label || c?.short || 'ai', [])
      next.ai.providers[id] = cfgOf(edit, c)
      ready = withTier(next, connectors, tier, id)
      if (edit.key.trim()) key = [id, edit.key.trim()]
    } else ready = withTier(next, connectors, tier)
    if (!ready) {
      toast('Lindley can’t set that up. Choose another tier.')
      return
    }
    setSaving(true)
    try {
      const saved = await api.saveSettings(ready)
      if (key) await api.saveKey(...key)
      const cloud = cloudInUse(saved, connectors)
      invalidate()
      toast(
        `Lindley is watching ${plural(folders.length, 'folder')} and will ${move ? 'move new scans into its library' : 'copy new scans, leaving your folders untouched'}.${
          cloud.length ? ` Your scans will be sent to ${cloud.join(' and ')}.` : ''
        }`,
      )
    } catch (e) {
      toast((e as Error).message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <dialog ref={ref} className="setup setup-dlg" aria-labelledby="setup-h" onCancel={(e) => e.preventDefault()}>
      <div className="setup-card">
        <div className="setup-brand">
          <Mark />
          <span>Lindley</span>
        </div>
        <h2 id="setup-h" tabIndex={-1}>
          Welcome. First, where do your scans come from?
        </h2>
        <p>
          Lindley watches the folders you choose. When a scan arrives it reads it and puts it in your Inbox. You can also add scans from anywhere, such as a thumb
          drive, with Add scans in the Inbox.
        </p>
        <section aria-labelledby="su-1">
          <h3 id="su-1">1. Folders to watch</h3>
          <FoldersEditor ctx="su" folders={folders} set={setFolders} />
        </section>
        <section aria-labelledby="su-2">
          <h3 id="su-2">2. When a scan arrives</h3>
          <ModeChoices ctx="su" move={move} set={setMove} />
        </section>
        <section aria-labelledby="su-3">
          <h3 id="su-3">3. When Lindley needs your help</h3>
          <p>Lindley flags any page it read with less than {settings.ocr.review_below}% confidence so you can check it. You can change this in Settings.</p>
        </section>
        <section aria-labelledby="su-4">
          <h3 id="su-4">4. How much AI can this computer run?</h3>
          <p>
            An AI reads handwriting, helps sort pages into documents, and answers your questions. The more memory this computer has, the more it can do itself. Where
            the AI runs decides who else sees your scans.
          </p>
          <TierChoices ctx="su" tiers={tiers} value={tier.id} onChoose={choose} />
          {elsewhere ? <ConnEditor e={edit} set={(e) => setEdit(e)} connectors={connectors} inSetup saved={false} /> : <TierModels tier={tier} />}
          <p className="fld-note">
            <Icon name="plus" /> You can change this in Settings at any time, and give each job its own AI: for example, reading on this computer and questions with
            a cloud AI.
          </p>
        </section>
        <div className="actions">
          <button className="btn primary" onClick={save} disabled={saving} data-tip="Save these settings and open the Inbox">
            {saving ? 'Saving…' : 'Start using Lindley'}
          </button>
        </div>
      </div>
    </dialog>
  )
}
