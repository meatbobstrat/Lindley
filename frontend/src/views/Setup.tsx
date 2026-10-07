// First-run setup: where scans come from, whether they're copied or moved, how much AI this
// computer runs, and who does the rest. Shown until there's a settings file.

import { useEffect, useRef, useState } from 'react'
import { api, type Connector, type Help, type HelpKind, JOBS, type Settings, type Tier } from '../api/client'
import { invalidate, useApi } from '../api/store'
import { cloudInUse } from '../lib/ai'
import { plural } from '../lib/words'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'
import { cfgOf, type Edit, editReady, newEdit, newId } from '../lib/connEdit'
import { withTier } from '../lib/tiers'
import { ComputerNote, ConnEditor, FoldersEditor, HelpChoices, LocalModels, ModeChoices, TierChoices } from './Settings'

/** The connection the help starts from: a server on your network, or a cloud AI. */
const editFor = (h: HelpKind, connectors: Connector[]): Edit =>
  h === 'cloud'
    ? { ...newEdit(connectors.find((c) => c.where === 'cloud')), help: h }
    : { ...newEdit(connectors.find((c) => c.id === 'local')), label: 'Your AI server', base_url: '', help: h }

export function Setup({ settings, connectors, tiers, helps }: { settings: Settings; connectors: Connector[]; tiers: Tier[]; helps: Help[] }) {
  const { toast } = useFeedback()
  const ref = useRef<HTMLDialogElement>(null)
  const [folders, setFolders] = useState<string[]>([])
  const [move, setMove] = useState(settings.move_files)
  // The tier this computer suits, once Lindley has looked at it (Middle until then), unless the
  // person has chosen one
  const computer = useApi('computer', api.computer).data
  const [chosen, setChosen] = useState<Tier | null>(null)
  const tier = chosen ?? tiers.find((t) => t.id === (computer?.suggested ?? 'middle')) ?? tiers[0]
  const [help, setHelp] = useState<HelpKind>('none')
  const [edit, setEdit] = useState<Edit>(() => editFor('cloud', connectors))
  const [download, setDownload] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    const d = ref.current
    if (d && !d.open) d.showModal()
  }, [])
  const c = connectors.find((x) => x.id === edit.type)
  const chooseHelp = (h: Help) => {
    if (h.id !== 'none' && h.id !== help) setEdit(editFor(h.id, connectors))
    setHelp(h.id)
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
    let helper: string | null = null
    if (help !== 'none') {
      if (c?.needs_key && !edit.ack) {
        toast('Read the warning and check the box to use a cloud AI, or choose another.')
        return
      }
      if (!editReady(edit, c)) {
        toast('Paste your API key, or choose another.')
        document.getElementById('su-key')?.focus()
        return
      }
      if (help === 'server' && !edit.base_url.trim()) {
        toast('Type your AI server’s address, for example http://192.168.1.20:11434/v1.')
        document.getElementById('su-url')?.focus()
        return
      }
      helper = newId(edit.label || c?.short || 'ai', [])
      next.ai.providers[helper] = cfgOf(edit, c)
      if (edit.key.trim()) key = [helper, edit.key.trim()]
    }
    const ready = withTier(next, connectors, tier, helper)
    setSaving(true)
    try {
      const saved = await api.saveSettings(ready)
      if (key) await api.saveKey(...key)
      const fetching = download && tier.downloads.length > 0
      if (fetching) await api.downloadModels(tier.downloads)
      const cloud = cloudInUse(saved, connectors)
      invalidate()
      toast(
        `Lindley is watching ${plural(folders.length, 'folder')} and will ${move ? 'move new scans into its library' : 'copy new scans, leaving your folders untouched'}.${
          cloud.length ? ` Your scans will be sent to ${cloud.join(' and ')}.` : ''
        }${fetching ? ' Its own AI is downloading: the status bar shows how far it’s got.' : ''}`,
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
            An AI reads handwriting, helps sort pages into documents, and answers your questions. The more this computer can do itself, the more private your
            scans stay and the less each page costs.
          </p>
          {computer && <ComputerNote computer={computer} tiers={tiers} />}
          <TierChoices ctx="su" tiers={tiers} value={tier.id} onChoose={setChosen} computer={computer} />
          <LocalModels tier={tier} now={download} setNow={setDownload} />
        </section>
        <section aria-labelledby="su-5">
          <h3 id="su-5">5. Who does the rest?</h3>
          <p>What this computer doesn’t do itself can go to another computer you own, or to a cloud AI, or wait for you.</p>
          <HelpChoices ctx="su" helps={helps} value={help} onChoose={chooseHelp} />
          {help !== 'none' && <ConnEditor e={edit} set={(e) => setEdit(e)} connectors={connectors} inSetup saved={false} />}
          <p className="fld-note">
            <Icon name="plus" /> You can change these in Settings at any time, and give each job its own AI: for example, reading on this computer and questions
            with a cloud AI.
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
