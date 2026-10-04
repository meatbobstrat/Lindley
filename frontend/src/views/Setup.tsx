// First-run setup: where scans come from, whether they're copied or moved, and which AI to use,
// if any. Shown until there's a settings file.

import { useEffect, useRef, useState } from 'react'
import { api, type Connector, JOBS, type Settings } from '../api/client'
import { invalidate } from '../api/store'
import { cloudInUse } from '../lib/ai'
import { plural } from '../lib/words'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'
import { cfgOf, type Edit, editReady, newEdit, newId } from '../lib/connEdit'
import { ConnEditor, FoldersEditor, ModeChoices } from './Settings'

export function Setup({ settings, connectors }: { settings: Settings; connectors: Connector[] }) {
  const { toast } = useFeedback()
  const ref = useRef<HTMLDialogElement>(null)
  const [folders, setFolders] = useState<string[]>([])
  const [move, setMove] = useState(settings.move_files)
  const [mode, setMode] = useState<'connect' | 'none'>('connect')
  const [edit, setEdit] = useState<Edit>(() => newEdit(connectors.find((c) => c.where !== 'cloud') ?? connectors[0]))
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    const d = ref.current
    if (d && !d.open) d.showModal()
  }, [])
  const c = connectors.find((x) => x.id === edit.type)

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
    if (mode === 'connect') {
      if (c?.needs_key && !edit.ack) {
        toast('Read the warning and check the box to use a cloud AI, or choose an AI on a computer you own.')
        return
      }
      if (!editReady(edit, c)) {
        toast('Paste your API key, or choose No AI.')
        document.getElementById('su-key')?.focus()
        return
      }
      const id = newId(edit.label || c?.short || 'ai', [])
      next.ai.providers[id] = cfgOf(edit, c)
      // The one connection does every job it can
      JOBS.forEach((j) => {
        if (c?.jobs.includes(j)) next.ai.jobs[j] = { connection: id, model: null }
      })
      if (edit.key.trim()) key = [id, edit.key.trim()]
    }
    setSaving(true)
    try {
      const saved = await api.saveSettings(next)
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
          <h3 id="su-4">4. Connect an AI, if you like</h3>
          <p>An AI reads handwriting, helps sort pages into documents, and answers your questions. Where it runs decides who else sees your scans.</p>
          <fieldset className="choices">
            <legend className="sr-only">Connect an AI</legend>
            <label className="choice">
              <input type="radio" name="su-ai" checked={mode === 'connect'} onChange={() => setMode('connect')} />
              <span>
                <b>
                  <Icon name="plus" />
                  Connect one AI.
                </b>{' '}
                Choose the service below. An AI on your own computer keeps your scans private. A cloud AI needs an API key, and sees every page it reads.
              </span>
            </label>
            <label className="choice">
              <input type="radio" name="su-ai" checked={mode === 'none'} onChange={() => setMode('none')} />
              <span>
                <b>
                  <Icon name="warn" />
                  No AI. Use Lindley with its rules and Tesseract.
                </b>{' '}
                Printed pages are read on this computer, and Lindley groups pages into documents when its rules are sure. You match the other scans to documents by
                hand. Handwriting waits for your review. You can connect an AI in Settings at any time.
              </span>
            </label>
          </fieldset>
          {mode === 'connect' && (
            <>
              <ConnEditor e={edit} set={(e) => setEdit(e)} connectors={connectors} inSetup saved={false} />
              <p className="fld-note">
                <Icon name="plus" /> You can add more AIs later in Settings, and give each job its own: for example, reading on this computer and questions with a
                cloud AI.
              </p>
            </>
          )}
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
