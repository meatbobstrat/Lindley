// Actions on pages and documents that several views offer, and the dialogs they open.

import { type ReactNode, useCallback, useMemo, useState } from 'react'
import { useNavigate } from 'react-router'
import { api, type Folder, type Sent } from '../api/client'
import { invalidate } from '../api/store'
import { Modal } from '../ui/feedback'
import { type MenuItem, useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'
import { type Actions, ActionsCtx } from './actionsContext'
import { useApp } from './appContext'
import { plural, quoted, shortName } from './words'

type Dialog =
  | { kind: 'newdoc'; ids: number[]; name: string; lindley: boolean }
  | { kind: 'export'; doc: { id: number; name: string; pages: number; suggested: boolean; folder_id: number | null }; toReview: number }
  | { kind: 'details'; doc: { id: number; name: string; doc_type: string | null; doc_date: string | null } }
  | { kind: 'add'; files: File[] }

/** A first guess at a name from a page's text: a printed heading, or "Letter, March 1892". */
function guessName(text: string): string {
  const first = text.split('\n')[0]?.trim() ?? ''
  if (/[A-Z]{3}/.test(first) && first === first.toUpperCase() && first.length < 60)
    return first.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase())
  const m = text.match(/\b(Jan|Feb|March|April|May|June|July|Aug|Sept|Oct|Nov|Dec)[a-z.]*\s+\d{1,2}(?:st|nd|rd|th)?,?\s+(1[6-9]\d\d)/)
  if (!m) return ''
  const month = ({ Jan: 'January', Feb: 'February', Aug: 'August', Sept: 'September', Oct: 'October', Nov: 'November', Dec: 'December' } as Record<string, string>)[m[1]] ?? m[1]
  return `${/^(Dear|Friend|My dear)/m.test(text) ? 'Letter' : 'Document'}, ${month} ${m[2]}`
}

export function ActionsProvider({ children }: { children: ReactNode }) {
  const { run, openMenu, toast } = useFeedback()
  const { docs, folders, settings } = useApp()
  const nav = useNavigate()
  const [dialog, setDialog] = useState<Dialog | null>(null)
  const close = useCallback(() => setDialog(null), [])

  const removedNote = (removed?: number[]) => (removed?.length ? ' A document left with no pages was removed.' : '')

  /** "Sent 3 pages to Ollama. …" for AI work queued in the background. */
  const sent = (what: string) => (r: Sent) => {
    const to = r.connection ? ` to ${r.connection}` : ''
    const also = r.already ? ` ${plural(r.already, 'page')} ${r.already === 1 ? 'was' : 'were'} on ${r.already === 1 ? 'its' : 'their'} way already.` : ''
    if (!r.queued) return `${r.already === 1 ? 'That page is' : 'Those pages are'} on ${r.already === 1 ? 'its' : 'their'} way to the AI already.`
    return `Sent ${plural(r.queued, 'page')}${to} to ${what}.${also} You can keep working: the status bar says when it’s done.`
  }

  const sendFiles = useCallback(
    (files: File[]) => {
      run(api.addScans(files), (r) => {
        const fresh = r.added.filter((a) => a.status === 'new').length
        const dup = r.added.filter((a) => a.status === 'duplicate').length
        const bad = r.added.filter((a) => a.status === 'failed')
        return [
          fresh ? `Added ${plural(fresh, 'scan')} to the Inbox. Lindley is reading ${fresh === 1 ? 'it' : 'them'} now.` : '',
          dup ? `${plural(dup, 'file')} ${dup === 1 ? 'was' : 'were'} already in Lindley.` : '',
          bad.length ? `${plural(bad.length, 'file')} couldn’t be added: ${bad[0].error}` : '',
        ]
          .filter(Boolean)
          .join(' ')
      })
      nav('/inbox')
    },
    [run, nav],
  )

  const value = useMemo<Actions>(() => {
    const a: Actions = {
      rotate: (ids, deg) => {
        if (ids.length) run(api.rotate(ids, deg), `Turned ${plural(ids.length, 'page')} ${deg < 0 ? 'left' : 'right'}.`)
      },
      flip: (ids) => {
        if (ids.length) run(api.flip(ids), `Turned ${plural(ids.length, 'page')} round left to right.`)
      },
      toInbox: (ids) => {
        if (ids.length)
          run(api.movePages(ids, 'inbox'), (r) => `Returned ${plural(ids.length, 'page')} to the Inbox. Lindley will sort ${ids.length === 1 ? 'it' : 'them'} again.${removedNote(r.removed)}`)
      },
      setAside: (ids) => {
        if (ids.length) run(api.movePages(ids, 'aside'), (r) => `Set aside ${plural(ids.length, 'scan')}. Nothing was deleted.${removedNote(r.removed)}`)
      },
      moveTo: (ids, doc) => {
        if (ids.length) run(api.movePages(ids, 'document', doc.id), (r) => `Moved ${plural(ids.length, 'page')} to ${quoted(shortName(doc.name))}.${removedNote(r.removed)}`)
      },
      shift: (docId, order, ids, dir) => {
        const arr = [...order]
        const idx = ids.map((id) => arr.indexOf(id)).sort((x, y) => (dir < 0 ? x - y : y - x))
        let moved = false
        idx.forEach((i) => {
          const j = i + dir
          if (i < 0 || j < 0 || j >= arr.length || ids.includes(arr[j])) return
          ;[arr[i], arr[j]] = [arr[j], arr[i]]
          moved = true
        })
        if (moved) run(api.reorder(docId, arr), `Moved ${plural(ids.length, 'page')} ${dir < 0 ? 'earlier' : 'later'}.`)
      },
      newDocument: (ids, name, lindley = false) => {
        if (!ids.length) return
        if (name !== undefined) {
          setDialog({ kind: 'newdoc', ids, name, lindley })
          return
        }
        api.page(ids[0]).then(
          (p) => setDialog({ kind: 'newdoc', ids, name: guessName(p.text), lindley: false }),
          () => setDialog({ kind: 'newdoc', ids, name: '', lindley: false }),
        )
      },
      readWithAi: (ids) => {
        run(api.readWithAi(ids), sent('read'))
      },
      sortWithAi: (itemId) => {
        run(api.sortWithAi(itemId), sent('sort'))
      },
      confirmExport: (doc, toReview) => setDialog({ kind: 'export', doc, toReview }),
      editDetails: (doc) => setDialog({ kind: 'details', doc }),
      addScans: (files) => {
        if (!files.length) return
        if (settings?.add_mode === 'ask') setDialog({ kind: 'add', files })
        else sendFiles(files)
      },
      fileDoc: (doc, folderId) => {
        if (doc.folder_id === folderId) return
        const to = folderId != null ? folders.get(folderId)?.name : null
        const from = doc.folder_id != null ? folders.get(doc.folder_id)?.name : null
        run(
          api.updateDocument(doc.id, { folder_id: folderId }),
          to
            ? `Moved ${quoted(shortName(doc.name))} to ${to}.`
            : `Took ${quoted(shortName(doc.name))} out of ${from}. It’s back under ${doc.status === 'complete' ? 'Completed' : 'In progress'}.`,
        )
      },
      newFolder: async (parent, docId) => {
        if (!docId) {
          const r = await run(api.newFolder('New folder', parent), 'Created a folder. Click its name to rename it.')
          if (r?.folder_id) nav(`/folders/${r.folder_id}`)
          return
        }
        try {
          const f = await api.newFolder('New folder', parent)
          invalidate()
          if (f.folder_id) run(api.updateDocument(docId, { folder_id: f.folder_id }), 'Created a folder and moved the document into it. Click the folder to rename it.')
        } catch (e) {
          toast((e as Error).message)
        }
      },
      moveMenu: (ids, anchor, here) => {
        const open = [...docs.values()].filter((d) => d.status === 'progress' && d.id !== here)
        const items: MenuItem[] = [
          { head: here ? 'Move to document' : 'Add to document' },
          ...open.map((d) => {
            const f = d.folder_id != null ? folders.get(d.folder_id) : undefined
            return {
              label: d.name + (f ? ` (in ${f.name})` : ''),
              icon: 'doc' as const,
              tip: `Add ${plural(ids.length, 'page')} to the end of ${quoted(d.name)}`,
              onSelect: () => a.moveTo(ids, d),
            }
          }),
          ...(open.length ? [] : [{ head: 'No documents in progress yet' }]),
          '-',
          { label: 'New document…', icon: 'newdoc', tip: 'Start a new document with these pages', onSelect: () => a.newDocument(ids) },
        ]
        openMenu(items, anchor)
      },
      folderMenu: (doc, anchor) => {
        const rows: MenuItem[] = []
        const walk = (parent: number | null, depth: number) =>
          [...folders.values()]
            .filter((f: Folder) => f.parent_id === parent)
            .forEach((f) => {
              const here = f.id === doc.folder_id
              rows.push({
                label: f.name + (here ? ' (here now)' : ''),
                icon: 'folder',
                depth,
                disabled: here,
                tip: here ? 'The document is in this folder now' : `File the document in ${f.name}`,
                onSelect: () => a.fileDoc(doc, f.id),
              })
              walk(f.id, depth + 1)
            })
        walk(null, 0)
        const from = doc.folder_id != null ? folders.get(doc.folder_id) : undefined
        openMenu(
          [
            { head: 'Move to folder' },
            ...rows,
            '-',
            { label: 'New folder', icon: 'plus', tip: 'Make a folder and put this document in it', onSelect: () => a.newFolder(null, doc.id) },
            ...(from
              ? [{ label: `Take out of ${from.name}`, icon: 'back' as const, tip: 'Back under In progress or Completed', onSelect: () => a.fileDoc(doc, null) }]
              : []),
          ],
          anchor,
        )
      },
      pageMenu: (ids, where, anchor, opts = {}) => {
        const items: MenuItem[] = []
        if (ids.length === 1 && where !== 'document')
          items.push({ label: 'Open to read and correct', icon: 'eye', tip: 'See the scan beside its text', onSelect: () => nav(`/scans/${ids[0]}`) }, '-')
        if (!opts.noBasics) {
          items.push(
            { label: 'Turn left', icon: 'rotL', tip: 'A quarter turn to the left. The scan itself isn’t changed.', onSelect: () => a.rotate(ids, -90) },
            { label: 'Turn right', icon: 'rotR', tip: 'A quarter turn to the right. The scan itself isn’t changed.', onSelect: () => a.rotate(ids, 90) },
            { label: 'Flip left to right', icon: 'flip', tip: 'For a mirror image, such as the back of a carbon copy. The scan itself isn’t changed.', onSelect: () => a.flip(ids) },
          )
          if (where === 'document' && opts.docId && opts.order) {
            const { docId, order } = opts
            items.push(
              '-',
              { label: 'Move earlier', icon: 'up', onSelect: () => a.shift(docId, order, ids, -1) },
              { label: 'Move later', icon: 'down', onSelect: () => a.shift(docId, order, ids, 1) },
            )
          }
          items.push('-')
        }
        const open = [...docs.values()].filter((d) => d.status === 'progress' && d.id !== opts.docId)
        open.forEach((d) => items.push({ label: d.name, icon: 'doc', tip: `Add to the end of ${quoted(d.name)}`, onSelect: () => a.moveTo(ids, d) }))
        items.push({ label: 'New document…', icon: 'newdoc', tip: 'Start a new document with these pages', onSelect: () => a.newDocument(ids) }, '-')
        if (where !== 'inbox')
          items.push({ label: 'Return to Inbox', icon: 'inbox', tip: 'Take the pages out, for Lindley to sort again', onSelect: () => a.toInbox(ids) })
        if (where !== 'aside')
          items.push({ label: 'Set aside', icon: 'aside', tip: 'For scans that aren’t part of a document. Nothing is deleted.', onSelect: () => a.setAside(ids) })
        openMenu(items, anchor, opts.at ? { at: opts.at } : undefined)
      },
    }
    return a
  }, [run, openMenu, toast, docs, folders, settings, nav, sendFiles])

  return (
    <ActionsCtx.Provider value={value}>
      {children}
      {dialog?.kind === 'newdoc' && <NewDocDialog d={dialog} close={close} />}
      {dialog?.kind === 'export' && <ExportDialog d={dialog} close={close} />}
      {dialog?.kind === 'details' && <DetailsDialog d={dialog} close={close} />}
      {dialog?.kind === 'add' && (
        <AddDialog
          files={dialog.files}
          close={close}
          go={() => {
            close()
            sendFiles(dialog.files)
          }}
        />
      )}
    </ActionsCtx.Provider>
  )
}

function folderOptions(folders: Map<number, Folder>): { id: number; label: string }[] {
  const out: { id: number; label: string }[] = []
  const walk = (parent: number | null, depth: number) =>
    [...folders.values()]
      .filter((f) => f.parent_id === parent)
      .forEach((f) => {
        out.push({ id: f.id, label: `${'   '.repeat(depth)}${f.name}` })
        walk(f.id, depth + 1)
      })
  walk(null, 0)
  return out
}

function NewDocDialog({ d, close }: { d: Extract<Dialog, { kind: 'newdoc' }>; close: () => void }) {
  const { folders } = useApp()
  const { run } = useFeedback()
  const nav = useNavigate()
  const [name, setName] = useState(d.name)
  const [folder, setFolder] = useState('')
  const create = async () => {
    close()
    const r = await run(
      api.newDocument(d.ids, name, folder ? Number(folder) : null, d.lindley && name === d.name),
      (x) => `Started ${quoted(shortName(name || 'Untitled document'))} with ${plural(d.ids.length, 'page')}.${x.removed?.length ? ' A document left with no pages was removed.' : ''}`,
    )
    if (r?.document_id) nav(`/documents/${r.document_id}`)
  }
  return (
    <Modal title="Start a new document" onClose={close}>
      <p>
        {d.ids.length === 1 ? 'The selected scan moves' : `The ${d.ids.length} selected scans move`} into it, in the order shown. You can change the order afterwards.
      </p>
      <label className="fld" htmlFor="nd-name">
        Name
      </label>
      <input
        className="fld-in"
        id="nd-name"
        autoFocus
        value={name}
        onChange={(e) => setName(e.target.value)}
        onFocus={(e) => e.target.select()}
        onKeyDown={(e) => {
          if (e.key === 'Enter') create()
        }}
        placeholder="For example: Letters from Will, 1890"
        aria-describedby={d.name ? 'nd-hint' : undefined}
      />
      {d.name && (
        <p className="fld-hint" id="nd-hint">
          <Mark /> Lindley suggested this from {d.lindley ? 'the pages' : 'the first page'}. Change it to anything you like.
        </p>
      )}
      <label className="fld" htmlFor="nd-folder">
        Put it in
      </label>
      <select className="fld-in" id="nd-folder" value={folder} onChange={(e) => setFolder(e.target.value)} data-tip="A folder of yours, or none: it then waits under In progress">
        <option value="">In progress (no folder)</option>
        {folderOptions(folders).map((f) => (
          <option key={f.id} value={f.id}>
            {f.label}
          </option>
        ))}
      </select>
      <div className="actions">
        <button className="btn" onClick={close} data-tip="Close without making a document">
          Cancel
        </button>
        <button className="btn primary" onClick={create} data-tip="Make the document and open it">
          Create document
        </button>
      </div>
    </Modal>
  )
}

function ExportDialog({ d, close }: { d: Extract<Dialog, { kind: 'export' }>; close: () => void }) {
  const { folders } = useApp()
  const { run } = useFeedback()
  const nav = useNavigate()
  const folder = d.doc.folder_id != null ? folders.get(d.doc.folder_id) : undefined
  const go = () => {
    close()
    run(api.exportDocument(d.doc.id), (r) => {
      const notes = [
        r.to_review.length ? `${plural(r.to_review.length, 'page')} still waiting for review ${r.to_review.length === 1 ? 'has' : 'have'} Lindley’s best reading.` : '',
        r.unplaced.length ? `${plural(r.unplaced.length, 'page')} read only by the AI ${r.unplaced.length === 1 ? 'is' : 'are'} searchable, but the text isn’t laid over the writing.` : '',
      ]
      return `Exported ${quoted(shortName(d.doc.name))} as ${r.file_name}. ${folder ? `It stays in ${folder.name}, marked Completed.` : 'It moved to Completed.'} ${notes.join(' ')}`
    })
  }
  return (
    <Modal title={`Export ${quoted(d.doc.name)}?`} onClose={close}>
      <p>
        Lindley will combine the {plural(d.doc.pages, 'page')} in their current order into one searchable PDF, with everything it read from the scans saved inside it.{' '}
        {folder ? `The document stays in ${folder.name} and is marked Completed.` : 'The document moves to Completed.'}
      </p>
      {d.toReview > 0 && (
        <div className="rv-state flag">
          <Icon name="flag" />
          <div>
            <b>
              {plural(d.toReview, 'page')} still {d.toReview === 1 ? 'needs' : 'need'} your review.
            </b>{' '}
            The PDF’s searchable text will use Lindley’s best guess for {d.toReview === 1 ? 'it' : 'them'}.
          </div>
        </div>
      )}
      {d.doc.suggested && <p>The name was suggested by Lindley. You can rename it first by clicking the title.</p>}
      <p>You can reopen it later to make changes.</p>
      <div className="actions">
        <button className="btn" onClick={close} data-tip="Close without exporting">
          Cancel
        </button>
        {d.toReview > 0 && (
          <button
            className="btn"
            autoFocus
            data-tip="Check the flagged pages first, then come back to export"
            onClick={() => {
              close()
              nav(`/review/${d.doc.id}`)
            }}
          >
            Review first
          </button>
        )}
        <button className="btn primary" autoFocus={!d.toReview} onClick={go} data-tip="Make the searchable PDF in your library’s Exports folder">
          <Icon name="pdf" /> Export PDF
        </button>
      </div>
    </Modal>
  )
}

function DetailsDialog({ d, close }: { d: Extract<Dialog, { kind: 'details' }>; close: () => void }) {
  const { run } = useFeedback()
  const [type, setType] = useState(d.doc.doc_type ?? '')
  const [date, setDate] = useState(d.doc.doc_date ?? '')
  const bad = !!date.trim() && !/^\d{4}(-\d{2}(-\d{2})?)?$/.test(date.trim())
  const save = () => {
    if (bad) return
    close()
    run(api.updateDocument(d.doc.id, { doc_type: type, doc_date: date }), 'Saved the type and date.')
  }
  return (
    <Modal title="Type and date" onClose={close}>
      <p>What kind of document {quoted(d.doc.name)} is, and when it was written. Lindley only suggests changes to what you give here.</p>
      <label className="fld" htmlFor="dt-type">
        Type
      </label>
      <input className="fld-in" id="dt-type" value={type} onChange={(e) => setType(e.target.value)} placeholder="For example: Letter, Deed, Diary" list="dt-types" />
      <datalist id="dt-types">
        {['Letter', 'Deed', 'Diary', 'Receipt', 'Certificate', 'Manuscript', 'Notes', 'Newspaper clipping', 'Photograph'].map((t) => (
          <option key={t} value={t} />
        ))}
      </datalist>
      <label className="fld" htmlFor="dt-date">
        Date
      </label>
      <input
        className="fld-in"
        id="dt-date"
        value={date}
        onChange={(e) => setDate(e.target.value)}
        placeholder="1892, 1892-03 or 1892-03-14"
        aria-describedby="dt-dateh"
        aria-invalid={bad}
      />
      <p className="fld-note" id="dt-dateh">
        {bad ? 'Write the year, then the month and day if you know them: 1892, 1892-03 or 1892-03-14.' : 'As much as you know: a year, a month, or a day.'}
      </p>
      <div className="actions">
        <button className="btn" onClick={close}>
          Cancel
        </button>
        <button className="btn primary" onClick={save} disabled={bad} data-tip={bad ? 'Fix the date first' : 'Save the type and date'}>
          Save
        </button>
      </div>
    </Modal>
  )
}

function AddDialog({ files, close, go }: { files: File[]; close: () => void; go: () => void }) {
  const n = files.length
  return (
    <Modal title={`Add ${plural(n, 'scan')} to the Inbox`} onClose={close}>
      <ul className="file-list">
        {files.slice(0, 6).map((f, i) => (
          <li key={i} className="mono">
            {f.name}
          </li>
        ))}
        {n > 6 && <li>and {n - 6} more</li>}
      </ul>
      <p>
        Lindley keeps its own copy of {n === 1 ? 'the scan' : 'each scan'} in its library. The {n === 1 ? 'file stays' : 'files stay'} where{' '}
        {n === 1 ? 'it is' : 'they are'} on your computer: a browser can copy files, but never move or delete them.
      </p>
      <p>Either way, Lindley never changes the scans themselves.</p>
      <div className="actions">
        <button className="btn" onClick={close}>
          Cancel
        </button>
        <button className="btn primary" onClick={go} autoFocus data-tip="Upload the files and start reading them">
          <Icon name="upload" /> Add {plural(n, 'scan')}
        </button>
      </div>
    </Modal>
  )
}

