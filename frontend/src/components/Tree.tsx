// The tree on the left: every place a page or document can be, with what's waiting in each.
// Pages drop onto a document, the Inbox or Set aside; documents drop onto a folder, or onto In
// progress or Completed to take them out of one.

import { type CSSProperties, type ReactNode, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'
import type { DocSummary, Folder } from '../api/client'
import { useLocal } from '../api/store'
import { useActions } from '../lib/actionsContext'
import { useApp } from '../lib/appContext'
import { drag } from '../lib/drag'
import { plural, when } from '../lib/words'
import { Icon, Mark } from '../ui/icons'

type Drop = { kind: 'doc'; id: number } | { kind: 'inbox' } | { kind: 'aside' } | { kind: 'folder'; id: number } | { kind: 'unfile' }

interface FolderStats {
  docs: number
  progress: number
  ready: number
  complete: number
  review: number
}

export function Tree({ onGo }: { onGo: () => void }) {
  const { overview, docs, folders, settings } = useApp()
  const acts = useActions()
  const loc = useLocation()
  const nav = useNavigate()
  const [closed, setClosed] = useLocal<Record<string, boolean>>('closed', {})
  const [over, setOver] = useState<string | null>(null)
  const c = overview?.counts

  const path = loc.pathname
  const on = (p: string) => path === p || path.startsWith(`${p}/`)
  const go = (to: string) => {
    nav(to)
    onGo()
  }

  const accept = (d: Drop, e: React.DragEvent) => {
    const docDrop = d.kind === 'folder' || d.kind === 'unfile'
    if (docDrop ? drag.doc == null : !drag.pages) return false
    if (d.kind === 'doc' && path === `/documents/${d.id}`) return false
    if (d.kind === 'inbox' && path === '/inbox') return false
    if (d.kind === 'aside' && path === '/aside') return false
    if (d.kind === 'doc' && docs.get(d.id)?.status === 'complete') return false
    e.preventDefault()
    return true
  }
  const dropProps = (key: string, d: Drop) => ({
    onDragOver: (e: React.DragEvent) => {
      if (accept(d, e)) setOver(key)
    },
    onDragLeave: () => setOver((o) => (o === key ? null : o)),
    onDrop: (e: React.DragEvent) => {
      setOver(null)
      if (!accept(d, e)) return
      if (d.kind === 'folder' || d.kind === 'unfile') {
        const doc = drag.doc != null ? docs.get(drag.doc) : undefined
        drag.doc = null
        if (doc) acts.fileDoc(doc, d.kind === 'folder' ? d.id : null)
        return
      }
      const ids = drag.pages ?? []
      drag.pages = null
      if (d.kind === 'doc') {
        const doc = docs.get(d.id)
        if (doc) acts.moveTo(ids, doc)
      } else if (d.kind === 'inbox') acts.toInbox(ids)
      else acts.setAside(ids)
    },
  })

  const stats = (fid: number): FolderStats => {
    const st = { docs: 0, progress: 0, ready: 0, complete: 0, review: 0 }
    docs.forEach((d) => {
      if (d.folder_id !== fid) return
      st.docs++
      if (d.status === 'complete') st.complete++
      else {
        st.progress++
        if (d.ready) st.ready++
        st.review += d.to_review
      }
    })
    ;[...folders.values()]
      .filter((f) => f.parent_id === fid)
      .forEach((k) => {
        const s = stats(k.id)
        ;(Object.keys(st) as (keyof FolderStats)[]).forEach((x) => (st[x] += s[x]))
      })
    return st
  }

  const meta = (parts: ReactNode[]) => {
    const shown = parts.filter(Boolean)
    return (
      <span className="t-meta">
        {shown.map((p, i) => (
          <span key={i} style={{ display: 'contents' }}>
            {i > 0 && (
              <span className="t-dot" aria-hidden="true">
                ·
              </span>
            )}
            {p}
          </span>
        ))}
      </span>
    )
  }
  const flag = (n: number) =>
    n ? (
      <span key="flag" className="t-flag">
        <Icon name="flag" />
        {n} to review
      </span>
    ) : null
  const aiFlag = (n: number) =>
    n ? (
      <span key="ai" className="t-flag">
        <Mark />
        {n} need{n === 1 ? 's' : ''} AI
      </span>
    ) : null

  const docItem = (d: DocSummary, depth = 0) => {
    const done = d.status === 'complete'
    const key = `doc:${d.id}`
    const active = on(`/documents/${d.id}`)
    return (
      <button
        key={d.id}
        className={`t-item t2${active ? ' is-active' : ''}${d.suggested ? ' is-temp' : ''}${over === key ? ' is-drop' : ''}`}
        aria-current={active ? 'page' : undefined}
        style={{ '--d': depth } as CSSProperties}
        onClick={() => go(`/documents/${d.id}`)}
        draggable
        onDragStart={(e) => {
          drag.doc = d.id
          e.dataTransfer.effectAllowed = 'move'
          e.dataTransfer.setData('text/plain', d.name)
        }}
        onDragEnd={() => (drag.doc = null)}
        {...(done ? {} : dropProps(key, { kind: 'doc', id: d.id }))}
        data-tip={`${d.name}${d.suggested ? ' (name suggested by Lindley)' : ''}. ${plural(d.pages, 'page')}${
          d.to_review ? `, ${d.to_review} to review` : ''
        }${d.ready ? '. Lindley thinks it’s ready to export' : ''}. Drag it onto a folder to file it.`}
      >
        <Icon name={done ? 'checkc' : 'doc'} />
        <span className="t-txt">
          <span className="t-name">
            {d.name}
            {d.suggested && <span className="sr-only"> (name suggested by Lindley)</span>}
          </span>
          {meta([
            flag(d.to_review),
            aiFlag(d.needs_ai),
            d.ready && !done ? (
              <span key="ready" className="t-ready" data-tip="Lindley thinks this document is complete and ready to export">
                Ready
              </span>
            ) : null,
            done ? <span key="done">Completed {when(d.exported_at, false)}</span> : null,
            <span key="pages">{plural(d.pages, 'page')}</span>,
          ])}
        </span>
      </button>
    )
  }

  const folderItem = (f: Folder, depth = 0): ReactNode => {
    const st = stats(f.id)
    const kids = [...folders.values()].filter((k) => k.parent_id === f.id)
    const key = `folder:${f.id}`
    const active = on(`/folders/${f.id}`)
    const summary = [
      plural(st.docs, 'document'),
      st.progress ? `${st.progress} in progress${st.ready ? ` (${st.ready} ready to export)` : ''}` : '',
      st.complete ? `${st.complete} completed` : '',
      st.review ? `${plural(st.review, 'page')} to review` : '',
    ]
      .filter(Boolean)
      .join(', ')
    return (
      <div key={f.id} style={{ display: 'contents' }}>
        <button
          className={`t-item t2${active ? ' is-active' : ''}${over === key ? ' is-drop' : ''}`}
          aria-current={active ? 'page' : undefined}
          style={{ '--d': depth } as CSSProperties}
          onClick={() => go(`/folders/${f.id}`)}
          {...dropProps(key, { kind: 'folder', id: f.id })}
          data-tip={`${f.name}: ${summary}. Drop a document here to file it.`}
        >
          <Icon name="folder" />
          <span className="t-txt">
            <span className="t-name">{f.name}</span>
            {meta([flag(st.review), kids.length ? <span key="kids">{plural(kids.length, 'subfolder')}</span> : null, <span key="docs">{plural(st.docs, 'document')}</span>])}
          </span>
        </button>
        {kids.map((k) => folderItem(k, depth + 1))}
        {[...docs.values()].filter((d) => d.folder_id === f.id).map((d) => docItem(d, depth + 1))}
      </div>
    )
  }

  const section = (key: string, label: string, n: number | null, inner: ReactNode, tip: string, unfile = false) => {
    const open = !closed[key]
    const dkey = `sec:${key}`
    return (
      <div className="t-sec">
        <button
          className={`t-sec-h${over === dkey ? ' is-drop' : ''}`}
          aria-expanded={open}
          onClick={() => setClosed({ ...closed, [key]: open })}
          data-tip={`${tip} Click to ${open ? 'hide' : 'show'} them.`}
          {...(unfile ? dropProps(dkey, { kind: 'unfile' }) : {})}
        >
          <Icon name="chev" className="chev" />
          <span>{label}</span>
          {n !== null && <span className="count">{n}</span>}
        </button>
        <div className="t-list" hidden={!open}>
          {inner}
        </div>
      </div>
    )
  }

  const all = [...docs.values()]
  const prog = all.filter((d) => d.status === 'progress' && d.folder_id == null)
  const done = all.filter((d) => d.status === 'complete' && d.folder_id == null)
  const roots = [...folders.values()].filter((f) => f.parent_id == null)
  const count = (n: number | undefined, strong: boolean, what: string) => (
    <span className={`count${strong ? ' strong' : ''}`}>
      <span className="sr-only">, </span>
      {n ?? '…'}
      <span className="sr-only"> {what}</span>
    </span>
  )

  return (
    <nav className="tree" aria-label="Folders">
      <button
        className={`t-item t-inbox${on('/inbox') ? ' is-active' : ''}${over === 'inbox' ? ' is-drop' : ''}`}
        aria-current={on('/inbox') ? 'page' : undefined}
        onClick={() => go('/inbox')}
        {...dropProps('inbox', { kind: 'inbox' })}
        data-tip="New scans land here until they’re part of a document. Drop pages here to have Lindley sort them again."
      >
        <Icon name="inbox" />
        <span className="t-name">Inbox</span>
        {count(c?.inbox, true, 'scans')}
      </button>
      <button
        className={`t-item t-review${on('/review') ? ' is-active' : ''}`}
        aria-current={on('/review') ? 'page' : undefined}
        onClick={() => go('/review')}
        data-tip={`Pages Lindley read with less than ${settings?.ocr.review_below ?? 80}% confidence, for you to check before they’re trusted for search and chat.`}
      >
        <Icon name={c?.review ? 'flag' : 'checkc'} />
        <span className="t-name">Needs your review</span>
        {count(c?.review, true, 'pages')}
      </button>
      <button
        className={`t-item${on('/duplicates') ? ' is-active' : ''}`}
        aria-current={on('/duplicates') ? 'page' : undefined}
        onClick={() => go('/duplicates')}
        data-tip="Pages that look like they were scanned more than once. Compare the copies and choose what to keep."
      >
        <Icon name="dup" />
        <span className="t-name">Duplicates</span>
        {count(c?.duplicates, !!c?.duplicates, 'to decide')}
      </button>
      <button
        className={`t-item${on('/needs-ai') ? ' is-active' : ''}`}
        aria-current={on('/needs-ai') ? 'page' : undefined}
        onClick={() => go('/needs-ai')}
        data-tip="Scans Lindley couldn’t read or sort well enough on its own, waiting for an AI to look at them."
      >
        <Mark />
        <span className="t-name">Needs AI</span>
        {count(c?.needs_ai, !!c?.needs_ai, 'scans waiting for an AI')}
      </button>
      {section(
        'progress',
        'In progress',
        prog.length,
        prog.length ? prog.map((d) => docItem(d)) : <p className="count" style={{ padding: '4px 12px' }}>Nothing in progress</p>,
        'Documents being put together, not in a folder of yours. Drop a document here to take it out of its folder.',
        true,
      )}
      {section(
        'complete',
        'Completed',
        done.length,
        done.length ? done.map((d) => docItem(d)) : <p className="count" style={{ padding: '4px 12px' }}>Nothing here</p>,
        'Documents exported as searchable PDFs, not in a folder of yours.',
        true,
      )}
      <button
        className={`t-item${on('/aside') ? ' is-active' : ''}${over === 'aside' ? ' is-drop' : ''}`}
        aria-current={on('/aside') ? 'page' : undefined}
        style={{ marginTop: 10 }}
        onClick={() => go('/aside')}
        {...dropProps('aside', { kind: 'aside' })}
        data-tip="Receipts, blank pages and other scans that aren’t part of a document. Nothing here is deleted. Drop pages here to set them aside."
      >
        <Icon name="aside" />
        <span className="t-name">Set aside</span>
        {count(c?.aside, false, 'scans')}
      </button>
      {section(
        'folders',
        'My folders',
        null,
        <>
          {roots.map((f) => folderItem(f))}
          <button className="t-add" onClick={() => acts.newFolder(null)} data-tip="Make a folder to file documents in">
            <Icon name="plus" /> New folder
          </button>
        </>,
        'Folders you make to file documents in.',
      )}
      <div className="legend">
        <span>
          <Icon name="flag" /> Pages that need your review
        </span>
        <span>
          <Mark /> Scans that need an AI to look at them
        </span>
        <span>
          <Icon name="checkc" /> Completed and exported
        </span>
        <span>
          <i>Italic names</i> were suggested by Lindley
        </span>
      </div>
      <button
        className={`t-item${on('/settings') ? ' is-active' : ''}`}
        aria-current={on('/settings') ? 'page' : undefined}
        style={{ marginTop: 8 }}
        onClick={() => go('/settings/ai')}
        data-tip="AI connections and privacy, watched folders, reading and review, your library, and how Lindley looks"
      >
        <Icon name="gear" />
        <span className="t-name">Settings</span>
      </button>
    </nav>
  )
}
