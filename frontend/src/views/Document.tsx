// A document, in three views: its pages, a reader, and each scan beside its text. Name it,
// date it, put its pages in order, then export it as a searchable PDF.

import { type CSSProperties, useEffect, useRef, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router'
import { api, type DocFull, imageAt, type Page } from '../api/client'
import { useApi } from '../api/store'
import { textOf, typing } from '../lib/view'
import { Banner, ConfChip, ErrorBox, Loading, Meter, NameInput } from '../components/bits'
import { PageGrid } from '../components/PageGrid'
import { SizeControl } from '../components/SizeControl'
import { TextPanel } from '../components/TextPanel'
import { useActions } from '../lib/actionsContext'
import { useSelection, useThumb } from '../lib/view'
import { useApp, useLooking } from '../lib/appContext'
import { who } from '../lib/ai'
import { docDate, needs, plural, them, when } from '../lib/words'
import { DBtn, Dock, DockText, Sep } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'

type View = 'grid' | 'reader' | 'side'
const TABS: [View, string, string][] = [
  ['grid', 'Pages', 'Every page at a glance: select, reorder and move them'],
  ['reader', 'Reader', 'Read the pages one at a time, large'],
  ['side', 'Scan and text', 'Each scan beside what Lindley read from it, to check and correct'],
]


export function DocumentView() {
  const docId = Number(useParams().id)
  const doc = useApi(`doc:${docId}`, () => api.document(docId))
  const [params, setParams] = useSearchParams()
  const view = (['grid', 'reader', 'side'].includes(params.get('view') ?? '') ? params.get('view') : 'grid') as View
  const d = doc.data
  const n = d?.pages.length ?? 0
  const cur = Math.max(0, Math.min(Number(params.get('page') ?? 0) || 0, n - 1))
  const setView = (v: View, page = cur) => setParams({ view: v, page: String(page) }, { replace: true })
  const setCur = (i: number) => setParams({ view, page: String(Math.max(0, Math.min(i, n - 1))) }, { replace: true })

  useLooking(d ? (view !== 'grid' && n ? `${d.name}, page ${cur + 1}` : d.name) : 'A document')

  // ← and → move between pages in the reader and beside the text
  useEffect(() => {
    if (view === 'grid') return
    const onKey = (e: KeyboardEvent) => {
      if (typing(e.target) || (e.target as Element).matches?.('.tab') || document.querySelector('dialog[open], .menu')) return
      if (e.key === 'ArrowRight' || e.key === 'PageDown') setCur(cur + 1)
      if (e.key === 'ArrowLeft' || e.key === 'PageUp') setCur(cur - 1)
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  })

  if (doc.error && !d) return <ErrorBox error={doc.error} />
  if (!d) return <Loading />
  return <DocBody d={d} view={view} cur={cur} setView={setView} setCur={setCur} />
}

function DocBody({ d, view, cur, setView, setCur }: { d: DocFull; view: View; cur: number; setView: (v: View, p?: number) => void; setCur: (i: number) => void }) {
  const { folderPath, ai } = useApp()
  const acts = useActions()
  const { run } = useFeedback()
  const nav = useNavigate()
  const waiting = useApi('needs-ai', api.needsAi)
  const [sel, setSel] = useSelection()
  const [thumb] = useThumb()
  const [zoom, setZoom] = useState(1)

  const done = d.status === 'complete'
  const order = d.pages.map((p) => p.id)
  const page = d.pages[cur]
  const toReview = d.pages.filter((p) => p.state === 'review')
  const toRead = (waiting.data?.read.pages ?? []).filter((r) => r.document_id === d.id).map((r) => r.page_id)
  const crumbs = d.folder_id != null ? ['My folders', ...folderPath(d.folder_id)] : [done ? 'Completed' : 'In progress']
  const targets = view === 'grid' ? order.filter((id) => sel.has(id)) : page ? [page.id] : []
  const t = targets.length
  const checksLeft = d.progress.checks.filter((c) => !c.done && c.key !== 'exported')

  const reorderDrop = (ids: number[], target: number, after: boolean) => {
    if (ids.includes(target)) return
    const rest = order.filter((x) => !ids.includes(x))
    rest.splice(rest.indexOf(target) + (after ? 1 : 0), 0, ...order.filter((x) => ids.includes(x)))
    run(api.reorder(d.id, rest), `Moved ${plural(ids.length, 'page')}.`)
  }
  const exportIt = () =>
    acts.confirmExport({ id: d.id, name: d.name, pages: d.pages.length, suggested: d.suggested, folder_id: d.folder_id }, toReview.length)

  return (
    <>
      <div className="head">
        <div className="crumbs">{crumbs.join(' › ')}</div>
        <div className="title-row">
          <h1 className="sr-only">{d.name}</h1>
          <NameInput
            key={d.name}
            value={d.name}
            className={`title-in${d.suggested ? ' is-temp' : ''}`}
            readOnly={done}
            label="Document name"
            tip={done ? 'Completed documents can’t be renamed. Reopen it to change it.' : 'Click to rename the document. Enter saves, Escape puts it back.'}
            onSave={(v) => run(api.updateDocument(d.id, { name: v }), 'Renamed the document.')}
          />
        </div>
        <div className="meta">
          {d.suggested && (
            <span className="chip ai" data-tip="Lindley made up this name from the pages. It stays in italics until you rename it or export the document.">
              <Mark /> Name suggested by Lindley. Click it to rename.
            </span>
          )}
          <span className="chip">{plural(d.pages.length, 'page')}</span>
          <button
            className="chip chip-btn"
            disabled={done}
            onClick={() => acts.editDetails(d)}
            data-tip={done ? 'Reopen the document to change its type' : 'Say what kind of document this is'}
          >
            {d.doc_type ? d.doc_type[0].toUpperCase() + d.doc_type.slice(1) : 'Add a type'}
          </button>
          <button
            className="chip chip-btn"
            disabled={done}
            onClick={() => acts.editDetails(d)}
            data-tip={done ? 'Reopen the document to change its date' : d.date_source === 'lindley' ? 'Lindley found this date on the pages. Click to change it.' : 'When the document was written. Click to change it.'}
          >
            {docDate(d.doc_date) || 'Add a date'}
          </button>
          {!done && (
            <span
              className="chip"
              data-tip={d.progress.checks.map((c) => `${c.done ? '✓' : '○'} ${c.label}${c.detail && !c.done ? `: ${c.detail}` : ''}`).join('\n')}
            >
              <Icon name={checksLeft.length ? 'flag' : 'checkc'} />
              {d.progress.checks.filter((c) => c.done).length} of {d.progress.checks.length} done
            </span>
          )}
          {!done && d.confidence != null && (
            <Meter
              value={d.confidence}
              label="Grouping confidence"
              tip={`How sure Lindley is that these pages belong together${d.reasons.length ? `: ${d.reasons.join('. ')}` : ''}`}
            />
          )}
        </div>
        {done && (
          <Banner
            kind="ok"
            actions={
              <a className="btn" href={`/api/documents/${d.id}/export`} download data-tip="Download the searchable PDF from your library’s Exports folder">
                <Icon name="pdf" /> Download PDF
              </a>
            }
          >
            <b>Completed {when(d.export?.exported_at, false)}.</b> Exported as a searchable PDF: <span className="mono">{d.export?.file_name}</span>
          </Banner>
        )}
        {!done && toRead.length > 0 && (
          <Banner
            kind="ai"
            actions={
              <>
                <button
                  className="btn ai"
                  data-tip={`Send ${them(toRead.length)} to ${who(ai('vision'))} to read now. Sending is your OK.`}
                  onClick={() => run(api.readWithAi(toRead), (r) => `The AI read ${plural(r.read, 'page')}${r.failed ? `; ${r.failed} failed` : ''}.`)}
                >
                  <Mark /> Ask the AI
                </button>
                <button className="btn ghost" onClick={() => nav('/needs-ai')} data-tip="See everything waiting for an AI">
                  See what’s waiting
                </button>
              </>
            }
          >
            <b>
              {plural(toRead.length, 'page')} {needs(toRead.length)} an AI to read {them(toRead.length)}.
            </b>{' '}
            Tesseract read {them(toRead.length)} with low confidence, so {toRead.length === 1 ? 'its' : 'their'} text is only a rough guess.
          </Banner>
        )}
        {!done && toReview.length > 0 && (
          <Banner
            kind="review"
            actions={
              <button className="btn" onClick={() => nav(`/review/${d.id}`)} data-tip="Check the flagged pages one by one">
                Review next
              </button>
            }
          >
            <b>
              {plural(toReview.length, 'page')} {needs(toReview.length)} your review.
            </b>{' '}
            Lindley isn’t sure it read {them(toReview.length)} correctly.
          </Banner>
        )}
        {!done && d.progress.ready && (
          <Banner
            kind="ok"
            actions={
              <button className="btn" onClick={exportIt} data-tip="Look it over, then make the searchable PDF">
                Review and export
              </button>
            }
          >
            <b>Lindley thinks this document is complete.</b> Every page is read and checked, and it has a name, a date and a type. Look it over, then export it
            when you’re ready.
          </Banner>
        )}
        {!done && d.inbox_hints.length > 0 && (
          <Banner
            kind="ai"
            actions={
              <>
                <button className="btn" onClick={() => acts.moveTo(d.inbox_hints, d)} data-tip="Add them to the end of this document">
                  Add {them(d.inbox_hints.length)}
                </button>
                <button
                  className="btn ghost"
                  onClick={() => nav('/inbox', { state: { select: d.inbox_hints, sort: 'match' } })}
                  data-tip="Go to the Inbox with them selected"
                >
                  Show me first
                </button>
              </>
            }
          >
            Lindley thinks {plural(d.inbox_hints.length, 'scan')} in your Inbox might belong here.
          </Banner>
        )}
        <div className="tabrow">
          <div className="tabs" role="tablist" aria-label="Views">
            {TABS.map(([k, l, tip]) => (
              <button
                key={k}
                className="tab"
                role="tab"
                aria-selected={view === k}
                tabIndex={view === k ? 0 : -1}
                data-tip={tip}
                onClick={() => setView(k)}
                onKeyDown={(e) => {
                  if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return
                  const i = TABS.findIndex((x) => x[0] === k)
                  const next = TABS[(i + (e.key === 'ArrowRight' ? 1 : TABS.length - 1)) % TABS.length][0]
                  setView(next)
                  requestAnimationFrame(() => document.querySelector<HTMLElement>('.tab[aria-selected="true"]')?.focus())
                }}
              >
                {l}
              </button>
            ))}
          </div>
          {view === 'grid' && !done && (
            <div className="viewbar">
              <button
                className="btn ghost"
                onClick={() => setSel(sel.size === order.length ? new Set() : new Set(order))}
                data-tip={sel.size === order.length ? 'Unselect every page' : 'Select every page of the document'}
              >
                {sel.size === order.length ? 'Clear selection' : 'Select all'}
              </button>
              <SizeControl />
            </div>
          )}
        </div>
      </div>

      {view === 'grid' && (
        <div className="scroll">
          <PageGrid
            pages={d.pages}
            numbered
            sel={sel}
            setSel={setSel}
            thumb={thumb}
            onOpen={(_, i) => {
              setSel(new Set())
              setView('reader', i)
            }}
            onReview={(p) => nav(`/review/${d.id}`, { state: { page: p.id } })}
            onReorder={done ? undefined : reorderDrop}
            onMenu={done ? undefined : (ids, at, anchor) => acts.pageMenu(ids, 'document', anchor, { at, docId: d.id, order })}
            empty="This document has no pages."
          />
        </div>
      )}
      {view === 'reader' && (page ? <Reader d={d} cur={cur} setCur={setCur} zoom={zoom} setZoom={setZoom} /> : <div className="empty">This document has no pages.</div>)}
      {view === 'side' && (page ? <Side d={d} cur={cur} setCur={setCur} /> : <div className="empty">This document has no pages.</div>)}

      <Dock label="Document actions">
        <DBtn icon="folder" label="Move to folder…" tip="File this document in one of your folders" onClick={(e) => acts.folderMenu(d, e.currentTarget)} />
        {done ? (
          <DBtn
            icon="back"
            label="Reopen for changes"
            tip="Make the document editable again. The exported PDF is kept until you export again."
            onClick={() => run(api.reopen(d.id), `Reopened “${d.name}”. The exported PDF is kept until you export again.`)}
          />
        ) : (
          <>
            <DBtn
              icon="pdf"
              label="Export PDF"
              kind="primary"
              tip="Make one searchable PDF of the pages in this order, in your library’s Exports folder"
              onClick={exportIt}
            />
            <Sep />
            <DockText>{view === 'grid' ? (t ? `${t} selected` : 'None selected') : `Page ${cur + 1}`}</DockText>
            <DBtn icon="rotL" label="Turn left" iconOnly disabled={!t} tip={t ? 'Turn a quarter turn left' : 'Select pages to turn them'} onClick={() => acts.rotate(targets, -90)} />
            <DBtn icon="rotR" label="Turn right" iconOnly disabled={!t} tip={t ? 'Turn a quarter turn right' : 'Select pages to turn them'} onClick={() => acts.rotate(targets, 90)} />
            <DBtn
              icon="up"
              label="Move earlier"
              iconOnly
              disabled={!t}
              tip={t ? 'Move one place earlier in the document' : 'Select pages to move them'}
              onClick={() => {
                acts.shift(d.id, order, targets, -1)
                if (view !== 'grid') setCur(cur - 1)
              }}
            />
            <DBtn
              icon="down"
              label="Move later"
              iconOnly
              disabled={!t}
              tip={t ? 'Move one place later in the document' : 'Select pages to move them'}
              onClick={() => {
                acts.shift(d.id, order, targets, 1)
                if (view !== 'grid') setCur(cur + 1)
              }}
            />
            <DBtn
              icon="more"
              label="More…"
              disabled={!t}
              tip={t ? 'Move to another document, a new one, the Inbox or Set aside' : 'Select pages for more actions'}
              onClick={(e) => acts.pageMenu(targets, 'document', e.currentTarget, { docId: d.id, order, noBasics: true })}
            />
          </>
        )}
      </Dock>
    </>
  )
}

function Pager({ n, cur, setCur, what = 'Page' }: { n: number; cur: number; setCur: (i: number) => void; what?: string }) {
  return (
    <>
      <button className="icon-btn" disabled={cur === 0} onClick={() => setCur(cur - 1)} aria-label={`Previous ${what.toLowerCase()}`} data-tip={`Previous ${what.toLowerCase()} (←)`}>
        <Icon name="prev" />
      </button>
      <span>
        {what} {cur + 1} of {n}
      </span>
      <button className="icon-btn" disabled={cur >= n - 1} onClick={() => setCur(cur + 1)} aria-label={`Next ${what.toLowerCase()}`} data-tip={`Next ${what.toLowerCase()} (→)`}>
        <Icon name="chev" />
      </button>
    </>
  )
}

export function Strip({ pages, cur, setCur, label, flag = true }: { pages: Page[]; cur: number; setCur: (i: number) => void; label: string; flag?: boolean }) {
  const ref = useRef<HTMLDivElement>(null)
  useEffect(() => {
    ref.current?.querySelector('[aria-current="true"]')?.scrollIntoView({ block: 'nearest', inline: 'nearest' })
  }, [cur])
  return (
    <div className="strip" ref={ref} role="group" aria-label={label}>
      {pages.map((p, i) => {
        const review = flag && p.state === 'review'
        return (
          <button
            key={p.id}
            aria-current={i === cur}
            aria-label={`Page ${i + 1}${review ? ', needs review' : ''}`}
            data-tip={`Page ${i + 1}${review ? ': needs your review' : ''}`}
            onClick={() => setCur(i)}
          >
            <span className="pg-img">
              <img src={imageAt(p.image, 160)} alt="" loading="lazy" />
            </span>
            <span>
              {review && <Icon name="flag" />}
              {i + 1}
            </span>
          </button>
        )
      })}
    </div>
  )
}

function Reader({ d, cur, setCur, zoom, setZoom }: { d: DocFull; cur: number; setCur: (i: number) => void; zoom: number; setZoom: (z: number) => void }) {
  const p = d.pages[cur]
  const nav = useNavigate()
  return (
    <div className="reader" style={{ minHeight: 0, flex: 1 }}>
      <div className="rd-bar">
        <Pager n={d.pages.length} cur={cur} setCur={setCur} />
        <span className="sep" style={{ width: 1, height: 18, background: 'var(--line)', margin: '0 6px' }} />
        <button className="icon-btn" onClick={() => setZoom(Math.max(0.5, +(zoom - 0.25).toFixed(2)))} aria-label="Zoom out" data-tip="Zoom out" disabled={zoom <= 0.5}>
          <Icon name="zout" />
        </button>
        <span>{Math.round(zoom * 100)}%</span>
        <button className="icon-btn" onClick={() => setZoom(Math.min(3, +(zoom + 0.25).toFixed(2)))} aria-label="Zoom in" data-tip="Zoom in" disabled={zoom >= 3}>
          <Icon name="zin" />
        </button>
        <button className="btn ghost" onClick={() => setZoom(1)} data-tip="Fit the page to the window">
          Fit
        </button>
        <span className="spacer" />
        <ConfChip p={p} onReview={() => nav(`/review/${d.id}`, { state: { page: p.id } })} />
        <span className="mono" data-tip="The scan file this page came from">
          {p.file}
        </span>
      </div>
      <div className="rd-stage">
        <img src={imageAt(p.image, 2000)} alt={`Page ${cur + 1}`} style={{ '--z': zoom } as CSSProperties} />
      </div>
      <Strip pages={d.pages} cur={cur} setCur={setCur} label="Pages" />
    </div>
  )
}

function Side({ d, cur, setCur }: { d: DocFull; cur: number; setCur: (i: number) => void }) {
  const p = d.pages[cur]
  const full = useApi(`page:${p.id}`, () => api.page(p.id))
  const { run } = useFeedback()
  const text = useRef<HTMLDivElement>(null)
  const done = d.status === 'complete'
  const nextReview = d.pages.findIndex((x, i) => i !== cur && x.state === 'review')
  return (
    <div className="side" style={{ flex: 1, minHeight: 0 }}>
      <div className="side-img">
        <img src={imageAt(p.image, 1600)} alt={`Page ${cur + 1}`} />
      </div>
      <div className="side-tx">
        {full.data && full.data.id === p.id ? (
          <TextPanel
            page={full.data}
            textRef={text}
            label={`page ${cur + 1}`}
            readonly={done}
            pager={<Pager n={d.pages.length} cur={cur} setCur={setCur} />}
            actions={
              <>
                {full.data.state === 'review' || full.data.state === 'needs_ai' || full.data.state === 'ai_failed' ? (
                  <>
                    <button className="btn primary" onClick={() => run(api.checkText(p.id), 'Marked the page as checked.')} data-tip="The text matches the scan: trust it for search and chat">
                      <Icon name="check" /> The text is correct
                    </button>
                    <button
                      className="btn"
                      onClick={() => run(api.checkText(p.id, textOf(text.current)), 'Saved your correction and marked the page as checked.')}
                      data-tip="Keep the text as you’ve corrected it. Lindley’s reading is kept too."
                    >
                      Save my correction
                    </button>
                  </>
                ) : (
                  <button
                    className="btn"
                    onClick={() => run(api.checkText(p.id, textOf(text.current)), 'Saved the transcription.')}
                    data-tip="Keep the text as it is in the box. Earlier readings are kept too."
                  >
                    Save transcription
                  </button>
                )}
                {nextReview >= 0 && (
                  <button className="btn ghost" onClick={() => setCur(nextReview)} data-tip="Go to the next page of this document that needs your review">
                    Next page to review
                  </button>
                )}
              </>
            }
          />
        ) : full.error ? (
          <ErrorBox error={full.error} />
        ) : (
          <Loading what="Reading the page…" />
        )}
      </div>
    </div>
  )
}

