// Duplicates: pages (or whole documents) scanned more than once, found by what they say. Compare
// the copies side by side and choose what to keep; copies you don't keep are set aside, never
// deleted.

import { type CSSProperties, useEffect, useRef, useState } from 'react'
import { useNavigate, useParams } from 'react-router'
import { api, type DupCopy, type DupDocPair, type Duplicates, type DupSet, imageAt } from '../api/client'
import { useApi } from '../api/store'
import { Banner, Crumb, Done, ErrorBox, Head, Loading } from '../components/bits'
import { docHome, useApp, useLooking } from '../lib/appContext'
import { plural, quoted, when } from '../lib/words'
import { DBtn, Dock, DockProgress, Sep } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'

interface Entry {
  key: string
  kind: 'same_page' | 'similar'
  set?: DupSet
  pair?: DupDocPair
}

/** A document scanned twice is one entry; every other set is an entry of its own. */
function entries(d: Duplicates): Entry[] {
  const inPair = new Set(d.documents.flatMap((p) => p.sets))
  return [
    ...d.documents.map((p) => ({ key: `d${p.documents[0]}-${p.documents[1]}`, kind: 'same_page' as const, pair: p })),
    ...d.sets.filter((s) => !inPair.has(s.id)).map((s) => ({ key: `s${s.id}`, kind: s.kind, set: s })),
  ]
}

const copyWhere = (c: DupCopy) => (c.where === 'document' ? `In ${quoted(c.document_name ?? '')}` : c.where === 'aside' ? 'In Set aside' : 'In the Inbox')

export function DupList() {
  const dups = useApi('duplicates', api.duplicates)
  const nav = useNavigate()
  useLooking('Duplicates')
  if (dups.error && !dups.data) return <ErrorBox error={dups.error} />
  if (!dups.data) return <Loading />
  const es = entries(dups.data)
  const setOf = (id: number) => dups.data!.sets.find((s) => s.id === id)
  const card = (e: Entry) => {
    let title: string
    let what: React.ReactNode
    let where: string
    let thumbs: string[]
    let sug: React.ReactNode = null
    if (e.pair) {
      const sets = e.pair.sets.map(setOf).filter(Boolean) as DupSet[]
      const extra = Object.values(e.pair.extra).reduce((n, x) => n + x.length, 0)
      title = `${quoted(e.pair.names[0])} was scanned twice`
      what = (
        <>
          <Icon name="dup" /> The same {plural(sets.length, 'page')} in two documents{extra ? `, and ${plural(extra, 'page')} only one has` : ''}
        </>
      )
      where = e.pair.names.map(quoted).join(' · ')
      thumbs = sets.slice(0, 1).flatMap((s) => s.copies.map((c) => c.image))
    } else {
      const s = e.set!
      title = s.kind === 'same_page' ? 'The same page, scanned twice' : 'Very similar text'
      what =
        s.kind === 'same_page' ? (
          <>
            <Icon name="dup" /> {s.reasons[0] ?? `${s.score}% alike`}
          </>
        ) : (
          <>
            <Icon name="doc" /> Close wording: perhaps another draft
          </>
        )
      where = s.copies.map(copyWhere).join(' · ')
      thumbs = s.copies.map((c) => c.image)
      if (s.kind === 'same_page')
        sug = (
          <small>
            <Mark /> Lindley suggests keeping copy {s.copies.findIndex((c) => c.page_id === s.suggested) + 1}
          </small>
        )
    }
    return (
      <button key={e.key} className="dcard" onClick={() => nav(`/duplicates/${e.key}`)} data-tip="Compare the copies side by side and choose what to keep">
        <span className="dup-thumbs">
          {thumbs.slice(0, 3).map((u, i) => (
            <span className="pg-img" key={i}>
              <img src={imageAt(u, 120)} alt="" loading="lazy" />
            </span>
          ))}
        </span>
        <span>
          <b>{title}</b>
          <small>{what}</small>
          <small>{where}</small>
          {sug}
        </span>
      </button>
    )
  }
  const same = es.filter((e) => e.kind === 'same_page')
  const similar = es.filter((e) => e.kind === 'similar')
  return (
    <>
      <Head
        crumbs="Duplicates"
        title={es.length ? `${plural(es.length, 'possible duplicate')} to look at` : 'No duplicates'}
        sub="Lindley compares what pages say, so a page scanned twice is found even when the scanner settings were different. Look at the copies side by side and choose what to keep. Copies you don’t keep are set aside, never deleted."
      />
      <div className="scroll">
        {es.length ? (
          <div className="dup-list">
            {same.length > 0 && (
              <section>
                <h2>
                  <Icon name="dup" />
                  Scanned more than once
                </h2>
                <p className="sub">The same pages, scanned again. Keep the better copy.</p>
                <div className="docs">{same.map(card)}</div>
              </section>
            )}
            {similar.length > 0 && (
              <section>
                <h2>
                  <Icon name="doc" />
                  Very similar text
                </h2>
                <p className="sub">The wording is close but not the same: perhaps another draft, or a copy with changes. You may want to keep both.</p>
                <div className="docs">{similar.map(card)}</div>
              </section>
            )}
          </div>
        ) : (
          <Done
            title="No duplicates"
            action={
              <button className="btn primary" onClick={() => nav('/inbox')} data-tip="See the scans waiting in the Inbox">
                Go to the Inbox
              </button>
            }
          >
            Lindley hasn’t found any page scanned more than once.
          </Done>
        )}
      </div>
    </>
  )
}

export function DupView() {
  const key = useParams().key ?? ''
  const dups = useApi('duplicates', api.duplicates)
  const { docs, folderPath } = useApp()
  const { run } = useFeedback()
  const nav = useNavigate()
  const [cur, setCur] = useState(0)
  const [mode, setMode] = useState<'scan' | 'text'>('scan')
  const [zoom, setZoom] = useState(1)
  // Sets decided here: forgotten at once, so the refresh after a decision doesn't ask for them
  const [closed, setClosed] = useState<number[]>([])
  const decided =
    (ids: number[]) =>
    <T,>(r: T) => {
      setClosed((c) => [...c, ...ids])
      return r
    }
  const es = dups.data ? entries(dups.data) : []
  const k = es.findIndex((e) => e.key === key)
  const e = es[k]
  const setIds = (e?.pair ? e.pair.sets : e?.set ? [e.set.id] : []).filter((id) => !closed.includes(id))
  const i = Math.min(cur, Math.max(0, setIds.length - 1))
  const full = useApi(setIds[i] != null ? `dupset:${setIds[i]}` : null, () => api.duplicateSet(setIds[i]))
  const cmp = useRef<HTMLDivElement>(null)
  useLooking('Duplicates')

  // Zoomed in, the scans of all copies scroll together, so the same spot stays side by side.
  useEffect(() => {
    const el = cmp.current
    if (!el) return
    let syncing = false
    const on = (ev: Event) => {
      const t = ev.target as HTMLElement
      if (!t.classList?.contains('dup-img') || syncing) return
      syncing = true
      el.querySelectorAll<HTMLElement>('.dup-img').forEach((o) => {
        if (o !== t) {
          o.scrollTop = t.scrollTop
          o.scrollLeft = t.scrollLeft
        }
      })
      requestAnimationFrame(() => (syncing = false))
    }
    el.addEventListener('scroll', on, true)
    return () => el.removeEventListener('scroll', on, true)
  })

  if (dups.error && !dups.data) return <ErrorBox error={dups.error} />
  if (!dups.data) return <Loading />
  if (!e)
    return (
      <>
        <Head crumbs={<Crumb to="/duplicates">Duplicates</Crumb>} title="Decided" />
        <Done
          title="Nothing left to decide here"
          action={
            <button className="btn primary" onClick={() => nav('/duplicates')} data-tip="See the other possible duplicates">
              Back to Duplicates
            </button>
          }
        >
          These copies have been decided.
        </Done>
      </>
    )

  const next = es[k + 1]?.key ?? es[k - 1]?.key
  const after = () => nav(next ? `/duplicates/${next}` : '/duplicates', { replace: true })
  const s = full.data
  // In a document pair, each column is one document.
  const copies = s ? (e.pair ? e.pair.documents.map((d) => s.copies.find((c) => c.document_id === d)).filter(Boolean) as DupCopy[] : s.copies) : []
  const similar = e.kind === 'similar'
  const keepBoth = e.pair ? 'Keep both documents' : similar ? 'Keep both' : 'Keep all, they’re different'

  let title: string
  let sub: string
  if (e.pair) {
    const extra = Object.entries(e.pair.extra).filter(([, v]) => v.length)
    title = `${quoted(e.pair.names[0])} was scanned twice`
    sub = `Both documents have the same ${plural(e.pair.sets.length, 'page')}. Compare them page by page, then keep one document, or the better scan of each page.${
      extra.length ? ` ${extra.map(([d, v]) => `${quoted(docs.get(Number(d))?.name ?? '')} also has ${plural(v.length, 'page')} the other doesn’t; ${v.length === 1 ? 'it stays' : 'they stay'} where ${v.length === 1 ? 'it is' : 'they are'}, whichever you keep.`).join(' ')}` : ''
    }`
  } else if (!similar) {
    title = 'The same page, scanned twice'
    sub = `${s?.reasons.join('. ') ?? ''}. The scans may look different because the scanner settings were. Keep the better copy; the other is set aside.`
  } else {
    title = 'Very similar text'
    sub = 'The wording is close but not the same, so these may be two drafts, or a copy with changes. If you want both, keep both.'
  }

  const keepCopy = (c: DupCopy) => run(api.keepCopy(s!.id, c.page_id).then(decided([s!.id])), (r) => `Kept ${c.file_name}. Set aside ${plural(r.set_aside.length, 'copy', 'copies')}. Nothing was deleted.`).then((r) => r && setIds.length <= 1 && after())
  const keepDoc = (keep: number) => {
    const other = e.pair!.documents.find((d) => d !== keep)!
    run(api.keepDocument(keep, other).then(decided(e.pair!.sets)), (r) => `Kept ${quoted(docs.get(keep)?.name ?? '')}. Set aside the other document’s ${plural(r.set_aside.length, 'page')}. Nothing was deleted.`).then((r) => r && after())
  }
  const notDup = async () => {
    const ids = e.pair ? e.pair.sets : [e.set!.id]
    for (const id of ids.slice(0, -1)) await api.notDuplicates(id).catch(() => undefined)
    run(api.notDuplicates(ids[ids.length - 1]).then(decided(ids)), 'Kept both. Lindley won’t suggest these as duplicates again.').then((r) => r && after())
  }

  return (
    <>
      <Head
        crumbs={
          <>
            <Crumb to="/duplicates">Duplicates</Crumb> › {title}
          </>
        }
        title={title}
        sub={sub}
      >
        {s && !similar && (
          <Banner kind="ai">
            <b>Lindley suggests keeping copy {copies.findIndex((c) => c.page_id === s.suggested) + 1}.</b> {s.why.join('. ')}.
          </Banner>
        )}
        {s && similar && (
          <Banner kind="ai">
            <b>These may be two drafts of the same page.</b> If so, keep both. If one is only a copy with mistakes, keep the other.
          </Banner>
        )}
        <div className="dup-bar">
          <div className="tabs" role="tablist" aria-label="Compare">
            {(
              [
                ['scan', 'Scans', 'The scans side by side'],
                ['text', 'Text, differences marked', 'What Lindley read from each, with the words that differ underlined'],
              ] as const
            ).map(([m, l, tip]) => (
              <button key={m} className="tab" role="tab" aria-selected={mode === m} tabIndex={mode === m ? 0 : -1} data-tip={tip} onClick={() => setMode(m)}>
                {l}
              </button>
            ))}
          </div>
          {e.pair && e.pair.sets.length > 1 && (
            <div className="strip rv-strip" role="group" aria-label="Pages in both documents">
              {e.pair.sets.map((sid, n) => {
                const one = dups.data!.sets.find((x) => x.id === sid)
                return (
                  <button key={sid} aria-current={n === i} aria-label={`Page ${n + 1}`} data-tip={`Compare page ${n + 1} of both documents`} onClick={() => setCur(n)}>
                    <span className="pg-img">{one && <img src={imageAt(one.copies[0].image, 100)} alt="" />}</span>
                    <span>{n + 1}</span>
                  </button>
                )
              })}
            </div>
          )}
        </div>
        {mode === 'text' && (
          <p className="sub">
            Words one copy has and the other doesn’t are <mark>underlined</mark>. Case and punctuation don’t count.
          </p>
        )}
      </Head>
      {!s ? (
        full.error ? (
          <ErrorBox error={full.error} />
        ) : (
          <Loading what="Comparing the copies…" />
        )
      ) : (
        <div className="dup-cmp" ref={cmp} style={{ '--n': copies.length } as CSSProperties}>
          {copies.map((c, n) => {
            const mine = c.page_id === s.suggested && !similar
            const diffs = (c.segments ?? []).filter(([, d]) => d).reduce((k2, [t]) => k2 + t.trim().split(/\s+/).length, 0)
            const doc = c.document_id != null ? docs.get(c.document_id) : undefined
            const others = copies.filter((o) => o !== c)
            const home = c.where !== 'document' ? others.find((o) => o.where === 'document') : undefined
            const note = e.pair
              ? 'Keeping this document sets the other one’s matching pages aside; its pages with no copy stay where they are.'
              : home
                ? `Keeping this one puts it in its place in ${quoted(home.document_name ?? '')}, and sets the other copy aside.`
                : `Keeping this one sets the other ${others.length === 1 ? 'copy' : 'copies'} aside.`
            return (
              <section className="dup-col" key={c.page_id} aria-labelledby={`dc${n}`}>
                <div className="dup-col-h">
                  <h2 id={`dc${n}`}>
                    Copy {n + 1}
                    {e.pair && doc ? `: ${docHome(doc, folderPath)}` : ''}
                  </h2>
                  {mine && (
                    <span className="chip ai" data-tip={s.why.join('. ')}>
                      <Mark />
                      Lindley suggests this one
                    </span>
                  )}
                  {e.pair ? (
                    <>
                      <button className={`btn${doc && c.page_id === s.suggested ? ' primary' : ''}`} onClick={() => keepDoc(c.document_id!)} data-tip="Keep this whole document; the other one’s matching pages are set aside">
                        <Icon name="check" />
                        Keep this document
                      </button>
                      <button className="btn" onClick={() => keepCopy(c)} data-tip="Keep only this page’s scan, and decide the other pages one by one">
                        Keep just this page
                      </button>
                    </>
                  ) : (
                    <button className={`btn${mine ? ' primary' : ''}`} onClick={() => keepCopy(c)} data-tip="Keep this copy; the others are set aside, never deleted">
                      <Icon name="check" />
                      Keep this one
                    </button>
                  )}
                </div>
                <p className="dup-note">{note}</p>
                {mode === 'scan' ? (
                  <div className={`dup-img${zoom > 1 ? ' zoomed' : ''}`}>
                    <img src={imageAt(c.image, 2000)} alt={`Scan ${c.file_name}, copy ${n + 1}`} style={{ '--z': zoom } as CSSProperties} />
                  </div>
                ) : (
                  <div className="dup-tx" role="region" tabIndex={0} aria-label={`Text of copy ${n + 1}`}>
                    {(c.segments ?? [[c.text ?? '', false]]).map(([t, d], j) =>
                      d ? (
                        <mark key={j}>
                          <span className="sr-only">[differs] </span>
                          {t}
                        </mark>
                      ) : (
                        <span key={j}>{t}</span>
                      ),
                    )}
                  </div>
                )}
                <dl className="dup-facts">
                  <dt>File</dt>
                  <dd className="mono">{c.file_name}</dd>
                  <dt>Imported</dt>
                  <dd>{when(c.imported_at)}</dd>
                  <dt>Scan</dt>
                  <dd data-tip={c.width ? `${c.width} × ${c.height} pixels` : undefined}>
                    {c.dpi ? `${c.dpi} dpi` : 'dpi unknown'}, {c.color_mode === 'rgb' ? 'colour' : c.color_mode === 'gray' ? 'grey' : (c.color_mode ?? '')}
                  </dd>
                  <dt>Read</dt>
                  <dd>
                    {c.confidence != null ? `${Math.round(c.confidence)}%` : 'not read'}
                    {c.corrected ? ', checked' : ''}
                  </dd>
                  <dt>Text</dt>
                  <dd className="wide">{diffs ? `${plural(diffs, 'word')} the other copy doesn’t have` : 'The same words as the other copy'}</dd>
                  <dt>Where</dt>
                  <dd className="wide">{copyWhere(c)}</dd>
                </dl>
              </section>
            )
          })}
        </div>
      )}
      <Dock label="Compare copies">
        <DBtn icon="prev" label="Previous" disabled={k === 0} tip="The possible duplicate before" onClick={() => nav(`/duplicates/${es[k - 1].key}`)} />
        <Sep />
        <DBtn icon="zout" label="Zoom out" iconOnly disabled={mode === 'text' || zoom <= 1} tip="Zoom out of both scans" onClick={() => setZoom(Math.max(1, zoom - 0.5))} />
        <DBtn icon="zin" label="Zoom in" iconOnly disabled={mode === 'text' || zoom >= 4} tip="Zoom in on both scans. They scroll together." onClick={() => setZoom(Math.min(4, zoom + 0.5))} />
        <Sep />
        <DBtn icon="checkc" label={keepBoth} kind={similar ? 'primary' : ''} tip="They aren’t copies of each other: keep them all. Lindley won’t suggest them again." onClick={notDup} />
        <Sep />
        <DockProgress text={`${k + 1} of ${plural(es.length, 'duplicate')}`} at={k + 1} of={es.length} />
        <DBtn icon="chev" label="Next" kind="next" disabled={k >= es.length - 1} tip="The next possible duplicate" onClick={() => nav(`/duplicates/${es[k + 1].key}`)} />
      </Dock>
    </>
  )
}
