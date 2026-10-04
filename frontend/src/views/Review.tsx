// Needs your review: pages Lindley read with less than the review threshold, so a person checks
// them before they're trusted for search and chat. Pick a document, or go through them all.

import { useEffect, useRef, useState } from 'react'
import { useLocation, useNavigate, useParams } from 'react-router'
import { api, imageAt, type Page } from '../api/client'
import { useApi } from '../api/store'
import { textOf, typing } from '../lib/view'
import { Crumb, Done, ErrorBox, Head, Loading } from '../components/bits'
import { TextPanel } from '../components/TextPanel'
import { docHome, useApp, useLooking } from '../lib/appContext'
import { needs, plural, shortName } from '../lib/words'
import { DBtn, Dock, DockProgress, Sep } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon } from '../ui/icons'
import { Strip } from './Document'


export function ReviewList() {
  const review = useApi('review', api.review)
  const { docs, folderPath } = useApp()
  const nav = useNavigate()
  useLooking('Pages to review')
  if (review.error && !review.data) return <ErrorBox error={review.error} />
  if (!review.data) return <Loading />
  const { count, groups, review_below } = review.data
  return (
    <>
      <Head
        crumbs="Needs your review"
        title={count ? `${plural(count, 'page')} ${needs(count)} your review` : 'All caught up'}
        sub={`Lindley flags any page it read with less than ${review_below}% confidence, so a person can check it before it’s trusted for search and chat. Pick a document to start with, or review everything in order.`}
      />
      <div className="scroll">
        {count ? (
          <div className="docs">
            {groups.map((g) => {
              const d = g.document ? docs.get(g.document.id) : undefined
              return (
                <button
                  key={g.document?.id ?? 'inbox'}
                  className="dcard"
                  onClick={() => nav(`/review/${g.document?.id ?? 'inbox'}`)}
                  data-tip={`Review the ${plural(g.pages.length, 'page')} ${g.document ? `in “${g.document.name}”` : 'in the Inbox'}, one by one`}
                >
                  <span className="pg-img">
                    <img src={imageAt(g.pages[0].image, 200)} alt="" loading="lazy" />
                  </span>
                  <span>
                    <b style={g.document?.suggested ? { fontStyle: 'italic' } : undefined}>{g.document ? g.document.name : 'Inbox'}</b>
                    <small>
                      <Icon name="flag" /> {plural(g.pages.length, 'page')} to review
                    </small>
                    <small>{g.document ? (d ? docHome(d, folderPath) : 'In progress') : 'Scans not in a document yet'}</small>
                  </span>
                </button>
              )
            })}
          </div>
        ) : (
          <Done
            title="Nothing left to review"
            action={
              <button className="btn primary" onClick={() => nav('/inbox')} data-tip="See the scans waiting in the Inbox">
                Go to the Inbox
              </button>
            }
          >
            Every page Lindley has read is either confident or checked by you.
          </Done>
        )}
      </div>
      {count > 0 && (
        <Dock label="Review actions">
          <DBtn icon="flag" label={`Start reviewing all ${count}`} kind="primary" tip="Go through every page to review, in order" onClick={() => nav('/review/all')} />
        </Dock>
      )}
    </>
  )
}

export function ReviewView() {
  const scope = useParams().scope ?? 'all'
  const review = useApi('review', api.review)
  const { docs } = useApp()
  const { run } = useFeedback()
  const nav = useNavigate()
  const loc = useLocation()
  const text = useRef<HTMLDivElement>(null)
  const want = (loc.state as { page?: number } | null)?.page
  const [at, setAt] = useState<number | null>(null)

  const groups = review.data?.groups ?? []
  const queue: Page[] =
    scope === 'all'
      ? groups.flatMap((g) => g.pages)
      : scope === 'inbox'
        ? (groups.find((g) => !g.document)?.pages ?? [])
        : (groups.find((g) => g.document?.id === Number(scope))?.pages ?? [])
  const start = want !== undefined ? Math.max(0, queue.findIndex((p) => p.id === want)) : 0
  const i = Math.max(0, Math.min(at ?? start, queue.length - 1))
  const p = queue[i]
  const full = useApi(p ? `page:${p.id}` : null, () => api.page(p!.id))
  const scopeName = scope === 'all' ? 'All pages' : scope === 'inbox' ? 'Inbox' : (docs.get(Number(scope))?.name ?? 'A document')
  useLooking(full.data?.document ? `${full.data.document.name}, page ${full.data.document.page_number}` : p ? `Inbox, ${p.file}` : 'Pages to review')

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (typing(e.target) || document.querySelector('dialog[open], .menu')) return
      if (e.key === 'ArrowRight' || e.key === 'PageDown') setAt(Math.min(queue.length - 1, i + 1))
      if (e.key === 'ArrowLeft' || e.key === 'PageUp') setAt(Math.max(0, i - 1))
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  })

  if (review.error && !review.data) return <ErrorBox error={review.error} />
  if (!review.data) return <Loading />

  const head = (
    <Head
      crumbs={
        <>
          <Crumb to="/review">Needs your review</Crumb> › {scopeName}
        </>
      }
      title={`${queue.length ? `${plural(queue.length, 'page')} to review` : 'All caught up'}${scope !== 'all' ? ` in ${shortName(scopeName)}` : ''}`}
      sub={`Lindley read ${queue.length === 1 ? 'this page' : 'these pages'} with less than ${review.data.review_below}% confidence. Compare the text with the scan, fix anything that’s wrong, then confirm it. The ← and → keys move between pages.`}
    >
      {queue.length > 1 ? (
        <div className="rv-strip">
          <Strip pages={queue} cur={i} setCur={setAt} label="Pages to review" flag={false} />
        </div>
      ) : (
        <div style={{ height: 6 }} />
      )}
    </Head>
  )

  if (!p)
    return (
      <>
        {head}
        <div className="scroll">
          <Done
            title="Nothing left to review here"
            action={
              <div className="actions">
                <button className="btn primary" onClick={() => nav('/review')} data-tip="See what else needs your review">
                  Back to Needs your review
                </button>
                {docs.get(Number(scope)) && (
                  <button className="btn" onClick={() => nav(`/documents/${scope}`)} data-tip="Open the document">
                    Open “{shortName(scopeName)}”
                  </button>
                )}
              </div>
            }
          >
            Every page is either confident or checked by you.
          </Done>
        </div>
      </>
    )

  const confirm = () => run(api.checkText(p.id), 'Marked the page as checked.')
  const save = () => run(api.checkText(p.id, textOf(text.current)), 'Saved your correction and marked the page as checked.')
  const doc = full.data?.document
  const where = (
    <div className="rv-where">
      {doc ? (
        <>
          In <b>{doc.name}</b>, page {doc.page_number}
        </>
      ) : (
        <>
          In the <b>Inbox</b>, <span className="mono">{p.file}</span>
        </>
      )}{' '}
      <button
        className="btn ghost"
        onClick={() => nav(doc ? `/documents/${doc.id}?view=side&page=${doc.page_number - 1}` : `/scans/${p.id}`)}
        data-tip={doc ? 'Open the page in its document' : 'Open the scan on its own'}
      >
        Open it there
      </button>
    </div>
  )
  return (
    <>
      {head}
      <div className="side" style={{ flex: 1, minHeight: 0 }}>
        <div className="side-img">
          <img src={imageAt(p.image, 1600)} alt="Scan of the page to review" />
        </div>
        <div className="side-tx">
          {full.data && full.data.id === p.id ? (
            <TextPanel page={full.data} textRef={text} label="the page to review" where={where} />
          ) : full.error ? (
            <ErrorBox error={full.error} />
          ) : (
            <Loading what="Reading the page…" />
          )}
        </div>
      </div>
      <Dock label="Move between pages to review">
        <DBtn icon="prev" label="Previous" disabled={i === 0} tip="The page before (←)" onClick={() => setAt(i - 1)} />
        <Sep />
        <DBtn icon="check" label="The text is correct" kind="primary" tip="The text matches the scan: trust it, and go on to the next page" onClick={confirm} />
        <DBtn icon="pen" label="Save my correction" tip="Keep the text as you’ve corrected it, and go on. Lindley’s reading is kept too." onClick={save} />
        <Sep />
        <DockProgress text={`Page ${i + 1} of ${queue.length} to review`} at={i + 1} of={queue.length} />
        <DBtn icon="chev" label="Next" kind="next" disabled={i >= queue.length - 1} tip="The next page, leaving this one for later (→)" onClick={() => setAt(i + 1)} />
      </Dock>
    </>
  )
}
