// Needs AI: scans Lindley couldn't read or sort well enough on its own. An AI that may run on
// its own gets them as they arrive; one that asks first waits here until you send them.

import { useMemo } from 'react'
import { useNavigate, useParams } from 'react-router'
import { api, imageAt, type ReadItem, type SortItem } from '../api/client'
import { useApi } from '../api/store'
import { Banner, Crumb, Done, ErrorBox, Head, Loading, PrivTag } from '../components/bits'
import { PageGrid } from '../components/PageGrid'
import { SizeControl } from '../components/SizeControl'
import { useActions } from '../lib/actionsContext'
import { useSelection, useThumb } from '../lib/view'
import { type Conn } from '../lib/ai'
import { docHome, useApp, useLooking } from '../lib/appContext'
import { needs, plural, quoted, sentences, them, when } from '../lib/words'
import { DBtn, Dock, DockProgress, Sep } from '../ui/Dock'
import { Icon, Mark } from '../ui/icons'

function ConnLine({ c, what }: { c: Conn | null; what: string }) {
  const nav = useNavigate()
  if (!c)
    return (
      <p className="na-to">
        <Icon name="warn" /> No AI is set up for {what}.{' '}
        <button className="btn ghost" onClick={() => nav('/settings/ai')} data-tip="Choose an AI connection for this job">
          Choose one
        </button>
      </p>
    )
  return (
    <p className="na-to">
      Goes to <b>{c.label}</b> <PrivTag c={c} /> ·{' '}
      <span data-tip={c.cfg.allow === 'auto' ? 'Lindley sends these by itself, within the connection’s limits' : 'Nothing is sent until you press a button here'}>
        {c.cfg.allow === 'auto' ? 'runs on its own, so these are sent as they arrive' : 'asks you first'}
      </span>
    </p>
  )
}

function Thumbs({ ids }: { ids: number[] }) {
  return (
    <span className="dup-thumbs">
      {ids.slice(0, 3).map((id) => (
        <span className="pg-img" key={id}>
          <img src={imageAt(`/api/pages/${id}/image`, 120)} alt="" loading="lazy" />
        </span>
      ))}
    </span>
  )
}

const sureOf = (x: SortItem) => {
  const place = x.proposal.some((g) => g.question === 'place')
  const confs = place ? x.proposal.flatMap((g) => (g.candidates ?? []).map((c) => c.confidence)) : x.proposal.map((g) => g.confidence)
  return { place, sure: confs.length ? Math.max(...confs) : 0 }
}

export function NeedsAiList() {
  const waiting = useApi('needs-ai', api.needsAi)
  const { ai, docs, folderPath } = useApp()
  const acts = useActions()
  const nav = useNavigate()
  useLooking('Needs AI')
  if (waiting.error && !waiting.data) return <ErrorBox error={waiting.error} />
  if (!waiting.data) return <Loading />
  const { read, sort } = waiting.data
  const vision = ai('vision')
  const sorter = ai('assemble')
  const items = sorter ? sort.items : []
  const groups = new Map<number | null, ReadItem[]>()
  read.pages.forEach((r) => groups.set(r.document_id, [...(groups.get(r.document_id) ?? []), r]))
  const n = read.pages.length + items.reduce((k, x) => k + x.pages.length, 0)
  const toRead = read.pages.filter((r) => !r.sending)
  const toSort = items.filter((x) => !x.sending)
  const unsent = toRead.length + toSort.reduce((k, x) => k + x.pages.length, 0)
  const auto = vision?.cfg.allow === 'auto' || sorter?.cfg.allow === 'auto'

  const readThem = acts.readWithAi
  const sortIt = acts.sortWithAi

  return (
    <>
      <Head
        crumbs="Needs AI"
        title={n ? `${plural(n, 'scan')} ${needs(n)} an AI to look at ${them(n)}` : 'Nothing needs an AI'}
        sub="Lindley reads and sorts what it can on its own. These are the scans it couldn’t: pages too hard for Tesseract to read, and pages its rules couldn’t sort into documents. An AI that may run on its own gets them as they arrive. One that asks first waits for you: send a document at a time, or everything at once."
      />
      <div className="scroll">
        {n ? (
          <div className="na-list">
            {read.pages.length > 0 && (
              <section>
                <h2>
                  <Icon name="eye" />
                  Too hard to read
                </h2>
                <p className="sub">Tesseract read these with low confidence, usually handwriting or a faint copy. Its rough reading is used until the AI reads them properly.</p>
                <ConnLine c={vision} what="reading hard pages" />
                <div className="docs">
                  {[...groups].map(([docId, rows]) => {
                    const d = docId != null ? docs.get(docId) : undefined
                    const confs = rows.map((r) => r.confidence ?? 0)
                    const lo = Math.round(Math.min(...confs))
                    const hi = Math.round(Math.max(...confs))
                    const failed = rows.filter((r) => r.failed && !r.sending)
                    const sending = rows.filter((r) => r.sending).length
                    return (
                      <div className="na-card" key={docId ?? 'inbox'}>
                        <button className="dcard" onClick={() => nav(docId != null ? `/documents/${docId}` : '/inbox')} data-tip={d ? `Open ${quoted(d.name)}` : 'Open the Inbox'}>
                          <Thumbs ids={rows.map((r) => r.page_id)} />
                          <span>
                            <b style={d?.suggested ? { fontStyle: 'italic' } : undefined}>{rows[0].document_name ?? 'Inbox'}</b>
                            <small>
                              <Icon name="eye" /> {plural(rows.length, 'page')} Tesseract read with {lo === hi ? lo : `${lo}–${hi}`}% confidence
                            </small>
                            {sending > 0 && (
                              <small data-tip="Lindley sends them in the background, one at a time. You can keep working meanwhile.">
                                <i className="dot busy" aria-hidden="true" /> {sending === rows.length ? (sending === 1 ? 'The AI is reading it' : 'The AI is reading them') : `${plural(sending, 'page')} on ${sending === 1 ? 'its' : 'their'} way to the AI`}
                              </small>
                            )}
                            {failed.length > 0 && (
                              <small data-tip={failed[0].why ?? ''}>
                                <Icon name="warn" /> The AI didn’t manage {plural(failed.length, 'page')}. Lindley won’t try again on its own.
                              </small>
                            )}
                            <small>{d ? docHome(d, folderPath) : 'Scans not in a document yet'}</small>
                          </span>
                        </button>
                        <div className="na-acts">
                          <button
                            className="btn ai"
                            disabled={!vision || sending === rows.length}
                            onClick={() => readThem(rows.filter((r) => !r.sending).map((r) => r.page_id))}
                            data-tip={
                              !vision
                                ? 'Set up an AI for reading hard pages first'
                                : sending === rows.length
                                  ? 'Sent already. The status bar says when the AI is done.'
                                  : `Send ${them(rows.length - sending)} to ${vision.label} to read now. Sending is your OK.`
                            }
                          >
                            <Mark /> {sending === rows.length ? 'Sent to the AI' : failed.length === rows.length ? 'Try again' : `Ask the AI to read ${them(rows.length - sending)}`}
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </section>
            )}
            {items.length > 0 && (
              <section>
                <h2>
                  <Mark />
                  Couldn’t be sorted
                </h2>
                <p className="sub">
                  Lindley’s rules couldn’t tell where one document ends and the next begins, or which of a few likely documents these belong in. The AI reads them
                  together and says.
                </p>
                <ConnLine c={sorter} what="sorting pages" />
                <div className="docs">
                  {items.map((x) => {
                    const { place, sure } = sureOf(x)
                    const cands = x.proposal.flatMap((g) => g.candidates ?? [])
                    return (
                      <div className="na-card" key={x.id}>
                        <button className="dcard" onClick={() => nav(`/needs-ai/${x.id}`)} data-tip="Look at these pages, with Lindley’s guess">
                          <Thumbs ids={x.pages.map((p) => p.id)} />
                          <span>
                            <b>
                              {plural(x.pages.length, 'page')}: {place ? 'which document?' : `maybe ${plural(x.proposal.length, 'document')}`}
                            </b>
                            <small>
                              <Mark /> Lindley is {sure}% sure at best{place ? `, of ${plural(cands.length, 'likely document')}` : ''}
                            </small>
                            <small>In the Inbox since {when(x.since)}</small>
                            {x.sending && (
                              <small data-tip="Lindley sends them in the background. You can keep working meanwhile.">
                                <i className="dot busy" aria-hidden="true" /> The AI is sorting {them(x.pages.length)}
                              </small>
                            )}
                          </span>
                        </button>
                        <div className="na-acts">
                          <button className="btn ghost" onClick={() => nav(`/needs-ai/${x.id}`)} data-tip="See the pages and Lindley’s guess">
                            Look at {them(x.pages.length)}
                          </button>
                          <button
                            className="btn ai"
                            disabled={x.sending}
                            onClick={() => sortIt(x.id)}
                            data-tip={x.sending ? 'Sent already. The status bar says when the AI is done.' : `Send the text of these pages to ${sorter?.label} to sort now. Sending is your OK.`}
                          >
                            <Mark /> {x.sending ? 'Sent to the AI' : place ? 'Ask the AI which' : 'Ask the AI to sort them'}
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </section>
            )}
            {!sorter && sort.items.length > 0 && (
              <Banner
                kind="warn"
                actions={
                  <button className="btn" onClick={() => nav('/inbox')} data-tip="Sort them yourself in the Inbox">
                    Go to the Inbox
                  </button>
                }
              >
                <b>No AI is set up to sort pages.</b> {plural(sort.items.reduce((k, x) => k + x.pages.length, 0), 'page')} Lindley’s rules couldn’t sort wait in
                the Inbox for you to group by hand.
              </Banner>
            )}
          </div>
        ) : (
          <Done
            title="Nothing is waiting for an AI"
            action={
              <button className="btn primary" onClick={() => nav('/inbox')} data-tip="See the scans waiting in the Inbox">
                Go to the Inbox
              </button>
            }
          >
            {auto ? 'Your AI runs on its own, so Lindley sends it scans as they arrive.' : 'Every scan has been read and sorted, by Lindley, the AI or you.'}
          </Done>
        )}
      </div>
      <Dock label="Needs AI actions">
        {n > 0 && (
          <DBtn
            icon="lindley"
            label={unsent ? `Ask the AI about all ${unsent}` : 'All sent to the AI'}
            kind="ai"
            disabled={!unsent}
            tip={unsent ? 'Send everything waiting to the AI now: hard pages to read, and pages to sort. Sending is your OK.' : 'Everything here is on its way to the AI. The status bar says when it’s done.'}
            onClick={() => {
              if (toRead.length && vision) readThem(toRead.map((r) => r.page_id))
              if (toSort.length) sortIt()
            }}
          />
        )}
        <DBtn icon="gear" label="AI settings" tip="Choose which AI does each job, and when it may run on its own" onClick={() => nav('/settings/ai')} />
      </Dock>
    </>
  )
}

export function NeedsAiItem() {
  const id = Number(useParams().id)
  const waiting = useApi('needs-ai', api.needsAi)
  const inbox = useApi('inbox', api.inbox)
  const { ai, docs } = useApp()
  const acts = useActions()
  const nav = useNavigate()
  const [sel, setSel] = useSelection()
  const [thumb] = useThumb()
  useLooking('Pages to sort')
  const items = useMemo(() => waiting.data?.sort.items ?? [], [waiting.data])
  const k = items.findIndex((x) => x.id === id)
  const x = items[k]
  const pages = useMemo(() => {
    const want = new Set(x?.pages.map((p) => p.id))
    return (inbox.data?.pages ?? []).filter((p) => want.has(p.id))
  }, [x, inbox.data])

  if (waiting.error && !waiting.data) return <ErrorBox error={waiting.error} />
  if (!waiting.data || !inbox.data) return <Loading />
  if (!x)
    return (
      <>
        <Head crumbs={<Crumb to="/needs-ai">Needs AI</Crumb>} title="This has been sorted" />
        <Done
          title="Nothing left here"
          action={
            <button className="btn primary" onClick={() => nav('/needs-ai')} data-tip="See what else is waiting">
              Back to Needs AI
            </button>
          }
        >
          These pages have been sorted since, by the AI or by you.
        </Done>
      </>
    )

  const { place } = sureOf(x)
  const one = x.pages.length === 1
  const ids = x.pages.map((p) => p.id)
  const sorter = ai('assemble')
  const cands = x.proposal.flatMap((g) => g.candidates ?? [])
  const go = (j: number) => items[j] && nav(`/needs-ai/${items[j].id}`)

  return (
    <>
      <Head
        crumbs={
          <>
            <Crumb to="/needs-ai">Needs AI</Crumb> › {plural(x.pages.length, 'page')} to {place ? 'place' : 'sort'}
          </>
        }
        title={`${one ? 'This scan needs' : 'These scans need'} an AI to look at ${one ? 'it' : 'them'}`}
        sub={
          place
            ? `Lindley’s rules couldn’t tell which of these documents ${one ? 'this page belongs' : 'these pages belong'} in. Ask the AI to read ${one ? 'it' : 'them'} beside them, or choose one yourself. Either way you can change it after.`
            : 'Lindley’s rules couldn’t tell whether these pages are one document, or several. Ask the AI to read them together, or group them yourself. Either way you can change it after.'
        }
      >
        <Banner kind="ai">
          {place ? (
            <>
              <b>Where Lindley thinks {one ? 'it goes' : 'they go'}, not sure enough to act on:</b>
              <ul className="na-cands">
                {cands.map((c, i) => (
                  <li key={i}>
                    <span>
                      <b>{c.name}</b>, at the {c.at}, {c.confidence}% sure
                      <small>{sentences(c.reasons)}</small>
                    </span>
                    {c.document != null && docs.get(c.document)?.status === 'progress' && (
                      <button className="btn" onClick={() => acts.moveTo(ids, { id: c.document!, name: c.name })} data-tip={`Add ${them(ids.length)} to the end of ${quoted(c.name)}`}>
                        Add {them(ids.length)} here
                      </button>
                    )}
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <>
              <b>Lindley’s guess, not sure enough to act on:</b>
              <ul className="na-guess">
                {x.proposal.map((g, i) => (
                  <li key={i} data-tip={sentences(g.reasons)}>
                    {plural(g.pages.length, 'page')}: <i>{g.name}</i>, {g.confidence}% sure
                  </li>
                ))}
              </ul>
              <span>{sentences([...new Set(x.proposal.flatMap((g) => g.reasons))])}</span>
            </>
          )}
        </Banner>
        <ConnLine c={sorter} what="sorting pages" />
        <div className="viewbar">
          <SizeControl />
        </div>
      </Head>
      <div className="scroll">
        <PageGrid
          pages={pages}
          sel={sel}
          setSel={setSel}
          thumb={thumb}
          onOpen={(p) => nav(`/scans/${p.id}`)}
          onMenu={(s, at, anchor) => acts.pageMenu(s, 'inbox', anchor, { at })}
          empty="These pages have moved since."
        />
      </div>
      <Dock label="Pages to sort">
        <DBtn icon="prev" label="Previous" disabled={k === 0} tip="The question before" onClick={() => go(k - 1)} />
        <Sep />
        <DBtn
          icon="lindley"
          label={place ? 'Ask the AI which' : 'Ask the AI to sort these'}
          kind="ai"
          disabled={!sorter}
          tip={sorter ? `Send the text of these pages to ${sorter.label} now. Sending is your OK.` : 'Set up an AI for sorting pages first'}
          onClick={() => acts.sortWithAi(x.id)}
        />
        <DBtn icon="newdoc" label={place ? 'Start a new document…' : 'Group them myself…'} tip="Make a document of these pages yourself" onClick={() => acts.newDocument(ids)} />
        <Sep />
        <DockProgress text={`${k + 1} of ${items.length} to sort`} at={k + 1} of={items.length} />
        <DBtn icon="chev" label="Next" kind="next" disabled={k >= items.length - 1} tip="The next question" onClick={() => go(k + 1)} />
      </Dock>
    </>
  )
}
