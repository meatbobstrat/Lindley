// The Inbox: new scans, with Lindley's hints under each (where it thinks a scan belongs, which
// scans go together, what could be set aside), and Set aside, where scans that aren't part of a
// document wait. Nothing is ever deleted.

import { useMemo, useRef } from 'react'
import { useLocation, useNavigate } from 'react-router'
import { api, type Page, type Suggestion } from '../api/client'
import { useApi, useLocal } from '../api/store'
import { Banner, ErrorBox, Head, Loading } from '../components/bits'
import { PageGrid } from '../components/PageGrid'
import { SizeControl } from '../components/SizeControl'
import { useActions } from '../lib/actionsContext'
import { useApp, useLooking } from '../lib/appContext'
import { pagesToSort, who } from '../lib/ai'
import { useSelection, useThumb } from '../lib/view'
import { plural, quoted, sentences, shortName, them } from '../lib/words'
import { DBtn, Dock, DockText, Sep } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'

const RANK = { add_to_document: 0, group_pages: 1, set_aside: 2 } as const

export function InboxView() {
  const inbox = useApi('inbox', api.inbox)
  const sugs = useApi('suggestions', api.suggestions)
  const waiting = useApi('needs-ai', api.needsAi)
  const { ai } = useApp()
  const acts = useActions()
  const { run, openMenu } = useFeedback()
  const nav = useNavigate()
  const loc = useLocation()
  const [sel, setSel] = useSelection()
  const [sort, setSort] = useLocal<'new' | 'old' | 'match'>('sort', (loc.state as { sort?: 'match' } | null)?.sort ?? 'new')
  const [thumb] = useThumb()
  const file = useRef<HTMLInputElement>(null)

  const pages = useMemo(() => inbox.data?.pages ?? [], [inbox.data])
  useLooking(`Inbox, ${plural(pages.length, 'scan')}${sel.size ? `, ${sel.size} selected` : ''}`)
  const inInbox = useMemo(() => new Set(pages.map((p) => p.id)), [pages])

  // Each page's hint: the best of its open suggestions, while its pages are all still here
  const hintOf = useMemo(() => {
    const m = new Map<number, Suggestion>()
    ;(sugs.data?.suggestions ?? []).forEach((s) => {
      if (!(s.payload.pages ?? [s.page_id]).every((id) => inInbox.has(id))) return
      const ids = s.kind === 'group_pages' ? (s.payload.pages ?? [s.page_id]) : [s.page_id]
      ids.forEach((id) => {
        const was = m.get(id)
        if (!was || RANK[s.kind] < RANK[was.kind]) m.set(id, s)
      })
    })
    return m
  }, [sugs.data, inInbox])

  const sorting = ai('assemble')
  const sortOf = useMemo(() => {
    const m = new Map<number, number>()
    if (sorting) waiting.data?.sort.items.forEach((x) => x.pages.forEach((p) => m.set(p.id, x.id)))
    return m
  }, [waiting.data, sorting])

  const shown = useMemo(() => {
    const a = [...pages]
    if (sort === 'old') a.reverse()
    if (sort === 'match') a.sort((x, y) => Number(hintOf.has(y.id)) - Number(hintOf.has(x.id)))
    return a
  }, [pages, sort, hintOf])

  if (inbox.error && !inbox.data) return <ErrorBox error={inbox.error} />
  if (!inbox.data) return <Loading />

  const ids = shown.filter((p) => sel.has(p.id)).map((p) => p.id)
  const n = ids.length
  const all = sugs.data?.suggestions ?? []
  const adds = pages.filter((p) => hintOf.get(p.id)?.kind === 'add_to_document').length
  const here = all.filter((s) => s.kind === 'group_pages' && (s.payload.pages ?? []).every((id) => inInbox.has(id)))
  // Groups the AI checked come first, offered for one-click accept; then the rules' own
  const offers = here.filter((s) => s.offer)
  const groups = [...offers, ...here.filter((s) => !s.offer)]
  const asides = pages.filter((p) => hintOf.get(p.id)?.kind === 'set_aside')
  const toRead = (waiting.data?.read.pages ?? []).filter((r) => r.document_id == null).map((r) => r.page_id)
  const toSort = sorting ? (waiting.data?.sort.items ?? []) : []
  const nAi = toRead.length + pagesToSort(toSort)

  const showOnly = (want: number[]) => {
    setSort('match')
    setSel(new Set(want))
  }

  return (
    <>
      <Head
        crumbs="Inbox"
        title={`${plural(pages.length, 'scan')} waiting`}
        sub="New scans land here. Lindley reads each one and groups pages into documents, but you don’t have to wait: select scans and start a document yourself, or double-click one to read and correct it."
      >
        <div className="viewbar">
          <label data-tip="The order the scans are shown in">
            Sort{' '}
            <select value={sort} onChange={(e) => setSort(e.target.value as typeof sort)}>
              <option value="new">Newest scans first</option>
              <option value="old">Oldest scans first</option>
              <option value="match">Suggested matches first</option>
            </select>
          </label>
          <SizeControl />
        </div>
      </Head>
      <div className="scroll">
        <div className="inbox-banners">
        {nAi > 0 && (
          <AiBanner
            read={toRead}
            sortPages={pagesToSort(toSort)}
            onAsk={() => {
              if (toRead.length) acts.readWithAi(toRead)
              if (toSort.length) acts.sortWithAi()
            }}
          />
        )}
        {adds > 0 && (
          <Banner
            kind="ai"
            actions={
              <button className="btn" onClick={() => setSort('match')} data-tip="Sort the Inbox so the scans with a suggestion come first">
                Show them first
              </button>
            }
          >
            <b>Lindley found {plural(adds, 'scan')} that probably belong to documents in progress.</b> Look for the suggestion under each page. The button beside it
            has the other likely places.
          </Banner>
        )}
        {offers.length > 1 && (
          <Banner
            kind="ai"
            actions={
              <button
                className="btn"
                onClick={() => run(api.acceptOffers(), (r) => `Made ${plural(r.documents.length, 'document')} from ${plural(r.pages.length, 'scan')}.`)}
                data-tip="Make each group the AI checked a document, as it suggests. One Undo takes them all back."
              >
                Accept all {offers.length}
              </button>
            }
          >
            <b>The AI checked {plural(offers.length, 'group')} of scans that go together.</b> It’s less sure than Lindley needs to sort them on its own, but its reasons are below.
          </Banner>
        )}
        {groups.slice(0, 2).map((g) => (
          <Banner
            key={g.id}
            kind="ai"
            actions={
              <>
                <button
                  className="btn"
                  data-tip="Make these scans one document, under Lindley’s name for it. You can rename it after."
                  onClick={() => run(api.acceptSuggestion(g.id), `Grouped ${plural(g.payload.pages?.length ?? 0, 'scan')} into ${quoted(shortName(g.payload.name ?? 'a document'))}.`)}
                >
                  {g.offer ? 'Accept' : 'Group them'}
                </button>
                <button className="btn ghost" onClick={() => showOnly(g.payload.pages ?? [])} data-tip="Select these scans and show them first">
                  Show them
                </button>
                <button
                  className="btn ghost"
                  onClick={() => run(api.dismissSuggestion(g.id), 'Okay. Lindley won’t suggest that again.')}
                  data-tip="They don’t go together. Lindley won’t suggest it again."
                >
                  No
                </button>
              </>
            }
          >
            <b>Do these {plural(g.payload.pages?.length ?? 0, 'scan')} go together?</b> {g.offer ? 'The AI checked them: it' : 'Lindley'} thinks they’re one document, <i>{g.payload.name}</i>, {g.confidence}% sure.{' '}
            {sentences(g.reasons)}
          </Banner>
        ))}
        {groups.length > 2 && (
          <Banner kind="ai">
            <b>Lindley thinks {plural(groups.length - 2, 'more set')} of scans go together.</b> Look for “Goes with…” under the scans.
          </Banner>
        )}
        {asides.length > 0 && (
          <Banner
            kind="ai"
            actions={
              <>
                <button
                  className="btn"
                  onClick={() => acts.setAside(asides.map((p) => p.id))}
                  data-tip="Move them to Set aside. Nothing is deleted, and you can bring them back."
                >
                  Set {them(asides.length)} aside
                </button>
                <button className="btn ghost" onClick={() => showOnly(asides.map((p) => p.id))} data-tip="Select them and show them first">
                  Show {them(asides.length)}
                </button>
              </>
            }
          >
            <b>
              Lindley thinks {plural(asides.length, 'scan')} {asides.length === 1 ? 'isn’t' : 'aren’t'} part of any document.
            </b>{' '}
            {sentences([...new Set(asides.flatMap((p) => hintOf.get(p.id)?.reasons ?? []))])}
          </Banner>
        )}
        </div>
        <PageGrid
          pages={shown}
          sel={sel}
          setSel={setSel}
          thumb={thumb}
          onOpen={(p) => nav(`/scans/${p.id}`)}
          onReview={(p) => nav('/review/inbox', { state: { page: p.id } })}
          onMenu={(sel2, at, anchor) => acts.pageMenu(sel2, 'inbox', anchor, { at })}
          below={(p) => <Hint p={p} s={hintOf.get(p.id)} sortItem={sortOf.get(p.id)} openMenu={openMenu} />}
          empty="The Inbox is empty. Every scan has a home."
        />
      </div>
      <input
        ref={file}
        type="file"
        multiple
        accept="image/*,.pdf,.tif,.tiff"
        hidden
        onChange={(e) => {
          acts.addScans([...(e.target.files ?? [])])
          e.target.value = ''
        }}
      />
      <Dock label="Inbox actions">
        <DBtn
          icon="upload"
          label="Add scans…"
          kind="keep"
          tip="Add scan files from this computer or a thumb drive. You can also drop files anywhere on Lindley."
          onClick={() => file.current?.click()}
        />
        <Sep />
        <DockText>{n ? `${n} selected` : 'None selected'}</DockText>
        <DBtn icon="rotL" label="Turn left" iconOnly disabled={!n} tip={n ? 'Turn the selected scans a quarter turn left' : 'Select scans to turn them'} onClick={() => acts.rotate(ids, -90)} />
        <DBtn icon="rotR" label="Turn right" iconOnly disabled={!n} tip={n ? 'Turn the selected scans a quarter turn right' : 'Select scans to turn them'} onClick={() => acts.rotate(ids, 90)} />
        <DBtn
          icon="newdoc"
          label="Group into document…"
          disabled={!n}
          tip={n ? 'Start a new document with the selected scans, in the order shown' : 'Select scans to group them into a document'}
          onClick={() => acts.newDocument(ids)}
        />
        <DBtn
          icon="move"
          label="Add to document…"
          disabled={!n}
          tip={n ? 'Add the selected scans to a document in progress' : 'Select scans to add them to a document'}
          onClick={(e) => acts.moveMenu(ids, e.currentTarget)}
        />
        <DBtn
          icon="aside"
          label="Set aside"
          disabled={!n}
          tip={n ? 'For scans that aren’t part of a document. Nothing is deleted.' : 'Select scans to set them aside'}
          onClick={() => acts.setAside(ids)}
        />
      </Dock>
    </>
  )
}

/** An offer to send scans to the AI, saying where they'd go and why they wait. */
function AiBanner({ read, sortPages, onAsk }: { read: number[]; sortPages: number; onAsk: () => void }) {
  const { ai } = useApp()
  const nav = useNavigate()
  const n = read.length + sortPages
  const parts = [
    read.length ? `${plural(read.length, 'page')} ${read.length === 1 ? 'was' : 'were'} too hard to read` : '',
    sortPages ? `${plural(sortPages, 'page')} couldn’t be sorted into documents` : '',
  ].filter(Boolean)
  const whom = [...new Set([read.length ? who(ai('vision')) : '', sortPages ? who(ai('assemble')) : ''].filter(Boolean))].join(' and ')
  const auto = (read.length ? ai('vision') : ai('assemble'))?.cfg.allow === 'auto'
  return (
    <Banner
      kind="ai"
      actions={
        <>
          <button className="btn ai" onClick={onAsk} data-tip={`Send ${them(n)} to ${whom} now. Sending is your OK.`}>
            <Mark /> Ask the AI
          </button>
          <button className="btn ghost" onClick={() => nav('/needs-ai')} data-tip="See everything waiting for an AI, and where it would go">
            See what’s waiting
          </button>
        </>
      }
    >
      <b>
        {plural(n, 'scan')} {n === 1 ? 'needs' : 'need'} an AI to look at {them(n)}.
      </b>{' '}
      {parts.join(', and ')}.{' '}
      {auto
        ? `${whom} may run on its own, but these are waiting for you: today’s limit is used up, or its calls failed.`
        : `Lindley asks you first, so nothing is sent to ${whom || 'an AI'} until you say so.`}
    </Banner>
  )
}

/** Lindley's hint under an Inbox scan, and a menu of the other places it thinks of, and "no". */
function Hint({
  p,
  s,
  sortItem,
  openMenu,
}: {
  p: Page
  s: Suggestion | undefined
  sortItem: number | undefined
  openMenu: ReturnType<typeof useFeedback>['openMenu']
}) {
  const { run } = useFeedback()
  const acts = useActions()
  const { docs } = useApp()
  const nav = useNavigate()
  if (!s) {
    if (sortItem === undefined) return null
    return (
      <button className="pg-hint" onClick={() => nav(`/needs-ai/${sortItem}`)} data-tip="This scan needs an AI to look at it: Lindley couldn’t tell which document it belongs in. Click to see it with the scans around it.">
        <Mark />
        <span>Needs AI to sort</span>
      </button>
    )
  }
  const sure = `${s.confidence}% sure${s.reasons.length ? `: ${sentences(s.reasons)}` : ''}`
  const dismiss = () => run(api.dismissSuggestion(s.id), 'Okay. Lindley won’t suggest that again.')
  let main
  let items: Parameters<typeof openMenu>[0]
  if (s.kind === 'add_to_document') {
    const name = s.document_name ?? 'a document'
    const n = s.payload.pages?.length ?? 1
    main = (
      <button
        className="pg-hint"
        onClick={() => run(api.acceptSuggestion(s.id), `Added ${plural(n, 'page')} to ${quoted(shortName(name))}.`)}
        data-tip={`Lindley thinks this belongs in ${quoted(name)}, ${sure}. Click to add it${n > 1 ? `, with the ${n - 1} scans that go with it` : ''}.`}
      >
        <Mark />
        <span>Add to {quoted(shortName(name))}</span>
      </button>
    )
    const cands = (s.payload.candidates ?? []).filter((c) => c.document != null && c.document !== s.document_id && docs.get(c.document!)?.status === 'progress')
    items = [
      { head: 'Where Lindley thinks it goes' },
      { label: `Add to ${quoted(shortName(name))}, ${s.confidence}% sure`, icon: 'doc', tip: sentences(s.reasons), onSelect: () => run(api.acceptSuggestion(s.id), `Added to ${quoted(shortName(name))}.`) },
      ...cands.map((c) => ({
        label: `Add to ${quoted(shortName(c.name))}, ${c.confidence}% sure`,
        icon: 'doc' as const,
        tip: sentences(c.reasons),
        onSelect: () => acts.moveTo([p.id], { id: c.document!, name: c.name }),
      })),
      '-',
      { label: 'Not in any of these', icon: 'close', tip: 'Lindley won’t suggest this again', onSelect: dismiss },
    ]
  } else if (s.kind === 'group_pages') {
    const ids = s.payload.pages ?? [s.page_id]
    items = [
      { head: `Lindley thinks these ${ids.length} scans go together` },
      { label: 'Group them', icon: 'newdoc', tip: 'Make them one document under Lindley’s name for it', onSelect: () => run(api.acceptSuggestion(s.id), `Grouped ${plural(ids.length, 'scan')} into ${quoted(shortName(s.payload.name ?? 'a document'))}.`) },
      { label: 'Group them, with another name…', icon: 'pen', onSelect: () => acts.newDocument(ids, s.payload.name ?? '', true) },
      '-',
      { label: 'They don’t go together', icon: 'close', tip: 'Lindley won’t suggest it again', onSelect: dismiss },
    ]
    const menu = items
    main = (
      <button
        className="pg-hint"
        onClick={(e) => openMenu(menu, e.currentTarget)}
        data-tip={`Lindley thinks these ${ids.length} scans are one document, ${quoted(s.payload.name ?? '')}, ${sure}. Click for choices.`}
      >
        <Mark />
        <span>Goes with {plural(ids.length - 1, 'other scan')}</span>
      </button>
    )
  } else {
    const why = s.reasons[0] ?? 'Not part of a document'
    main = (
      <button className="pg-hint" onClick={() => acts.setAside([p.id])} data-tip={`${why}, so Lindley thinks it could be set aside, ${s.confidence}% sure. Click to set it aside; nothing is deleted.`}>
        <Mark />
        <span>Set aside? {why}</span>
      </button>
    )
    items = [
      { head: `${why}, ${s.confidence}% sure` },
      { label: 'Set it aside', icon: 'aside', onSelect: () => acts.setAside([p.id]) },
      '-',
      { label: 'Keep it in the Inbox', icon: 'close', tip: 'Lindley won’t suggest this again', onSelect: dismiss },
    ]
  }
  return (
    <div className="pg-hints">
      {main}
      <button
        className="pg-hint more"
        aria-label="More choices for this suggestion"
        data-tip="Other places Lindley thinks of, or say no"
        onClick={(e) => openMenu(items, e.currentTarget)}
      >
        <Icon name="more" />
      </button>
    </div>
  )
}

export function AsideView() {
  const aside = useApi('aside', api.aside)
  const acts = useActions()
  const nav = useNavigate()
  const [sel, setSel] = useSelection()
  const [thumb] = useThumb()
  useLooking('Set aside')
  if (aside.error && !aside.data) return <ErrorBox error={aside.error} />
  if (!aside.data) return <Loading />
  const pages = aside.data.pages
  const ids = pages.filter((p) => sel.has(p.id)).map((p) => p.id)
  const n = ids.length
  return (
    <>
      <Head
        crumbs="Set aside"
        title="Set aside"
        sub="Receipts, blank pages, sticky notes and other scans that aren’t part of a document, and copies of pages you scanned twice. Nothing here is deleted. Return a scan to the Inbox if it was set aside by mistake."
      >
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
          onMenu={(s, at, anchor) => acts.pageMenu(s, 'aside', anchor, { at })}
          empty="Nothing has been set aside."
        />
      </div>
      <Dock label="Set aside actions">
        <DockText>{n ? `${n} selected` : 'None selected'}</DockText>
        <DBtn icon="rotL" label="Turn left" iconOnly disabled={!n} tip={n ? 'Turn the selected scans a quarter turn left' : 'Select scans to turn them'} onClick={() => acts.rotate(ids, -90)} />
        <DBtn icon="rotR" label="Turn right" iconOnly disabled={!n} tip={n ? 'Turn the selected scans a quarter turn right' : 'Select scans to turn them'} onClick={() => acts.rotate(ids, 90)} />
        <DBtn icon="inbox" label="Return to Inbox" disabled={!n} tip={n ? 'Put the selected scans back in the Inbox, for Lindley to sort' : 'Select scans to return them'} onClick={() => acts.toInbox(ids)} />
        <DBtn
          icon="move"
          label="Add to document…"
          disabled={!n}
          tip={n ? 'Add the selected scans to a document in progress' : 'Select scans to add them to a document'}
          onClick={(e) => acts.moveMenu(ids, e.currentTarget)}
        />
      </Dock>
    </>
  )
}
