// One Inbox or Set aside scan, opened to read and correct before it joins a document. Previous
// and Next (and ← →) go through the rest, in the order the list shows them.

import { useRef, useState } from 'react'
import { Navigate, useNavigate, useParams } from 'react-router'
import { api } from '../api/client'
import { useApi } from '../api/store'
import { textOf } from '../lib/view'
import { Banner, Crumb, ErrorBox, Head, Loading } from '../components/bits'
import { PageTools } from '../components/PageTools'
import { ScanImage } from '../components/ScanImage'
import { TextPanel } from '../components/TextPanel'
import { useActions } from '../lib/actionsContext'
import { useApp, useLooking } from '../lib/appContext'
import { who } from '../lib/ai'
import { useInboxOrder } from '../lib/inboxOrder'
import { usePageKeys } from '../lib/usePageKeys'
import { plural, quoted, sentences, shortName } from '../lib/words'
import { DBtn, Dock, DockText, Sep } from '../ui/Dock'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'

export function ScanView() {
  const id = Number(useParams().id)
  const page = useApi(`page:${id}`, () => api.page(id))
  const sugs = useApi('suggestions', api.suggestions)
  const { ai } = useApp()
  const acts = useActions()
  const { run } = useFeedback()
  const nav = useNavigate()
  const text = useRef<HTMLDivElement>(null)
  const p = page.data
  useLooking(p ? `${p.where === 'aside' ? 'Set aside' : 'Inbox'}, ${p.file}` : 'A scan', { page_id: p?.id, document_id: p?.document_id ?? undefined })

  // The scans around this one: the Inbox as it's sorted there, or Set aside. Where it was opened
  // from, and its place, are kept, so once it leaves (set aside, added to a document) the scan
  // that followed it is shown, without going back to the list.
  const { shown } = useInboxOrder()
  const asides = useApi('aside', api.aside)
  const [from, setFrom] = useState<'inbox' | 'aside' | null>(null)
  const [at, setAt] = useState(0)
  if (p && !from && (p.where === 'inbox' || p.where === 'aside')) setFrom(p.where)
  const list = (from ?? p?.where) === 'aside' ? (asides.data?.pages ?? []) : shown
  const i = list.findIndex((x) => x.id === id)
  if (i >= 0 && i !== at) setAt(i)
  const open = (x: { id: number } | undefined) => x && nav(`/scans/${x.id}`, { replace: true })
  usePageKeys(
    () => open(list[i - 1]),
    () => open(list[i + 1]),
    i >= 0,
  )

  if (page.error && !p) return <ErrorBox error={page.error} />
  if (!p) return <Loading />
  if (from && p.where !== from) {
    // It left from here: on to the scan that followed it, or back to the list if it was the last
    const rest = list.filter((x) => x.id !== id)
    const next = rest[Math.min(at, rest.length - 1)]
    return <Navigate to={next ? `/scans/${next.id}` : from === 'aside' ? '/aside' : '/inbox'} replace />
  }
  if (p.where === 'document' && p.document) {
    // It joined a document before it was opened here: show it there
    return <Navigate to={`/documents/${p.document.id}?view=side&page=${p.document.page_number - 1}`} replace />
  }
  const aside = p.where === 'aside'
  const home = aside ? '/aside' : '/inbox'
  const hints = (sugs.data?.suggestions ?? []).filter((s) => (s.kind === 'group_pages' ? (s.payload.pages ?? []).includes(id) : s.page_id === id))
  const add = hints.find((s) => s.kind === 'add_to_document')
  const group = hints.find((s) => s.kind === 'group_pages')
  const setAside = hints.find((s) => s.kind === 'set_aside')
  const reading = p.state === 'needs_ai' || p.state === 'ai_failed'
  const check = p.state === 'review' || p.state === 'ai_reading' || reading

  return (
    <>
      <Head
        crumbs={
          <>
            <Crumb to={home}>{aside ? 'Set aside' : 'Inbox'}</Crumb> › {p.file}
          </>
        }
        title={p.file}
        sub="This scan isn’t in a document yet. Correct its text here if you like, then start a new document with it or add it to one you already have."
      >
        {!aside && add && (
          <Banner
            kind="ai"
            actions={
              <button className="btn" onClick={() => run(api.acceptSuggestion(add.id), `Added to ${quoted(shortName(add.document_name ?? ''))}.`)} data-tip="Add it to the end of that document">
                Add it there
              </button>
            }
          >
            Lindley thinks this belongs in {quoted(add.document_name ?? '')}, {add.confidence}% sure. {sentences(add.reasons)}
          </Banner>
        )}
        {!aside && !add && group && (
          <Banner
            kind="ai"
            actions={
              <>
                <button className="btn" onClick={() => run(api.acceptSuggestion(group.id), `Grouped ${plural(group.payload.pages?.length ?? 0, 'scan')}.`)} data-tip="Make them one document under Lindley’s name for it">
                  Group them
                </button>
                <button className="btn ghost" onClick={() => run(api.dismissSuggestion(group.id), 'Okay. Lindley won’t suggest that again.')} data-tip="They don’t go together">
                  No
                </button>
              </>
            }
          >
            Lindley thinks this goes with {plural((group.payload.pages?.length ?? 1) - 1, 'other scan')}: <i>{group.payload.name}</i>, {group.confidence}% sure.
          </Banner>
        )}
        {!aside && !add && !group && setAside && (
          <Banner
            kind="ai"
            actions={
              <button className="btn" onClick={() => acts.setAside([id])} data-tip="Nothing is deleted">
                Set it aside
              </button>
            }
          >
            {setAside.reasons[0] ?? 'It isn’t part of a document'}, so Lindley thinks it could be set aside.
          </Banner>
        )}
        {reading && (
          <Banner
            kind="ai"
            actions={
              <button
                className="btn ai"
                onClick={() => acts.readWithAi([id])}
                data-tip={`Send it to ${who(ai('vision'))} to read now. Sending is your OK.`}
              >
                <Mark /> Ask the AI to read it
              </button>
            }
          >
            <b>This scan needs an AI to look at it.</b> Tesseract read it with only {p.confidence}% confidence, so its text is only a rough guess.
          </Banner>
        )}
        {p.state === 'review' && ai('vision') && (
          <Banner
            kind="ai"
            actions={
              <button
                className="btn ghost"
                onClick={() => acts.readWithAi([id])}
                data-tip={`Send it to ${who(ai('vision'))} to read again, instead of checking every word yourself. Sending is your OK.`}
              >
                <Mark /> Ask the AI to read it
              </button>
            }
          >
            Lindley read it with {p.confidence}% confidence, so a few words may be wrong. Check it yourself, or ask the AI to read it.
          </Banner>
        )}
        {p.mirror_found && p.mirrored && (
          <Banner
            kind="ai"
            actions={
              <button className="btn ghost" onClick={() => acts.flip([id])} data-tip="Show it as it was scanned, if Lindley was wrong">
                Flip it back
              </button>
            }
          >
            <b>This scan is a mirror image</b>, perhaps the back of a carbon copy, so Lindley turned it round to read it. The scan itself isn’t changed.
          </Banner>
        )}
        {aside && p.duplicate_of && (
          <Banner kind="ok" icon="dup">
            This is a copy you set aside. You kept <span className="mono">{p.duplicate_of.file}</span> instead.
          </Banner>
        )}
      </Head>
      <div className="side" style={{ flex: 1, minHeight: 0 }}>
        <ScanImage className="side-img" src={p.image} alt={`Scan ${p.file}`} />
        <div className="side-tx">
          <TextPanel
            page={p}
            textRef={text}
            label={p.file}
            actions={
              check ? (
                <>
                  <button className="btn primary" onClick={() => run(api.checkText(id), 'Marked the page as checked.')} data-tip="The text matches the scan: trust it for search and chat">
                    <Icon name="check" /> The text is correct
                  </button>
                  <button className="btn" onClick={() => run(api.checkText(id, textOf(text.current)), 'Saved your correction.')} data-tip="Keep the text as you’ve corrected it. Lindley’s reading is kept too.">
                    Save my correction
                  </button>
                </>
              ) : (
                <button className="btn" onClick={() => run(api.checkText(id, textOf(text.current)), 'Saved the transcription.')} data-tip="Keep the text as it is in the box">
                  Save transcription
                </button>
              )
            }
          />
        </div>
      </div>
      <Dock label="Scan actions">
        <DBtn icon="back" label={aside ? 'Back to Set aside' : 'Back to Inbox'} tip={`Back to ${aside ? 'Set aside' : 'the Inbox'}`} onClick={() => nav(home)} />
        <DBtn icon="prev" label="Previous" iconOnly disabled={i <= 0} tip={`The scan before, as ${aside ? 'Set aside' : 'the Inbox'} is sorted (←)`} onClick={() => open(list[i - 1])} />
        <Sep />
        {/* No More…: everything in it is on this toolbar */}
        <PageTools ids={[id]} where={aside ? 'aside' : 'inbox'} noun="scan" more={false} />
        <DBtn icon="newdoc" label="New document…" tip="Start a new document with this scan" onClick={() => acts.newDocument([id])} />
        <DBtn icon="move" label="Add to document…" tip="Add this scan to a document in progress" onClick={(e) => acts.moveMenu([id], e.currentTarget)} />
        {aside ? (
          <DBtn icon="inbox" label="Return to Inbox" tip="Put it back in the Inbox, for Lindley to sort" onClick={() => acts.toInbox([id])} />
        ) : (
          <DBtn icon="aside" label="Set aside" tip="For scans that aren’t part of a document. Nothing is deleted." onClick={() => acts.setAside([id])} />
        )}
        {i >= 0 && (
          <>
            <Sep />
            <DockText>
              {i + 1} of {list.length}
            </DockText>
          </>
        )}
        <DBtn icon="chev" label="Next" kind="next" disabled={i < 0 || i >= list.length - 1} tip={`The next scan, as ${aside ? 'Set aside' : 'the Inbox'} is sorted (→)`} onClick={() => open(list[i + 1])} />
      </Dock>
    </>
  )
}
