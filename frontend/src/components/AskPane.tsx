// Ask Lindley: a side pane (or a floating window) for questions about the archive.
// With an AI chosen for answering questions (GET /api/chat/status says it's ready), a question
// goes to it with the pages Lindley finds for it, and the answer comes as it's written, citing
// those pages as [n]. Conversations are kept in the library. Without one, Lindley answers what it
// can from its own data, without any AI: every page that mentions a word, and what's waiting for
// review. It says so plainly. Those two are answered that way with an AI too: it's complete,
// free and instant.

import { Fragment, type ReactNode, useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router'
import { api, ask as askAi, type ChatConn, type ChatSaved, type ChatSource } from '../api/client'
import { invalidate, useApi, useLocal } from '../api/store'
import { anyAi, cloudInUse } from '../lib/ai'
import { useApp } from '../lib/appContext'
import { plural, quoted, shortName, when } from '../lib/words'
import { Icon, Mark } from '../ui/icons'

type Mode = 'dock' | 'float' | 'collapsed'
// An answer from the AI: finding pages, being written, or over
type State = 'reading' | 'writing' | 'done' | 'stopped' | 'failed'
interface Msg {
  key: number
  from: 'me' | 'ai'
  text: string
  list?: string[]
  acts?: { label: string; tip?: string; go: () => void }[]
  sources?: ChatSource[] // the pages sent with the question, which the answer cites as [n]
  state?: State
}

let made = 0
const msg = (m: Omit<Msg, 'key'>): Msg => ({ ...m, key: ++made })

const fromSaved = (m: ChatSaved): Msg =>
  m.role === 'user' ? msg({ from: 'me', text: m.text }) : msg({ from: 'ai', text: m.text, sources: m.sources, state: m.status })

const HELLO_FIND =
  'Hello, I’m Lindley. I can find every page that mentions a name or a word, and tell you what’s waiting for your review. When I’m not sure I read a page correctly, I flag it for you instead of guessing quietly.'
const HELLO_ASK =
  'Hello, I’m Lindley. Ask me about your documents: I’ll find the pages that answer it and tell you which they are, so you can check. When a page may have been misread, I say so instead of guessing quietly.'

/** Where an AI runs, and so who sees the questions, in words. */
const whereWords = (o: ChatConn) =>
  o.where === 'cloud'
    ? `Your questions, and the pages that answer them, are sent to ${o.company ?? o.label}.`
    : 'It runs on computers you control, so your pages stay private.'

export function AskPane({ open, close }: { open: boolean; close: () => void }) {
  const { looking, scope, overview, settings, connectors, ai } = useApp()
  const nav = useNavigate()
  const status = useApi('chat-status', api.chatStatus).data
  const ready = status?.state === 'ready'
  const [mode, setMode] = useLocal<Mode>('ai', 'dock')
  const [chatId, setChatId] = useLocal<number | null>('ask-chat', null)
  const [pos, setPos] = useState({ x: 0, y: 70 })
  const [msgs, setMsgs] = useState<Msg[]>([])
  const [q, setQ] = useState('')
  const [busy, setBusy] = useState(false)
  const [writing, setWriting] = useState(false)
  const [listing, setListing] = useState(false)
  const stopper = useRef<AbortController | null>(null)
  const shown = useRef<number | null>(null) // the conversation in the log now
  const pane = useRef<HTMLElement>(null)
  const log = useRef<HTMLDivElement>(null)

  useEffect(() => {
    log.current?.scrollTo({ top: log.current.scrollHeight })
  }, [msgs, busy, listing])

  // A conversation kept from before, or chosen from the list, is shown as it was
  useEffect(() => {
    if (chatId === shown.current) return
    shown.current = chatId
    if (chatId === null) return // a new conversation: fresh() emptied the log
    let live = true
    api.conversation(chatId).then(
      (c) => live && setMsgs(c.messages.map(fromSaved)),
      () => {
        if (!live) return
        shown.current = null
        setChatId(null)
      },
    )
    return () => {
      live = false
    }
  }, [chatId, setChatId])

  const cloud = cloudInUse(settings, connectors)
  const chat = ai('chat')
  const privacy = cloud.length
    ? { cls: 'cloud', icon: 'cloud' as const, text: `Cloud: ${cloud.join(', ')}`, tip: `Your pages and questions are sent to ${cloud.join(' and ')}. They are not private. Click to change.` }
    : anyAi(settings, connectors)
      ? { cls: 'priv', icon: 'lock' as const, text: 'Private', tip: 'Lindley’s AI runs on computers you control. Click for AI settings.' }
      : { cls: 'off', icon: 'warn' as const, text: 'No AI', tip: 'No AI is connected. Click to connect one.' }

  const say = (m: Msg) => setMsgs((x) => [...x, m])
  const update = (key: number, f: (m: Msg) => Msg) => setMsgs((x) => x.map((m) => (m.key === key ? f(m) : m)))

  const openPage = (s: ChatSource) =>
    nav(s.document_id ? `/documents/${s.document_id}?view=side&page=${(s.page_number ?? 1) - 1}` : `/scans/${s.page_id}`)

  const choose = async (o: ChatConn) => {
    try {
      const s = await api.settings()
      s.ai.jobs.chat = { connection: o.name, model: s.ai.jobs.chat?.model ?? null }
      await api.saveSettings(s)
      invalidate()
      say(msg({ from: 'ai', text: `Done: ${o.label} answers your questions now. ${whereWords(o)} Ask away.` }))
    } catch (e) {
      say(msg({ from: 'ai', text: (e as Error).message }))
    }
  }

  const fresh = () => {
    if (writing) return
    shown.current = null
    setChatId(null)
    setMsgs([])
    setListing(false)
  }

  /** Why the AI can't answer, and what to do about it. */
  const cantAnswer = (): Msg => {
    if (status?.state === 'offer')
      return msg({
        from: 'ai',
        text: `No AI is chosen for answering questions yet, so I can’t answer that. ${plural(status.offers.length, 'AI')} you’ve set up can. I can still find pages that mention a word: try “Find” and a name.`,
        acts: status.offers.slice(0, 3).map((o) => ({ label: `Use ${o.label} for Ask Lindley`, tip: whereWords(o), go: () => choose(o) })),
      })
    if (status?.state === 'broken')
      return msg({
        from: 'ai',
        text: `I can’t answer that: ${status.reason}. I can still find pages that mention a word: try “Find” and a name.`,
        acts: [{ label: 'Open AI settings', go: () => nav('/settings/ai') }],
      })
    return msg({
      from: 'ai',
      text: 'No AI is connected for answering questions, so I can’t answer that. I can still find pages that mention a word: try “Find” and a name. You can connect an AI on this computer, which keeps your scans private, or a cloud AI, in Settings.',
      acts: [{ label: 'Open AI settings', go: () => nav('/settings/ai') }],
    })
  }

  const answerWithAi = async (question: string) => {
    const ctl = new AbortController()
    stopper.current = ctl
    setWriting(true)
    const a = msg({ from: 'ai', text: '', sources: [], state: 'reading' })
    say(a)
    try {
      await askAi(
        { question, chat_id: chatId, scope, looking },
        (e) => {
          if (e.event === 'chat') {
            shown.current = e.data.chat_id // already in the log: not loaded again
            if (e.data.chat_id !== chatId) setChatId(e.data.chat_id)
          } else if (e.event === 'sources') update(a.key, (m) => ({ ...m, sources: e.data.sources, state: 'writing' }))
          else if (e.event === 'text') update(a.key, (m) => ({ ...m, text: m.text + e.data.text, state: 'writing' }))
          else if (e.event === 'done') update(a.key, (m) => ({ ...m, state: e.data.status }))
          else if (e.event === 'error') update(a.key, (m) => ({ ...m, text: e.data.message, state: 'failed' }))
        },
        ctl.signal,
      )
      if (ctl.signal.aborted) update(a.key, (m) => (m.state === 'reading' || m.state === 'writing' ? { ...m, state: 'stopped' } : m))
    } catch (e) {
      update(a.key, (m) => ({ ...m, text: (e as Error).message, state: 'failed' }))
    } finally {
      stopper.current = null
      setWriting(false)
    }
  }

  const ask = async (question: string) => {
    const text = question.trim()
    if (!text || busy || writing) return
    setListing(false)
    say(msg({ from: 'me', text }))
    setQ('')
    // With an AI, only plain requests for every page with a word, or for the review queue, are
    // answered here: "Find out where Edith lived" is a question for the AI
    const find = ready
      ? (text.match(/^(?:find|search for|show me)\b.*?\bmention(?:ing|s)?\s+(.+?)[?.!]*$/i) ?? text.match(/^search for\s+(.+?)[?.!]*$/i))
      : text.match(/(?:mention(?:ing|s)?|find|search for)\s+(.+?)[?.!]*$/i)
    const review = ready ? /\b(which|what)\b.*\b(need|needs|waiting)\b.*\breview\b/i.test(text) : /review|unsure|not sure|check/i.test(text)
    if (!find && !review) {
      if (ready) await answerWithAi(text)
      else say(cantAnswer())
      return
    }
    setBusy(true)
    try {
      if (find) {
        const term = find[1].replace(/^(every|all)\s+(pages?|letters?|documents?)\s+(mentioning|about|with)\s+/i, '')
        const r = await api.search(term)
        if (!r.results.length) {
          say(msg({ from: 'ai', text: `I couldn’t find “${term}” on any page. Handwriting is sometimes read loosely, so try a surname on its own, or another spelling.` }))
          return
        }
        const top = r.results.slice(0, 6)
        say(
          msg({
            from: 'ai',
            text: `I searched the text of every page (no AI needed for that) and found “${term}” on ${plural(r.results.length, 'page')}${r.results.length === 50 ? ' or more' : ''}. The first few:`,
            list: top.map((x) => (x.document_name ? `${x.document_name}, page ${x.page_number}` : `${x.where === 'aside' ? 'Set aside' : 'Inbox'}: ${x.file}`)),
            acts: [
              ...top.slice(0, 3).map((x) => ({
                label: `Open ${x.document_name ? quoted(shortName(x.document_name, 23)) : x.file}`,
                tip: 'Open this page beside its text',
                go: () => nav(x.document_id ? `/documents/${x.document_id}?view=side&page=${(x.page_number ?? 1) - 1}` : `/scans/${x.page_id}`),
              })),
              { label: 'See all results', tip: `Search for “${term}”`, go: () => nav(`/search?q=${encodeURIComponent(term)}`) },
            ],
          }),
        )
        return
      }
      const r = await api.review()
      if (!r.count) {
        say(msg({ from: 'ai', text: 'Nothing needs your review right now. Every page I’ve read is either confident or checked by you.' }))
        return
      }
      say(
        msg({
          from: 'ai',
          text: `I’m not confident I read ${plural(r.count, 'page')} correctly:`,
          list: r.groups.map((g) => `${g.document ? g.document.name : 'Inbox'}: ${plural(g.pages.length, 'page')}`),
          acts: [{ label: 'Start reviewing', tip: 'Go through them one by one', go: () => nav('/review/all') }],
        }),
      )
    } catch (e) {
      say(msg({ from: 'ai', text: (e as Error).message }))
    } finally {
      setBusy(false)
    }
  }

  const input = useRef<HTMLTextAreaElement>(null)
  // Suggestions: one to ask at once, one to finish typing.
  const sugg: { label: string; ask?: string; fill?: string; tip: string }[] = [
    ...(ready && scope.page_id ? [{ label: 'What does this page say?', ask: 'What does this page say?', tip: 'Ask about the page you have open' }] : []),
    ...(ready && scope.document_id && !scope.page_id ? [{ label: 'What is this document about?', ask: 'What is this document about?', tip: 'Ask about the document you have open' }] : []),
    ...(overview?.counts.review ? [{ label: 'Which pages need my review?', ask: 'Which pages need my review?', tip: 'Ask this' }] : []),
    { label: 'Find every page mentioning…', fill: 'Find every page mentioning ', tip: 'Start the question, then type a name or word' },
  ]

  const onHeadDown = (e: React.PointerEvent) => {
    if (mode !== 'float' || (e.target as Element).closest('button') || window.matchMedia('(max-width: 860px)').matches) return
    const app = pane.current?.parentElement?.getBoundingClientRect()
    const r = pane.current?.getBoundingClientRect()
    if (!app || !r) return
    const ox = e.clientX - r.left
    const oy = e.clientY - r.top
    const mv = (ev: PointerEvent) =>
      setPos({ x: Math.max(0, Math.min(ev.clientX - app.left - ox, app.width - r.width)), y: Math.max(0, Math.min(ev.clientY - app.top - oy, app.height - 60)) })
    const up = () => {
      window.removeEventListener('pointermove', mv)
      window.removeEventListener('pointerup', up)
    }
    window.addEventListener('pointermove', mv)
    window.addEventListener('pointerup', up)
  }

  if (mode === 'collapsed' && !open)
    return (
      <button className="ai-rail" onClick={() => setMode('dock')} data-tip="Show Ask Lindley, to ask about your documents">
        <Mark />
        <span>Ask Lindley</span>
      </button>
    )

  return (
    <aside
      ref={pane}
      className={`ai-pane${mode === 'float' ? ' is-float' : ''}`}
      aria-label="Ask Lindley"
      style={mode === 'float' ? { left: pos.x, top: pos.y } : undefined}
    >
      <div className="ai-h" onPointerDown={onHeadDown} data-tip={mode === 'float' ? 'Drag here to move the window' : undefined}>
        <Mark />
        <strong>Ask Lindley</strong>
        <button className="icon-btn" onClick={fresh} disabled={writing} aria-label="New conversation" data-tip="Start a new conversation. This one is kept.">
          <Icon name="plus" />
        </button>
        <button
          className={`icon-btn${listing ? ' on' : ''}`}
          onClick={() => setListing(!listing)}
          aria-pressed={listing}
          aria-label="Conversations"
          data-tip={listing ? 'Back to the conversation' : 'Your conversations with Lindley, kept on this computer'}
        >
          <Icon name="history" />
        </button>
        <button
          className="icon-btn popout-btn"
          onClick={() => {
            const app = pane.current?.parentElement?.getBoundingClientRect()
            if (mode !== 'float' && app) setPos({ x: Math.max(12, app.width - 400), y: 70 })
            setMode(mode === 'float' ? 'dock' : 'float')
          }}
          aria-label={mode === 'float' ? 'Dock to the side' : 'Pop out into a floating window'}
          data-tip={mode === 'float' ? 'Dock to the side' : 'Pop out into a floating window you can move and resize'}
        >
          <Icon name={mode === 'float' ? 'dock' : 'popout'} />
        </button>
        <button
          className="icon-btn"
          onClick={() => {
            if (window.matchMedia('(max-width: 860px)').matches) close()
            else setMode('collapsed')
          }}
          aria-label="Hide Ask Lindley"
          data-tip="Hide Ask Lindley. The bar at the right brings it back."
        >
          <Icon name="close" />
        </button>
      </div>
      <div className="ai-ctx">
        <span data-tip="What you have open. Questions are about this unless you say otherwise.">
          Looking at: <b>{looking}</b>
        </span>
        <button className={`ai-priv ${privacy.cls}`} onClick={() => nav('/settings/ai')} data-tip={privacy.tip}>
          <Icon name={privacy.icon} />
          <span>{privacy.text}</span>
        </button>
      </div>
      {status?.state === 'offer' && !listing && (
        <div className="ai-offer">
          <p>
            {status.offers.length === 1
              ? `${status.offers[0].label} can answer your questions about your documents. ${whereWords(status.offers[0])}`
              : 'An AI you’ve set up can answer your questions about your documents. Choose one:'}
          </p>
          <div className="msg-acts">
            {status.offers.slice(0, 3).map((o) => (
              <button key={o.name} onClick={() => choose(o)} data-tip={`Use ${o.label} for Ask Lindley. ${whereWords(o)} You can change this in Settings.`}>
                Use {o.label}
              </button>
            ))}
          </div>
        </div>
      )}
      {listing ? (
        <Conversations
          current={chatId}
          choose={(id) => {
            setChatId(id)
            setListing(false)
          }}
          gone={(id) => {
            if (id === chatId) fresh()
          }}
        />
      ) : (
        <div className="ai-log" ref={log} aria-live="polite">
          <div className="msg ai">
            <p>{ready ? HELLO_ASK : HELLO_FIND}</p>
          </div>
          {msgs.map((m) => (
            <div key={m.key} className={`msg ${m.from}${m.state === 'failed' ? ' failed' : ''}`}>
              {m.state ? <Answer m={m} open={openPage} by={status?.connection?.label} /> : <p>{m.text}</p>}
              {m.list && (
                <ul>
                  {m.list.map((l, k) => (
                    <li key={k}>{l}</li>
                  ))}
                </ul>
              )}
              {m.acts && (
                <div className="msg-acts">
                  {m.acts.map((a, k) => (
                    <button key={k} onClick={a.go} data-tip={a.tip}>
                      {a.label}
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))}
          {busy && (
            <div className="msg ai" aria-label="Lindley is looking">
              <Dots />
            </div>
          )}
        </div>
      )}
      <div className="ai-sugg">
        {sugg.map((s) => (
          <button
            key={s.label}
            data-tip={s.tip}
            onClick={() => {
              if (s.ask) ask(s.ask)
              else {
                setQ(s.fill ?? '')
                input.current?.focus()
              }
            }}
          >
            {s.label}
          </button>
        ))}
      </div>
      <form
        className="ai-form"
        onSubmit={(e) => {
          e.preventDefault()
          ask(q)
        }}
      >
        <textarea
          ref={input}
          rows={2}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              ask(q)
            }
          }}
          placeholder={!ready ? 'Find a name or word on every page' : chat?.cloud ? `Ask about your documents (sent to ${chat.company})` : 'Ask about your documents'}
          aria-label="Ask Lindley a question"
        />
        {writing ? (
          <button type="button" className="btn" aria-label="Stop" data-tip="Stop the answer here. What’s written so far is kept." onClick={() => stopper.current?.abort()}>
            <Icon name="stop" />
          </button>
        ) : (
          <button className="btn primary" aria-label="Send" data-tip="Send your question (Enter). Shift+Enter starts a new line." disabled={busy}>
            <Icon name="send" />
          </button>
        )}
      </form>
    </aside>
  )
}

const Dots = () => (
  <span className="typing">
    <i />
    <i />
    <i />
  </span>
)

/** An answer from the AI: its text with each [n] a button that opens that page, and the pages
 * it came from underneath. */
function Answer({ m, open, by }: { m: Msg; open: (s: ChatSource) => void; by?: string }) {
  const sources = m.sources ?? []
  const byN = new Map(sources.map((s) => [s.n, s]))
  const tip = (s: ChatSource) => `${s.label}${s.unsure ? ': read with low confidence, and not checked yet' : ''}. Click to open it.`
  const inline = (text: string): ReactNode[] =>
    text.split(/(\[\d+\]|\*\*[^*\n]+\*\*)/).map((part, i) => {
      const c = part.match(/^\[(\d+)\]$/)
      const s = c ? byN.get(Number(c[1])) : undefined
      if (s)
        return (
          <button key={i} type="button" className={`cite${s.unsure ? ' unsure' : ''}`} onClick={() => open(s)} data-tip={tip(s)}>
            {s.n}
          </button>
        )
      if (/^\*\*[^*\n]+\*\*$/.test(part)) return <strong key={i}>{part.slice(2, -2)}</strong>
      return <Fragment key={i}>{part}</Fragment>
    })
  const cited = sources.filter((s) => m.text.includes(`[${s.n}]`))
  const over = m.state === 'done' || m.state === 'stopped'
  return (
    <>
      {m.text.trim() &&
        m.text
          .trim()
          .split(/\n{2,}/)
          .map((p, i) => (
            <p key={i} className="ans">
              {m.state === 'failed' ? p : inline(p)}
            </p>
          ))}
      {m.state === 'reading' && (
        <p className="msg-note">
          <Dots /> Finding the pages that answer this…
        </p>
      )}
      {m.state === 'writing' && !m.text && (
        <p className="msg-note">
          <Dots /> {sources.length ? `Reading ${plural(sources.length, 'page')}` : 'Thinking'}
          {by ? ` with ${by}` : ''}…
        </p>
      )}
      {m.state === 'stopped' && <p className="msg-note">Stopped here.</p>}
      {over && sources.length > 0 && (
        <details className="msg-src">
          <summary data-tip="The pages Lindley found and sent with your question">
            {cited.length ? `From ${plural(cited.length, 'page')}` : `${plural(sources.length, 'page')} read`}
          </summary>
          <ul>
            {(cited.length ? cited : sources).map((s) => (
              <li key={s.n}>
                <button type="button" onClick={() => open(s)} data-tip="Open this page beside its text">
                  {s.n}. {s.label}
                </button>
                {s.unsure && <span data-tip="Read with low confidence, and not checked yet"> (may be misread)</span>}
              </li>
            ))}
          </ul>
        </details>
      )}
    </>
  )
}

/** The conversations kept, the latest first: open one, or delete it (asked twice). */
function Conversations({ current, choose, gone }: { current: number | null; choose: (id: number) => void; gone: (id: number) => void }) {
  const list = useApi('chat-conversations', api.conversations)
  const [deleting, setDeleting] = useState<number | null>(null)
  const rows = list.data?.conversations ?? []
  const remove = async (id: number) => {
    await api.deleteConversation(id)
    setDeleting(null)
    gone(id)
    list.reload()
  }
  return (
    <div className="ai-log ai-convs">
      {list.error && <p className="msg-note">{list.error.message}</p>}
      {list.data && !rows.length && <p className="msg-note">No conversations yet. Questions you ask the AI, and its answers, are kept here.</p>}
      <ul>
        {rows.map((c) => (
          <li key={c.id} className={c.id === current ? 'on' : undefined}>
            <button className="conv" onClick={() => choose(c.id)} data-tip="Open this conversation">
              <span>{c.title}</span>
              <small>
                {when(c.updated_at)} · {plural(c.questions, 'question')}
              </small>
            </button>
            {deleting === c.id ? (
              <button className="btn danger" onClick={() => remove(c.id)} data-tip="Delete it for good: it can’t be undone">
                Delete
              </button>
            ) : (
              <button className="icon-btn" onClick={() => setDeleting(c.id)} aria-label="Delete this conversation" data-tip="Delete this conversation">
                <Icon name="close" />
              </button>
            )}
          </li>
        ))}
      </ul>
    </div>
  )
}
