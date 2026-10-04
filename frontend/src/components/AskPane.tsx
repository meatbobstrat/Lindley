// Ask Lindley: a side pane (or a floating window) for questions about the archive.
// Answering open questions with the chat AI comes with the chat backend (POST /api/chat, still a
// stub). Until then Lindley answers what it can from its own data, without any AI: finding every
// page that mentions a word, and what's waiting for review. It says so plainly.

import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router'
import { api } from '../api/client'
import { useLocal } from '../api/store'
import { cloudInUse, anyAi } from '../lib/ai'
import { useApp } from '../lib/appContext'
import { plural } from '../lib/words'
import { Icon, Mark } from '../ui/icons'

type Mode = 'dock' | 'float' | 'collapsed'
interface Msg {
  from: 'me' | 'ai'
  text: string
  list?: string[]
  acts?: { label: string; tip?: string; go: () => void }[]
}

const HELLO: Msg = {
  from: 'ai',
  text: 'Hello, I’m Lindley. I can find every page that mentions a name or a word, and tell you what’s waiting for your review. When I’m not sure I read a page correctly, I flag it for you instead of guessing quietly.',
}

export function AskPane({ open, close }: { open: boolean; close: () => void }) {
  const { looking, overview, settings, connectors, ai } = useApp()
  const nav = useNavigate()
  const [mode, setMode] = useLocal<Mode>('ai', 'dock')
  const [pos, setPos] = useState({ x: 0, y: 70 })
  const [msgs, setMsgs] = useState<Msg[]>([HELLO])
  const [q, setQ] = useState('')
  const [busy, setBusy] = useState(false)
  const pane = useRef<HTMLElement>(null)
  const log = useRef<HTMLDivElement>(null)

  useEffect(() => {
    log.current?.scrollTo({ top: log.current.scrollHeight })
  }, [msgs, busy])

  const cloud = cloudInUse(settings, connectors)
  const chat = ai('chat')
  const privacy = cloud.length
    ? { cls: 'cloud', icon: 'cloud' as const, text: `Cloud: ${cloud.join(', ')}`, tip: `Your pages and questions are sent to ${cloud.join(' and ')}. They are not private. Click to change.` }
    : anyAi(settings, connectors)
      ? { cls: 'priv', icon: 'lock' as const, text: 'Private', tip: 'Lindley’s AI runs on computers you control. Click for AI settings.' }
      : { cls: 'off', icon: 'warn' as const, text: 'No AI', tip: 'No AI is connected. Click to connect one.' }

  const say = (m: Msg) => setMsgs((x) => [...x, m])

  const ask = async (question: string) => {
    const text = question.trim()
    if (!text || busy) return
    say({ from: 'me', text })
    setQ('')
    setBusy(true)
    try {
      const find = text.match(/(?:mention(?:ing|s)?|find|search for)\s+(.+?)[?.!]*$/i)
      if (find) {
        const term = find[1].replace(/^(every|all)\s+(pages?|letters?|documents?)\s+(mentioning|about|with)\s+/i, '')
        const r = await api.search(term)
        if (!r.results.length) {
          say({ from: 'ai', text: `I couldn’t find “${term}” on any page. Handwriting is sometimes read loosely, so try a surname on its own, or another spelling.` })
          return
        }
        const top = r.results.slice(0, 6)
        say({
          from: 'ai',
          text: `I searched the text of every page (no AI needed for that) and found “${term}” on ${plural(r.results.length, 'page')}${r.results.length === 50 ? ' or more' : ''}. The first few:`,
          list: top.map((x) => (x.document_name ? `${x.document_name}, page ${x.page_number}` : `${x.where === 'aside' ? 'Set aside' : 'Inbox'}: ${x.file}`)),
          acts: [
            ...top.slice(0, 3).map((x) => ({
              label: `Open ${x.document_name ? `“${x.document_name.slice(0, 22)}${x.document_name.length > 22 ? '…' : ''}”` : x.file}`,
              tip: 'Open this page beside its text',
              go: () => nav(x.document_id ? `/documents/${x.document_id}?view=side&page=${(x.page_number ?? 1) - 1}` : `/scans/${x.page_id}`),
            })),
            { label: 'See all results', tip: `Search for “${term}”`, go: () => nav(`/search?q=${encodeURIComponent(term)}`) },
          ],
        })
        return
      }
      if (/review|unsure|not sure|check/i.test(text)) {
        const r = await api.review()
        if (!r.count) {
          say({ from: 'ai', text: 'Nothing needs your review right now. Every page I’ve read is either confident or checked by you.' })
          return
        }
        say({
          from: 'ai',
          text: `I’m not confident I read ${plural(r.count, 'page')} correctly:`,
          list: r.groups.map((g) => `${g.document ? g.document.name : 'Inbox'}: ${plural(g.pages.length, 'page')}`),
          acts: [{ label: 'Start reviewing', tip: 'Go through them one by one', go: () => nav('/review/all') }],
        })
        return
      }
      if (!chat) {
        say({
          from: 'ai',
          text: 'No AI is connected for answering questions, so I can’t answer that. I can still find pages that mention a word: try “Find” and a name. You can connect an AI on this computer, which keeps your scans private, or a cloud AI, in Settings.',
          acts: [{ label: 'Open AI settings', go: () => nav('/settings/ai') }],
        })
        return
      }
      try {
        const r = await api.chat(text)
        say({ from: 'ai', text: r.answer })
      } catch {
        say({
          from: 'ai',
          text: `Answering questions about your documents with ${chat.label} isn’t built yet: it’s the next part of Lindley. Nothing was sent to it. Meanwhile I can find every page that mentions a word: try “Find” and a name.`,
        })
      }
    } catch (e) {
      say({ from: 'ai', text: (e as Error).message })
    } finally {
      setBusy(false)
    }
  }

  const input = useRef<HTMLTextAreaElement>(null)
  // Suggestions: one to ask at once, one to finish typing.
  const sugg: { label: string; ask?: string; fill?: string; tip: string }[] = [
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
      <div className="ai-log" ref={log} aria-live="polite">
        {msgs.map((m, i) => (
          <div key={i} className={`msg ${m.from}`}>
            <p>{m.text}</p>
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
            <span className="typing">
              <i />
              <i />
              <i />
            </span>
          </div>
        )}
      </div>
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
          placeholder={!chat ? 'Find a name or word on every page' : chat.cloud ? `Ask about your documents (sent to ${chat.company})` : 'Ask about your documents'}
          aria-label="Ask Lindley a question"
        />
        <button className="btn primary" aria-label="Send" data-tip="Send your question (Enter). Shift+Enter starts a new line." disabled={busy}>
          <Icon name="send" />
        </button>
      </form>
    </aside>
  )
}
