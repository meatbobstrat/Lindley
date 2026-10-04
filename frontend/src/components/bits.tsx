// Small pieces every view uses. Status is always said in words, with an icon, never by colour
// alone, and anything shortened says the rest in its tooltip.

import { type CSSProperties, type ReactNode, useState } from 'react'
import { useNavigate } from 'react-router'
import { imageAt, type Page } from '../api/client'
import type { Conn } from '../lib/ai'
import { readLong, readShort } from '../lib/words'
import { Icon, type IconName, Mark } from '../ui/icons'

export function Banner({
  kind,
  icon,
  children,
  actions,
  role,
}: {
  kind: 'ok' | 'warn' | 'ai' | 'review' | 'danger'
  icon?: IconName
  children: ReactNode
  actions?: ReactNode
  role?: string
}) {
  const ic = icon ?? ({ ok: 'checkc', warn: 'warn', ai: 'lindley', review: 'flag', danger: 'cloud' } as const)[kind]
  return (
    <div className={`banner ${kind}`} role={role}>
      <Icon name={ic} />
      <div className="b-body">
        <div className="b-text">{children}</div>
        {actions && <span className="b-acts">{actions}</span>}
      </div>
    </div>
  )
}

/** Where a connection sends things, in words: private, your server, or not private. */
export function PrivTag({ c }: { c: Conn }) {
  if (c.reach === 'cloud')
    return (
      <span className="ptag cloud" data-tip={`Pages and questions this AI handles are sent over the internet to ${c.company}. They are not private.`}>
        <Icon name="cloud" />
        Not private · sent to {c.company}
      </span>
    )
  if (c.reach === 'server')
    return (
      <span className="ptag server" data-tip="This address isn’t on your own network. Use it only if you or your organization run that server.">
        <Icon name="warn" />
        Your server · check you control it
      </span>
    )
  return (
    <span className="ptag priv" data-tip="Your scans stay on computers you control. Nothing is sent to an AI company.">
      <Icon name="lock" />
      Private · {c.reach === 'this' ? 'this computer' : 'your network'}
    </span>
  )
}

export function LimitsTag({ c }: { c: Conn }) {
  const auto = c.cfg.allow === 'auto'
  const parts = [auto ? 'Runs on its own' : 'Asks you first']
  if (auto) {
    const caps = [c.cfg.daily_limit && `${c.cfg.daily_limit.toLocaleString()} a day`, c.cfg.monthly_limit && `${c.cfg.monthly_limit.toLocaleString()} a month`].filter(Boolean)
    parts.push(caps.length ? `up to ${caps.join(', ')}` : 'no daily or monthly limit')
  }
  if (c.cfg.per_minute) parts.push(`${c.cfg.per_minute} a minute`)
  parts.push(`${c.cfg.at_once ?? 2} at once`)
  return (
    <span
      className="ptag"
      data-tip={
        auto
          ? 'Lindley sends work to this AI by itself, as it arrives, until a limit is reached. Then it waits for your OK again.'
          : 'Nothing is sent to this AI until you say so. Work waits in Needs AI.'
      }
    >
      <Icon name={auto ? 'send' : 'flag'} />
      {parts.join(' · ')}
    </span>
  )
}

/** How well a page was read, and what it's waiting for. Clicking it goes there. */
export function ConfChip({ p, onReview }: { p: Page; onReview?: () => void }) {
  const nav = useNavigate()
  const how = readShort(p)
  const long = readLong(p)
  const pct = p.confidence != null ? ` ${p.confidence}%` : ''
  const go = (to: string) => (e: React.MouseEvent) => {
    e.stopPropagation()
    nav(to)
  }
  switch (p.state) {
    case 'reading':
      return (
        <span className="conf reading" data-tip="Lindley is still reading this scan. Its text will appear here soon.">
          Reading…
        </span>
      )
    case 'failed':
      return (
        <span className="conf flag" data-tip={`Lindley couldn’t read this scan${p.why ? `: ${p.why}` : ''}. It tries again when Lindley next starts.`}>
          <Icon name="warn" />
          Couldn’t read
        </span>
      )
    case 'checked':
      return (
        <span className="conf mine" data-tip={`${long}. You checked the text, so it’s trusted for search.`}>
          <Icon name="check" />
          Checked
        </span>
      )
    case 'ai_failed':
      return (
        <span
          className="conf ai"
          onClick={go('/needs-ai')}
          data-tip={`The AI was asked to read this scan and ${p.why || 'didn’t manage'}. ${long}. Click to try again in Needs AI.`}
        >
          <Icon name="warn" />
          AI failed · {how}
          {pct}
        </span>
      )
    case 'ai_reading':
      return (
        <span className="conf ai" data-tip={`The AI is reading this scan now. ${long} meanwhile.`}>
          <i className="dot busy" aria-hidden="true" />
          AI reading…
        </span>
      )
    case 'needs_ai':
      return (
        <span
          className="conf ai"
          onClick={go('/needs-ai')}
          data-tip={`This scan needs an AI to read it: ${long}, so its text is a rough guess. Click to see what’s waiting for the AI.`}
        >
          <Mark />
          Needs AI · {how}
          {pct}
        </span>
      )
    case 'review':
      return (
        <span
          className="conf flag"
          data-review
          onClick={(e) => {
            e.stopPropagation()
            onReview?.()
          }}
          data-tip={`${long}: Lindley isn’t sure it read this right. Click to review this page.`}
        >
          <Icon name="flag" />
          Review · {how}
          {pct}
        </span>
      )
    default:
      return (
        <span className="conf" data-tip={`${long}. Confident enough to trust for search.`}>
          {how}
          {pct}
        </span>
      )
  }
}

export function Thumb({ p, size = 300, alt = '' }: { p: Pick<Page, 'image' | 'blank'>; size?: number; alt?: string }) {
  return (
    <span className="pg-img">
      <img src={imageAt(p.image, size)} alt={alt} loading="lazy" draggable={false} className={p.blank ? 'blank' : undefined} />
    </span>
  )
}

export function Done({ title, children, action }: { title: string; children: ReactNode; action?: ReactNode }) {
  return (
    <div className="rv-done">
      <Icon name="checkc" />
      <h2>{title}</h2>
      <p>{children}</p>
      {action}
    </div>
  )
}

export function Loading({ what = 'Opening the archive…' }: { what?: string }) {
  return <div className="loading small">{what}</div>
}

export function ErrorBox({ error }: { error: Error }) {
  return (
    <div className="err" role="alert">
      <Icon name="warn" />
      <div>
        <b>Lindley couldn’t load this.</b> {error.message}
      </div>
    </div>
  )
}

export function Meter({ value, label, tip }: { value: number; label: string; tip: string }) {
  return (
    <span className="meter" data-tip={tip}>
      {label} <b style={{ '--v': `${value}%` } as CSSProperties} aria-hidden="true" /> {value}%
    </span>
  )
}

export function Head({ crumbs, title, sub, children }: { crumbs: ReactNode; title: ReactNode; sub?: ReactNode; children?: ReactNode }) {
  return (
    <div className="head">
      <div className="crumbs">{crumbs}</div>
      <div className="title-row">{typeof title === 'string' ? <h1 className="title">{title}</h1> : title}</div>
      {sub && <p className="sub">{sub}</p>}
      {children}
      {!children && <div style={{ height: 6 }} />}
    </div>
  )
}

/** A link-like button for breadcrumbs. */
export function Crumb({ to, children, tip }: { to: string; children: ReactNode; tip?: string }) {
  const nav = useNavigate()
  return (
    <button className="crumb" onClick={() => nav(to)} data-tip={tip ?? `Back to ${typeof children === 'string' ? children : 'the list'}`}>
      {children}
    </button>
  )
}

/** A name you edit in place: saved when you press Enter or leave it, put back with Escape.
 * Give it key={value}, so a new saved name starts it afresh. */
export function NameInput({
  value,
  onSave,
  readOnly,
  label,
  tip,
  className = 'title-in',
}: {
  value: string
  onSave: (v: string) => void
  readOnly?: boolean
  label: string
  tip: string
  className?: string
}) {
  const [v, setV] = useState(value)
  return (
    <input
      className={className}
      value={v}
      readOnly={readOnly}
      aria-label={label}
      data-tip={tip}
      onChange={(e) => setV(e.target.value)}
      onBlur={() => {
        const t = v.trim()
        if (!t) setV(value)
        else if (t !== value) onSave(t)
      }}
      onKeyDown={(e) => {
        if (e.key === 'Enter') e.currentTarget.blur()
        if (e.key === 'Escape') setV(value)
      }}
    />
  )
}
