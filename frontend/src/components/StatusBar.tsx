// The status bar: where the AI runs, what's being watched and read, what the AI is doing, and
// what's waiting. When AI work a person asked for is done, it says so in a toast.

import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router'
import type { AiWorking } from '../api/client'
import { anyAi, cloudInUse } from '../lib/ai'
import { useApp } from '../lib/appContext'
import { plural } from '../lib/words'
import { useFeedback } from '../ui/feedbackContext'
import { Icon, Mark } from '../ui/icons'

/** "AI reading page 2 of 5", "AI sorting 6 pages · question 1 of 2 so far": answers can raise more */
function doing(w: AiWorking): string {
  if (w.kind === 'sort') {
    const what = `AI sorting ${w.pages ? plural(w.pages, 'page') : 'the Inbox'}`
    return w.of > 0 ? `${what} · question ${Math.min(w.done + 1, w.of)} of ${w.of} so far` : what
  }
  return w.of > 1 ? `AI reading page ${Math.min(w.done + 1, w.of)} of ${w.of}` : 'AI reading a page'
}

export function StatusBar() {
  const { overview, settings, connectors, offline } = useApp()
  const { toast } = useFeedback()
  const nav = useNavigate()
  const c = overview?.counts
  const cloud = cloudInUse(settings, connectors)
  const folders = settings?.watch_folders ?? []
  const working = overview?.ai.working ?? []
  const queued = overview?.ai.waiting.length ?? 0

  // Work a person asked for that's done since this page opened: said once, in a toast
  const seen = useRef<number | null>(null)
  const finished = overview?.ai.finished
  useEffect(() => {
    if (!finished) return
    const last = finished.length ? finished[finished.length - 1].id : 0
    if (seen.current !== null) finished.filter((f) => f.id > seen.current!).forEach((f) => toast(f.message))
    seen.current = Math.max(seen.current ?? 0, last)
  }, [finished, toast])

  return (
    <footer className="status">
      {offline && (
        <span className="offline" role="alert" data-tip="Start it with scripts\dev.ps1, or python -m lindley in the backend folder. This page reconnects by itself.">
          <Icon name="warn" /> Lindley’s backend isn’t answering
        </span>
      )}
      <button
        onClick={() => nav('/settings/ai')}
        data-tip={
          cloud.length
            ? `Your scans are sent to ${cloud.join(' and ')} for some jobs. They are not private. Click to change.`
            : 'Where Lindley’s AI runs. Click to change.'
        }
      >
        {cloud.length ? (
          <>
            <Icon name="cloud" /> AI: sent to {cloud.join(' and ')} · not private
          </>
        ) : anyAi(settings, connectors) ? (
          <>
            <Icon name="lock" /> AI: private, on your own computers
          </>
        ) : (
          <>
            <Icon name="warn" /> AI: not connected
          </>
        )}
      </button>
      {settings && (
        <button
          className="hide-narrow"
          onClick={() => nav('/settings/scans')}
          data-tip={folders.length ? `Watching: ${folders.join(', ')}. Click to change.` : 'No folders are watched. Click to add one.'}
        >
          <Icon name="eye" /> Watching {plural(folders.length, 'folder')} · {settings.move_files ? 'moving' : 'copying'} new scans
        </button>
      )}
      <span
        data-tip={
          c?.reading || c?.reading_again
            ? `Lindley reads ${c.reading ? 'new scans' : 'the pages you turned again, the right way round,'} in the background. You can keep working meanwhile.`
            : 'Every scan that has arrived has been read.'
        }
      >
        <i className={`dot ${c?.reading || c?.reading_again ? 'busy' : ''}`} aria-hidden="true" />
        {c?.reading
          ? `Reading ${plural(c.reading, 'new scan')}${c.reading_again ? ` and ${plural(c.reading_again, 'turned page')}` : ''}`
          : c?.reading_again
            ? `Reading ${plural(c.reading_again, 'turned page')} again`
            : 'All scans read'}
      </span>
      {working.map((w, i) => (
        <button
          key={i}
          onClick={() => nav('/needs-ai')}
          data-tip={`${w.connection ?? 'The AI'} is ${w.kind === 'sort' ? 'sorting pages' : 'reading hard pages'}, ${w.asked ? 'as you asked' : 'on its own'}.${
            w.kind === 'sort' ? ' It’s asked about the pages the rules couldn’t settle, a few at a time, and more questions can come up as it goes.' : ''
          } You can keep working meanwhile.${
            queued ? ` ${plural(queued, 'more request')} ${queued === 1 ? 'waits' : 'wait'} behind it.` : ''
          }`}
        >
          <i className="dot busy" aria-hidden="true" />
          {doing(w)}
          {i === 0 && queued ? ` · ${queued} more waiting` : ''}
        </button>
      ))}
      {!!c?.failed && (
        <span data-tip="Scans Lindley couldn’t read. They’re in the quarantine folder, and Lindley tries them again when it next starts.">
          <Icon name="warn" /> {plural(c.failed, 'scan')} couldn’t be read
        </span>
      )}
      {!!c?.review && (
        <button onClick={() => nav('/review')} data-tip="Pages Lindley isn’t sure it read right. Click to review them.">
          <Icon name="flag" /> {plural(c.review, 'page')} to review
        </button>
      )}
      {!!c?.duplicates && (
        <button onClick={() => nav('/duplicates')} data-tip="Pages that may have been scanned more than once. Click to compare them.">
          <Icon name="dup" /> {plural(c.duplicates, 'possible duplicate')}
        </button>
      )}
      {!!c?.needs_ai && (
        <button onClick={() => nav('/needs-ai')} data-tip="Scans waiting for an AI to read or sort them. Click to see them.">
          <Mark /> {plural(c.needs_ai, 'scan')} waiting for an AI
        </button>
      )}
      {overview && (
        <span className="hide-narrow" data-tip="The version of Lindley’s backend">
          v{overview.version}
        </span>
      )}
    </footer>
  )
}
