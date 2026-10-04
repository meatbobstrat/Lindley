// The status bar: where the AI runs, what's being watched and read, and what's waiting.

import { useNavigate } from 'react-router'
import { anyAi, cloudInUse } from '../lib/ai'
import { useApp } from '../lib/appContext'
import { plural } from '../lib/words'
import { Icon, Mark } from '../ui/icons'

export function StatusBar() {
  const { overview, settings, connectors, offline } = useApp()
  const nav = useNavigate()
  const c = overview?.counts
  const cloud = cloudInUse(settings, connectors)
  const folders = settings?.watch_folders ?? []

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
      <span data-tip={c?.reading ? 'Lindley reads new scans in the background. You can keep working meanwhile.' : 'Every scan that has arrived has been read.'}>
        <i className={`dot ${c?.reading ? 'busy' : ''}`} aria-hidden="true" />
        {c?.reading ? `Reading ${plural(c.reading, 'new scan')}` : 'All scans read'}
      </span>
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
