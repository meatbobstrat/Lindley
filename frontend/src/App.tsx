import { type ComponentType, useEffect, useRef, useState } from 'react'
import { Navigate, Route, Routes, useLocation, useNavigate, useParams, useSearchParams } from 'react-router'
import { api } from './api/client'
import { useApi } from './api/store'
import { AskPane } from './components/AskPane'
import { StatusBar } from './components/StatusBar'
import { Tree } from './components/Tree'
import { useActions } from './lib/actionsContext'
import { useApp } from './lib/appContext'
import { typing, useTheme } from './lib/view'
import { useFeedback } from './ui/feedbackContext'
import { Icon, Mark, MarkDefs } from './ui/icons'
import { TooltipLayer } from './ui/Tooltip'
import { AsideView, InboxView } from './views/Inbox'
import { DocumentView } from './views/Document'
import { DupList, DupView } from './views/Duplicates'
import { FolderView } from './views/Folder'
import { NeedsAiItem, NeedsAiList } from './views/NeedsAi'
import { ReviewList, ReviewView } from './views/Review'
import { ScanView } from './views/Scan'
import { SearchView } from './views/Search'
import { SettingsView } from './views/Settings'
import { Setup } from './views/Setup'

/** A view that starts afresh for each id in the address. */
function keyed(C: ComponentType) {
  return function Keyed() {
    const p = useParams()
    return <C key={Object.values(p).join('/')} />
  }
}
const Doc = keyed(DocumentView)
const Folder = keyed(FolderView)
const Scan = keyed(ScanView)
const Review = keyed(ReviewView)
const Dup = keyed(DupView)
const NeedsAi = keyed(NeedsAiItem)

export default function App() {
  const [theme, setTheme] = useTheme()
  const loc = useLocation()
  // The folder tree (in a narrow window) is open on the page it was opened on, so a new page closes it
  const [treeOn, setTreeOn] = useState<string | null>(null)
  const showTree = treeOn === loc.pathname
  const setShowTree = (on: boolean) => setTreeOn(on ? loc.pathname : null)
  const [showAi, setShowAi] = useState(false)
  const [fileDrop, setFileDrop] = useState(false)
  const { undo } = useFeedback()
  const acts = useActions()
  const nav = useNavigate()
  const [params] = useSearchParams()
  const { settings, connectors, tiers } = useApp()
  const [q, setQ] = useState(params.get('q') ?? '')
  const main = useRef<HTMLElement>(null)
  const setup = useApi('setup', api.setupNeeded)

  // The theme: as the computer has it, or chosen here
  useEffect(() => {
    if (theme === 'system') delete document.documentElement.dataset.theme
    else document.documentElement.dataset.theme = theme
  }, [theme])
  const dark = theme === 'dark' || (theme === 'system' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  // A new place: focus moves to it
  useEffect(() => {
    main.current?.focus({ preventScroll: true })
  }, [loc.pathname])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setTreeOn(null)
        setShowAi(false)
      }
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z' && !e.shiftKey && !typing(e.target) && !document.querySelector('dialog[open]')) {
        e.preventDefault()
        undo()
      }
    }
    // Scans dropped in from the computer go to the Inbox
    const hasFiles = (e: DragEvent) => [...(e.dataTransfer?.types ?? [])].includes('Files')
    const over = (e: DragEvent) => {
      if (!hasFiles(e)) return
      e.preventDefault()
      setFileDrop(true)
    }
    const leave = (e: DragEvent) => {
      if (hasFiles(e) && !e.relatedTarget) setFileDrop(false)
    }
    const drop = (e: DragEvent) => {
      if (!hasFiles(e)) return
      e.preventDefault()
      setFileDrop(false)
      acts.addScans([...(e.dataTransfer?.files ?? [])])
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('dragover', over)
    document.addEventListener('dragleave', leave)
    document.addEventListener('drop', drop)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('dragover', over)
      document.removeEventListener('dragleave', leave)
      document.removeEventListener('drop', drop)
    }
  }, [undo, acts])

  return (
    <>
      <MarkDefs />
      <div className={`app${showTree ? ' show-tree' : ''}${showAi ? ' show-ai' : ''}`}>
        <header className="top">
          <button
            className="icon-btn mobile-only"
            aria-label="Show folders"
            data-tip="Show the Inbox, documents and folders"
            onClick={() => {
              setShowTree(!showTree)
              setShowAi(false)
            }}
          >
            <Icon name="menu" />
          </button>
          <a className="brand" href="/inbox" onClick={(e) => (e.preventDefault(), nav('/inbox'))} data-tip="Lindley: go to the Inbox">
            <Mark />
            <span>Lindley</span>
          </a>
          <form
            className="search"
            role="search"
            onSubmit={(e) => {
              e.preventDefault()
              if (q.trim()) nav(`/search?q=${encodeURIComponent(q.trim())}`)
            }}
          >
            <Icon name="search" />
            <input
              type="search"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search every page, printed or handwritten"
              aria-label="Search every page"
              data-tip="Type words that are on the page, then press Enter. Every word must be there; the last can be the start of a word."
            />
          </form>
          <div className="spacer" />
          <button className="icon-btn" aria-label="Settings" data-tip="Settings: AI and privacy, folders, reading, library and appearance" onClick={() => nav('/settings/ai')}>
            <Icon name="gear" />
          </button>
          <button
            className="icon-btn"
            aria-label="Switch light or dark theme"
            data-tip={dark ? 'Switch to the light theme' : 'Switch to the dark theme'}
            onClick={() => setTheme(dark ? 'light' : 'dark')}
          >
            <Icon name={dark ? 'sun' : 'moon'} />
          </button>
          <button
            className="icon-btn mobile-only"
            aria-label="Ask Lindley"
            data-tip="Ask Lindley about your documents"
            onClick={() => {
              setShowAi(!showAi)
              setShowTree(false)
            }}
          >
            <Mark />
          </button>
        </header>

        <Tree onGo={() => setShowTree(false)} />

        <main className={`main${fileDrop ? ' file-drop' : ''}`} ref={main} tabIndex={-1}>
          <Routes>
            <Route path="/" element={<Navigate to="/inbox" replace />} />
            <Route path="/inbox" element={<InboxView />} />
            <Route path="/aside" element={<AsideView />} />
            <Route path="/review" element={<ReviewList />} />
            <Route path="/review/:scope" element={<Review />} />
            <Route path="/duplicates" element={<DupList />} />
            <Route path="/duplicates/:key" element={<Dup />} />
            <Route path="/needs-ai" element={<NeedsAiList />} />
            <Route path="/needs-ai/:id" element={<NeedsAi />} />
            <Route path="/documents/:id" element={<Doc />} />
            <Route path="/folders/:id" element={<Folder />} />
            <Route path="/search" element={<SearchView />} />
            <Route path="/scans/:id" element={<Scan />} />
            <Route path="/settings" element={<Navigate to="/settings/ai" replace />} />
            <Route path="/settings/:section" element={<SettingsView />} />
            <Route path="*" element={<Navigate to="/inbox" replace />} />
          </Routes>
        </main>

        <AskPane open={showAi} close={() => setShowAi(false)} />
        <StatusBar />
        {(showTree || showAi) && (
          <div
            className="scrim"
            onClick={() => {
              setShowTree(false)
              setShowAi(false)
            }}
          />
        )}
      </div>
      {setup.data?.needed && settings && connectors.length > 0 && tiers.length > 0 && <Setup settings={settings} connectors={connectors} tiers={tiers} />}
      <TooltipLayer />
    </>
  )
}
