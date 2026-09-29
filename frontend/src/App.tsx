import { useEffect, useState } from 'react'
import { NavLink, Navigate, Route, Routes } from 'react-router'
import { api, type Health } from './api/client'
import ChatPage from './pages/ChatPage'
import InboxPage from './pages/InboxPage'
import LibraryPage from './pages/LibraryPage'
import SearchPage from './pages/SearchPage'
import SettingsPage from './pages/SettingsPage'

const NAV = [
  { to: '/library', label: 'Library' },
  { to: '/search', label: 'Search' },
  { to: '/chat', label: 'Chat' },
  { to: '/inbox', label: 'Inbox' },
  { to: '/settings', label: 'Settings' },
]

function BackendStatus() {
  const [health, setHealth] = useState<Health | null>(null)
  const [error, setError] = useState(false)

  useEffect(() => {
    api.health().then(setHealth, () => setError(true))
  }, [])

  if (error) return <span className="status status-down">Backend offline</span>
  if (!health) return <span className="status">Connecting…</span>
  return <span className="status status-ok">Backend v{health.version}</span>
}

export default function App() {
  return (
    <div className="shell">
      <aside className="sidebar">
        <h1 className="brand">Lindley</h1>
        <nav>
          {NAV.map((item) => (
            <NavLink key={item.to} to={item.to}>
              {item.label}
            </NavLink>
          ))}
        </nav>
        <BackendStatus />
      </aside>
      <main className="content">
        <Routes>
          <Route path="/" element={<Navigate to="/library" replace />} />
          <Route path="/library" element={<LibraryPage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/chat" element={<ChatPage />} />
          <Route path="/inbox" element={<InboxPage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Routes>
      </main>
    </div>
  )
}
