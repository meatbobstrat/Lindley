// What every view needs, shared through context (lib/app.tsx provides it).

import { createContext, useContext, useEffect } from 'react'
import type { Connector, DocSummary, Folder, Job, Overview, Scope, Settings, Tier } from '../api/client'
import type { Conn } from './ai'

export interface AppData {
  overview: Overview | undefined
  offline: boolean
  settings: Settings | undefined
  connectors: Connector[]
  tiers: Tier[]
  docs: Map<number, DocSummary>
  folders: Map<number, Folder>
  folderPath: (id: number | null) => string[]
  ai: (job: Job) => Conn | null
  /** What Ask Lindley is looking at, said by the view that's open, and the page or document. */
  looking: string
  scope: Scope
  setLooking: (s: string, scope?: Scope) => void
}

export const AppCtx = createContext<AppData | null>(null)

export function useApp(): AppData {
  const a = useContext(AppCtx)
  if (!a) throw new Error('useApp outside AppProvider')
  return a
}

/** The view says what Ask Lindley is looking at: in words, and which page or document it is. */
export function useLooking(label: string, scope: Scope = {}) {
  const { setLooking } = useApp()
  const { document_id, page_id } = scope
  useEffect(() => setLooking(label, { document_id, page_id }), [label, document_id, page_id, setLooking])
}

/** Where a document lives, in words: its folder path, or In progress or Completed. */
export function docHome(d: Pick<DocSummary, 'folder_id' | 'status'>, folderPath: (id: number | null) => string[]): string {
  return d.folder_id != null ? `In ${folderPath(d.folder_id).join(' › ')}` : d.status === 'complete' ? 'Completed' : 'In progress'
}
