// What every view needs, shared through context (lib/app.tsx provides it).

import { createContext, useContext, useEffect } from 'react'
import type { Connector, DocSummary, Folder, Job, Overview, Settings } from '../api/client'
import type { Conn } from './ai'

export interface AppData {
  overview: Overview | undefined
  offline: boolean
  settings: Settings | undefined
  connectors: Connector[]
  docs: Map<number, DocSummary>
  folders: Map<number, Folder>
  folderPath: (id: number | null) => string[]
  ai: (job: Job) => Conn | null
  /** What Ask Lindley is looking at, said by the view that's open. */
  looking: string
  setLooking: (s: string) => void
}

export const AppCtx = createContext<AppData | null>(null)

export function useApp(): AppData {
  const a = useContext(AppCtx)
  if (!a) throw new Error('useApp outside AppProvider')
  return a
}

/** The view says what Ask Lindley is looking at. */
export function useLooking(label: string) {
  const { setLooking } = useApp()
  useEffect(() => setLooking(label), [label, setLooking])
}

/** Where a document lives, in words: its folder path, or In progress or Completed. */
export function docHome(d: Pick<DocSummary, 'folder_id' | 'status'>, folderPath: (id: number | null) => string[]): string {
  return d.folder_id != null ? `In ${folderPath(d.folder_id).join(' › ')}` : d.status === 'complete' ? 'Completed' : 'In progress'
}
