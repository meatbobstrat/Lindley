// Actions on pages and documents, shared through context (lib/actions.tsx provides them).

import { createContext, useContext } from 'react'
import type { DocSummary } from '../api/client'

export interface Actions {
  rotate: (ids: number[], deg: 90 | -90) => void
  toInbox: (ids: number[]) => void
  setAside: (ids: number[]) => void
  moveTo: (ids: number[], doc: DocSummary | { id: number; name: string }) => void
  /** Move pages one place earlier or later in their document. */
  shift: (docId: number, order: number[], ids: number[], dir: -1 | 1) => void
  newDocument: (ids: number[], name?: string, lindley?: boolean) => void
  confirmExport: (doc: { id: number; name: string; pages: number; suggested: boolean; folder_id: number | null }, toReview: number) => void
  editDetails: (doc: { id: number; name: string; doc_type: string | null; doc_date: string | null }) => void
  addScans: (files: File[]) => void
  fileDoc: (doc: { id: number; name: string; folder_id: number | null; status: string }, folderId: number | null) => void
  newFolder: (parent: number | null, docId?: number) => void
  moveMenu: (ids: number[], anchor: Element, here?: number) => void
  folderMenu: (doc: { id: number; name: string; folder_id: number | null; status: string }, anchor: Element) => void
  pageMenu: (ids: number[], where: 'inbox' | 'aside' | 'document', anchor: Element, opts?: { at?: { x: number; y: number }; docId?: number; order?: number[]; noBasics?: boolean }) => void
}

export const ActionsCtx = createContext<Actions | null>(null)

export function useActions(): Actions {
  const a = useContext(ActionsCtx)
  if (!a) throw new Error('useActions outside ActionsProvider')
  return a
}

