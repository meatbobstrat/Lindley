// The Inbox in the order it's shown: newest first, oldest first, or Lindley's matches first, as the
// person chose. Shared by the Inbox and a scan opened from it, so Next goes where the grid does.

import { useMemo } from 'react'
import { useLocation } from 'react-router'
import { api, type Suggestion } from '../api/client'
import { useApi, useLocal } from '../api/store'

export type InboxSort = 'new' | 'old' | 'match'

const RANK = { add_to_document: 0, group_pages: 1, set_aside: 2 } as const

export function useInboxOrder() {
  const inbox = useApi('inbox', api.inbox)
  const sugs = useApi('suggestions', api.suggestions)
  const loc = useLocation()
  const [sort, setSort] = useLocal<InboxSort>('sort', (loc.state as { sort?: 'match' } | null)?.sort ?? 'new')

  const pages = useMemo(() => inbox.data?.pages ?? [], [inbox.data])
  const inInbox = useMemo(() => new Set(pages.map((p) => p.id)), [pages])

  // Each page's hint: the best of its open suggestions, while its pages are all still here
  const hintOf = useMemo(() => {
    const m = new Map<number, Suggestion>()
    ;(sugs.data?.suggestions ?? []).forEach((s) => {
      if (!(s.payload.pages ?? [s.page_id]).every((id) => inInbox.has(id))) return
      const ids = s.kind === 'group_pages' ? (s.payload.pages ?? [s.page_id]) : [s.page_id]
      ids.forEach((id) => {
        const was = m.get(id)
        if (!was || RANK[s.kind] < RANK[was.kind]) m.set(id, s)
      })
    })
    return m
  }, [sugs.data, inInbox])

  const shown = useMemo(() => {
    const a = [...pages]
    if (sort === 'old') a.reverse()
    if (sort === 'match') a.sort((x, y) => Number(hintOf.has(y.id)) - Number(hintOf.has(x.id)))
    return a
  }, [pages, sort, hintOf])

  return { inbox, sugs, pages, inInbox, hintOf, sort, setSort, shown }
}
