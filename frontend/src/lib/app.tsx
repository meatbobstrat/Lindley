// What every view needs: the overview (counts, documents, folders), settings, connectors, and the
// tiers and helps.
// The overview is checked every few seconds; when the background work has moved on (a scan
// read, pages sorted), every view fetches its data again.

import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { api, type Job, type Scope } from '../api/client'
import { invalidate, useApi } from '../api/store'
import { forJob } from './ai'
import { AppCtx, type AppData } from './appContext'

const POLL_MS = 4000

export function AppProvider({ children }: { children: ReactNode }) {
  const ov = useApi('overview', api.overview)
  const st = useApi('settings', api.settings)
  const cn = useApi('connectors', api.connectors)
  const tr = useApi('tiers', api.tiers)
  const [offline, setOffline] = useState(false)
  const [looking, setLookingNow] = useState<{ label: string; scope: Scope }>({ label: 'Your archive', scope: {} })
  const setLooking = useCallback((label: string, scope: Scope = {}) => setLookingNow({ label, scope }), [])
  const last = useRef<string>('')

  useEffect(() => {
    const t = window.setInterval(() => {
      api.overview().then(
        (o) => {
          setOffline(false)
          const now = JSON.stringify(o)
          if (last.current && now !== last.current) invalidate()
          last.current = now
        },
        () => setOffline(true),
      )
    }, POLL_MS)
    return () => window.clearInterval(t)
  }, [])
  useEffect(() => {
    if (ov.data) last.current = JSON.stringify(ov.data)
  }, [ov.data])

  const value = useMemo<AppData>(() => {
    const docs = new Map((ov.data?.documents ?? []).map((d) => [d.id, d]))
    const folders = new Map((ov.data?.folders ?? []).map((f) => [f.id, f]))
    const folderPath = (id: number | null) => {
      const out: string[] = []
      for (let f = id != null ? folders.get(id) : undefined; f; f = f.parent_id != null ? folders.get(f.parent_id) : undefined)
        out.unshift(f.name)
      return out
    }
    const connectors = cn.data ?? []
    return {
      overview: ov.data,
      offline: offline || (!!ov.error && !ov.data),
      settings: st.data,
      connectors,
      tiers: tr.data?.tiers ?? [],
      helps: tr.data?.helps ?? [],
      docs,
      folders,
      folderPath,
      ai: (job: Job) => forJob(st.data, connectors, job),
      looking: looking.label,
      scope: looking.scope,
      setLooking,
    }
  }, [ov.data, ov.error, st.data, cn.data, tr.data, offline, looking, setLooking])

  return <AppCtx.Provider value={value}>{children}</AppCtx.Provider>
}
