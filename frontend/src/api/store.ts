// Data from the API, kept per key and fetched again whenever anything changes: after a person's
// change (invalidate), and when the background work moves on (App watches the overview).

import { useCallback, useEffect, useLayoutEffect, useRef, useState, useSyncExternalStore } from 'react'

let version = 0
const listeners = new Set<() => void>()
const cache = new Map<string, unknown>()

/** Something changed: every view fetches its data again, showing what it had meanwhile. */
export function invalidate() {
  version++
  listeners.forEach((l) => l())
}

function subscribe(l: () => void) {
  listeners.add(l)
  return () => {
    listeners.delete(l)
  }
}

export interface Loaded<T> {
  data: T | undefined
  error: Error | undefined
  reload: () => void
}

/** `key` names the data (null: nothing to fetch yet); `fn` fetches it. */
export function useApi<T>(key: string | null, fn: () => Promise<T>): Loaded<T> {
  const v = useSyncExternalStore(subscribe, () => version)
  const [, force] = useState(0)
  const [error, setError] = useState<Error>()
  const fnRef = useRef(fn)
  useLayoutEffect(() => {
    fnRef.current = fn
  })
  const reload = useCallback(() => force((n) => n + 1), [])

  useEffect(() => {
    if (key === null) return
    let live = true
    fnRef.current().then(
      (d) => {
        if (!live) return
        cache.set(key, d)
        setError(undefined)
        force((n) => n + 1)
      },
      (e: Error) => {
        if (live) setError(e)
      },
    )
    return () => {
      live = false
    }
  }, [key, v])

  return { data: key === null ? undefined : (cache.get(key) as T | undefined), error, reload }
}

/** A value kept in this browser (per viewer): a remembered size, position or choice. Every
 * component using the same name sees a change at once. */
export function useLocal<T>(name: string, initial: T): [T, (v: T) => void] {
  const key = `lindley-${name}`
  const read = useCallback((): T => {
    try {
      const raw = localStorage.getItem(key)
      return raw === null ? initial : (JSON.parse(raw) as T)
    } catch {
      return initial
    }
    // `initial` is only the first value
  }, [key]) // oxlint-disable-line react-hooks/exhaustive-deps
  const [value, setValue] = useState<T>(read)
  useEffect(() => {
    const on = (e: Event) => {
      if ((e as CustomEvent<string>).detail === key) setValue(read())
    }
    window.addEventListener('lindley-local', on)
    return () => window.removeEventListener('lindley-local', on)
  }, [key, read])
  const set = useCallback(
    (v: T) => {
      try {
        localStorage.setItem(key, JSON.stringify(v))
      } catch {
        // it just won't be remembered past this page
      }
      setValue(v)
      window.dispatchEvent(new CustomEvent('lindley-local', { detail: key }))
    },
    [key],
  )
  return [value, set]
}
