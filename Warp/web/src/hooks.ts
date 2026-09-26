import { useCallback, useEffect, useRef, useState } from 'react'

export function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}

export interface Loaded<T> { data: T | null; error: string | null; loading: boolean; reload: () => void; updated: Date | null }

/**
 * Fetch on mount (and every `pollMs`, when given). Keeps the previous data while refreshing, and
 * reports the error text without throwing: pages render an honest error/empty state from it.
 * `fn` must be stable (module-level API function or useCallback).
 */
export function useLoad<T>(fn: () => Promise<T>, pollMs?: number): Loaded<T> {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [updated, setUpdated] = useState<Date | null>(null)
  const alive = useRef(true)
  const run = useCallback(() => {
    fn()
      .then((d) => { if (alive.current) { setData(d); setError(null); setUpdated(new Date()) } })
      .catch((e) => { if (alive.current) setError(errorText(e)) })
      .finally(() => { if (alive.current) setLoading(false) })
  }, [fn])
  useEffect(() => {
    alive.current = true
    run()
    const id = pollMs ? setInterval(run, pollMs) : undefined
    return () => { alive.current = false; if (id) clearInterval(id) }
  }, [run, pollMs])
  return { data, error, loading, reload: run, updated }
}

/** Host[:port]/db portion of a JDBC URL for compact "target" columns (never includes credentials). */
export function targetOf(url: string): string {
  return url.replace(/^jdbc:[^:]+:(\/\/)?/, '').replace(/\?.*$/, '')
}
