// Wave 2A endpoints: sampled metrics history, cache counters, policy decisions, audit, Prometheus text.
// Kept apart from client.ts so this file merges cleanly with the other wave-2 work; it reuses the same stored connection.
import { clearConnection, getRequestTimeoutMs, getStoredConnection } from './client'

async function call<T>(path: string, init: RequestInit = {}, text = false): Promise<T> {
  const conn = getStoredConnection()
  if (!conn) { window.location.href = '/connect'; throw new Error('not connected') }
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), getRequestTimeoutMs())
  let res: Response
  try {
    res = await fetch(`${conn.baseUrl}${path}`, {
      ...init, signal: controller.signal,
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${conn.token}`, ...init.headers },
    })
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') throw new Error('Request timed out')
    throw e
  } finally { clearTimeout(timer) }
  if (res.status === 401) { clearConnection(); window.location.href = '/connect'; throw new Error('admin token rejected') }
  if (text) {
    if (!res.ok) throw new Error(`Request failed (HTTP ${res.status})`)
    return (await res.text()) as T
  }
  const body = res.headers.get('content-type')?.includes('application/json') ? await res.json() : null
  if (!res.ok) throw new Error(body?.error ?? `Request failed (HTTP ${res.status})`)
  return body as T
}

// --- /api/metrics/history -------------------------------------------------------------------------

export interface HistorySample { t: number; c: Record<string, number> }
export interface MetricsHistory { enabled: boolean; intervalSeconds?: number; capacity?: number; samples: HistorySample[] }
export const getMetricsHistory = () => call<MetricsHistory>('/api/metrics/history')

export interface RatePoint { t: number; v: number }

/** Per-second rate of one cumulative counter between consecutive samples (a decrease, i.e. a restart, is skipped). */
export function ratePoints(samples: HistorySample[], key: string | ((c: Record<string, number>) => number | null)): RatePoint[] {
  const get = typeof key === 'function' ? key : (c: Record<string, number>) => (key in c ? c[key] : null)
  const out: RatePoint[] = []
  for (let i = 1; i < samples.length; i++) {
    const a = get(samples[i - 1].c); const b = get(samples[i].c)
    const dt = (samples[i].t - samples[i - 1].t) / 1000
    if (a === null || b === null || dt <= 0 || b < a) continue
    out.push({ t: samples[i].t, v: (b - a) / dt })
  }
  return out
}

/** Average of (delta numerator / delta denominator) between consecutive samples; windows with no new calls are skipped. */
export function ratioPoints(samples: HistorySample[], num: string, den: string): RatePoint[] {
  const out: RatePoint[] = []
  for (let i = 1; i < samples.length; i++) {
    const a = samples[i - 1].c; const b = samples[i].c
    if (!(num in b) || !(den in b)) continue
    const dn = b[num] - (a[num] ?? 0); const dd = b[den] - (a[den] ?? 0)
    if (dd <= 0 || dn < 0) continue
    out.push({ t: samples[i].t, v: dn / dd })
  }
  return out
}

export function sumKeys(c: Record<string, number>, prefix: string): number {
  return Object.entries(c).reduce((s, [k, v]) => (k.startsWith(prefix) ? s + v : s), 0)
}

// --- /api/cache/stats, /api/cache/invalidate ------------------------------------------------------

export interface CacheTier { entries: number; hits: number; misses: number }
export interface CacheStatsResponse {
  enabled: boolean
  clusterNodes: number
  tablePatterns?: string[]
  ttlMillis?: number
  tiers: {
    result?: CacheTier
    pk?: CacheTier
    row?: CacheTier & { attached: boolean; invalidatedKeys: number }
    translation?: { entries: number; maxEntries: number; hits: number; misses: number; evictions: number }
  }
  invalidations?: {
    entriesRemoved: number; events: number; fullClears: number
    recent: Array<{ at: string; kind: 'table' | 'clear'; target: string; entries: number; source: string }>
  }
  byTable?: Array<{ table: string; hits: number; misses: number }>
}
export const getCacheStats = () => call<CacheStatsResponse>('/api/cache/stats')
export const invalidateCache = (table?: string) =>
  call<{ ok: boolean; scope: 'all' | 'table'; table?: string }>('/api/cache/invalidate', { method: 'POST', body: JSON.stringify(table ? { table } : {}) })

// --- /api/policy-decisions, /api/audit --------------------------------------------------------------

export interface PolicyDecision {
  timestamp: string; policy: string; identity: string | null; target: string | null; decision: string; reason: string | null
  source: 'policy-log' | 'audit'
}
export const getPolicyDecisions = (limit = 100) =>
  call<{ decisions: PolicyDecision[]; policyLogRecorded: number; auditAvailable: boolean; mcpReadOnly: boolean }>(`/api/policy-decisions?limit=${limit}`)

// --- Prometheus text at /metrics ------------------------------------------------------------------

export interface PromSample { name: string; labels: Record<string, string>; value: number }

export function parsePrometheus(text: string): PromSample[] {
  const out: PromSample[] = []
  for (const line of text.split('\n')) {
    if (!line || line.startsWith('#')) continue
    const m = /^([a-zA-Z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)$/.exec(line)
    if (!m) continue
    const labels: Record<string, string> = {}
    if (m[2]) for (const lm of m[2].matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)) labels[lm[1]] = lm[2].replace(/\\"/g, '"').replace(/\\\\/g, '\\')
    const value = Number(m[3])
    if (Number.isFinite(value)) out.push({ name: m[1], labels, value })
  }
  return out
}
export const getPrometheus = async () => parsePrometheus(await call<string>('/metrics', {}, true))
