// API calls used by the Routing & QoS, Access & ACLs and Compatibility lab pages (wave 2B). Kept out of client.ts so the
// shared client stays untouched; auth handling mirrors client.ts (Bearer admin token from sessionStorage, 401 -> /connect).
import { clearConnection, getStoredConnection } from './client'
import type { AbCompareEntry } from './client'

export class HttpError extends Error {
  status: number
  constructor(status: number, message: string) { super(message); this.status = status }
}

async function request(path: string): Promise<Response> {
  const conn = getStoredConnection()
  if (!conn) { window.location.href = '/connect'; throw new HttpError(401, 'not connected') }
  const res = await fetch(`${conn.baseUrl}${path}`, { headers: { Authorization: `Bearer ${conn.token}` } })
  if (res.status === 401) { clearConnection(); window.location.href = '/connect'; throw new HttpError(401, 'admin token rejected') }
  return res
}

async function getJson<T>(path: string): Promise<T> {
  const res = await request(path)
  const isJson = res.headers.get('content-type')?.includes('application/json')
  const body = isJson ? await res.json() : null
  if (!res.ok) throw new HttpError(res.status, body?.error ?? `Request failed (HTTP ${res.status})`)
  return body as T
}

// ---- QoS counters: Prometheus text from GET /metrics (warp_qos_admitted_total / warp_qos_rejected_total) ----

export interface QosCounter { tenant: string; workloadClass: string; admitted: number; rejected: number }

export async function getQosCounters(): Promise<QosCounter[]> {
  const res = await request('/metrics')
  if (!res.ok) throw new HttpError(res.status, `GET /metrics failed (HTTP ${res.status})`)
  return parseQosCounters(await res.text())
}

export function parseQosCounters(text: string): QosCounter[] {
  const map = new Map<string, QosCounter>()
  const re = /^warp_qos_(admitted|rejected)_total\{tenant="((?:[^"\\]|\\.)*)",workload_class="((?:[^"\\]|\\.)*)"\}\s+([0-9.eE+-]+)/
  for (const line of text.split('\n')) {
    const m = re.exec(line)
    if (!m) continue
    const key = `${m[2]}\u0000${m[3]}`
    const c = map.get(key) ?? { tenant: m[2], workloadClass: m[3], admitted: 0, rejected: 0 }
    if (m[1] === 'admitted') c.admitted = Number(m[4]); else c.rejected = Number(m[4])
    map.set(key, c)
  }
  return [...map.values()]
}

// ---- Audit stream: GET /api/audit ----

export type AuditType = string
export interface AuditEvent { timestamp: string; type: AuditType; userId: string | null; summary: string; details: Record<string, string> }

export async function getAudit(limit: number): Promise<AuditEvent[]> {
  return getJson(`/api/audit?limit=${limit}`)
}

// ---- Access summary (non-secret auth methods per frontend): GET /api/access-summary ----

export interface AccessFrontend { id: string; method: string; enforced: boolean; detail: string; principals?: string[] }
export interface AccessSummary {
  authMode: string | null
  sqlCredentials: { mode: 'single-shared' | 'multi-user'; users: string[] }
  frontends: AccessFrontend[]
  tls: { serverKeystoreConfigured: boolean; clientCertificatesRequired: boolean }
}

export async function getAccessSummary(): Promise<AccessSummary> {
  return getJson('/api/access-summary')
}

// ---- Workload capture: GET /api/capture (404 unless WARP_CAPTURE_ENABLED=true) ----

export interface CaptureEntry {
  localSeq: number; wallClock: string; nodeId: string; protocol: string; tenantId: string; sqlText: string
  targetBackend: string | null; bindParams: string[]
}

export async function getCapture(limit = 1000): Promise<CaptureEntry[]> {
  return getJson(`/api/capture?since=0&limit=${limit}`)
}

// ---- A/B compare ring buffer (all entries, not only differences) ----

export async function getAbCompareAll(limit = 1000): Promise<AbCompareEntry[]> {
  const r = await getJson<{ entries: AbCompareEntry[] }>(`/api/ab-routing/compare?limit=${limit}`)
  return r.entries
}
