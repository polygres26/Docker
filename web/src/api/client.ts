export interface ScanRequest {
  name?: string
  jdbcUrl: string
  user: string
  password: string
}

export interface ScoreFinding {
  feature: string
  count: number
  weightPerUnit: number
  points: number
  note: string
}

export interface MigrationScoreReport {
  sourceVersion: string | null
  totalScore: number
  tier: string
  findings: ScoreFinding[]
  warnings: string[]
}

export interface CatalogSnapshot {
  dialect: string
  sourceVersion: string | null
  versionWarning: string | null
  tableCount: number
  viewCount: number
  materializedViewCount: number
  sequenceCount: number
  simpleTriggerCount: number
  complexTriggerCount: number
  packageCount: number
  standaloneProcedureCount: number
  standaloneFunctionCount: number
  dbLinkCount: number
  scheduledJobCount: number
  synonymCount: number
  partitionedTableCount: number
  builtinPackageUsage: Record<string, number>
  syntaxConstructUsage: Record<string, number>
  warnings: string[]
}

export interface ScanResult {
  snapshot: CatalogSnapshot
  score: MigrationScoreReport
}

async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(path, {
    ...options,
    headers: { 'Content-Type': 'application/json', ...options.headers },
  })
  const isJson = res.headers.get('content-type')?.includes('application/json')
  const body = isJson ? await res.json() : null
  if (!res.ok) {
    throw new Error(body?.error ?? `Request failed (HTTP ${res.status})`)
  }
  return body as T
}

export async function runScan(req: ScanRequest): Promise<ScanResult> {
  return api<ScanResult>('/api/scan', { method: 'POST', body: JSON.stringify(req) })
}

// --- Auth ---

export async function login(username: string, password: string): Promise<void> {
  await api('/api/login', { method: 'POST', body: JSON.stringify({ username, password }) })
}

export async function logout(): Promise<void> {
  await api('/api/logout', { method: 'POST' })
}

export async function checkSession(): Promise<boolean> {
  const res = await api<{ authenticated: boolean }>('/api/session')
  return res.authenticated
}

// --- Connections ---

export interface Connection {
  id: string
  name: string
  jdbcUrl: string
  user: string
  createdAt: string
}

export async function listConnections(): Promise<Connection[]> {
  return api<Connection[]>('/api/connections')
}

export async function createConnection(c: { name: string; jdbcUrl: string; user: string; password: string }): Promise<Connection> {
  return api<Connection>('/api/connections', { method: 'POST', body: JSON.stringify(c) })
}

export async function deleteConnection(id: string): Promise<void> {
  await fetch(`/api/connections/${id}`, { method: 'DELETE' })
}

export async function getObjects(id: string): Promise<Record<string, string[]>> {
  return api(`/api/connections/${id}/objects`)
}

export async function getObjectDetail(id: string, type: string, name: string): Promise<{ columns?: unknown[]; source?: string }> {
  return api(`/api/connections/${id}/objects/detail?type=${encodeURIComponent(type)}&name=${encodeURIComponent(name)}`)
}

export interface ParameterInfo {
  name: string
  value: string
  defaultValue: string | null
  isDefault: boolean
  description: string
}

export async function getParameters(id: string): Promise<ParameterInfo[]> {
  return api(`/api/connections/${id}/parameters`)
}

export async function runConnectionScan(id: string): Promise<ScanResult> {
  return api(`/api/connections/${id}/scan`, { method: 'POST' })
}
