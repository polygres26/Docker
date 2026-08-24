// This SPA talks directly to PolyWire's own admin API (see
// wire/src/main/java/com/polygres/wire/http/admin/MetricsServer.java) -- there is no advisor
// backend in between. That server is explicitly documented as designed for server-to-server use
// ("no CORS handling and no session/cookie machinery on purpose"), so the browser has to supply
// its own base URL + bearer token on every request instead of relying on a cookie-backed session.
//
// Both are entered once on the Connect screen and kept in sessionStorage (NOT localStorage) --
// cleared automatically when the tab closes, rather than lingering on disk. A 401 clears the
// stored token and sends the user back to /connect.

const BASE_URL_KEY = 'polywire.adminUrl'
const TOKEN_KEY = 'polywire.adminToken'

export function getStoredConnection(): { baseUrl: string; token: string } | null {
  const baseUrl = sessionStorage.getItem(BASE_URL_KEY)
  const token = sessionStorage.getItem(TOKEN_KEY)
  if (!baseUrl || !token) return null
  return { baseUrl, token }
}

export function storeConnection(baseUrl: string, token: string): void {
  sessionStorage.setItem(BASE_URL_KEY, baseUrl.replace(/\/+$/, ''))
  sessionStorage.setItem(TOKEN_KEY, token)
}

export function clearConnection(): void {
  sessionStorage.removeItem(BASE_URL_KEY)
  sessionStorage.removeItem(TOKEN_KEY)
}

/** Redirect target after a 401 or an explicit disconnect. Kept as one place so it's easy to change. */
const CONNECT_PATH = '/connect'

class UnauthorizedError extends Error {}

async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const conn = getStoredConnection()
  if (!conn) {
    window.location.href = CONNECT_PATH
    throw new UnauthorizedError('not connected')
  }
  const res = await fetch(`${conn.baseUrl}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${conn.token}`,
      ...options.headers,
    },
  })
  if (res.status === 401) {
    clearConnection()
    window.location.href = CONNECT_PATH
    throw new UnauthorizedError('admin token rejected')
  }
  const isJson = res.headers.get('content-type')?.includes('application/json')
  const body = isJson ? await res.json() : null
  if (!res.ok) {
    throw new Error(body?.error ?? `Request failed (HTTP ${res.status})`)
  }
  return body as T
}

/** Connect-screen probe: unlike `api()`, this takes the candidate baseUrl/token as arguments
 * instead of reading them from sessionStorage, since nothing has been stored yet. */
export async function testConnection(baseUrl: string, token: string): Promise<WireMetricsSummary> {
  const res = await fetch(`${baseUrl.replace(/\/+$/, '')}/api/metrics/summary`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  const isJson = res.headers.get('content-type')?.includes('application/json')
  const body = isJson ? await res.json() : null
  if (!res.ok) {
    throw new Error(body?.error ?? `Request failed (HTTP ${res.status})`)
  }
  return body as WireMetricsSummary
}

// --- Firewall rules: /api/firewall-rules ---

export interface FirewallRule {
  id: number
  priority: number
  action: 'allow' | 'deny'
  statementType: string | null
  tablePattern: string | null
  sqlPattern: string | null
  enabled: boolean
  description: string | null
  createdAt: string
}

export async function listFirewallRules(): Promise<FirewallRule[]> {
  return api('/api/firewall-rules')
}

export async function createFirewallRule(rule: {
  priority: number
  action: 'allow' | 'deny'
  statementType?: string
  tablePattern?: string
  sqlPattern?: string
  enabled: boolean
  description?: string
}): Promise<{ id: number }> {
  return api('/api/firewall-rules', { method: 'POST', body: JSON.stringify(rule) })
}

export async function updateFirewallRule(id: number, rule: {
  priority: number
  action: 'allow' | 'deny'
  statementType?: string
  tablePattern?: string
  sqlPattern?: string
  enabled: boolean
  description?: string
}): Promise<void> {
  await api(`/api/firewall-rules/${id}`, { method: 'PUT', body: JSON.stringify(rule) })
}

export async function deleteFirewallRule(id: number): Promise<void> {
  await api(`/api/firewall-rules/${id}`, { method: 'DELETE' })
}

// --- Full config: /api/config ---
// One GET/PUT(-partial) resource over every field of PolyWireConfig -- see
// com.polygres.wire.config.PolyWireConfig and MetricsServer#handleConfig. A PUT only needs to
// carry the fields a page actually edits; everything else is carried forward from the latest
// polywire_config version untouched.

export interface WireConfig {
  qosRatePerSec: string | null
  qosBurst: string | null
  qosMaxWaitMs: string | null
  qosClassLimits: string | null
  qosPoolWaitThreshold: string | null
  cacheTables: string | null
  cacheTtlMs: string | null
  backends: string | null
  shardBackends: string | null
  routerSchemaRules: string | null
  routerPredicateRules: string | null
  routerValueShardRules: string | null
  routerShardTables: string | null
  rollupDefinitionsYaml: string | null
  aclRules: string | null
  aclPpv2Enabled: string | null
  aclTrustedProxies: string | null
  oauthIssuer: string | null
  oauthAudience: string | null
  oauthUserIdClaim: string | null
  oauthRolesClaim: string | null
  awsIamCredentials: string | null
}

export async function getWireConfig(): Promise<WireConfig> {
  return api('/api/config')
}

export async function saveWireConfig(partial: Partial<WireConfig>): Promise<{ ok: boolean; version: number }> {
  return api('/api/config', { method: 'PUT', body: JSON.stringify(partial) })
}

// --- Live metrics: /api/metrics/summary ---

export interface WireMetricsSql {
  sql: string
  calls: number
  totalMs: number
  avgMs: number
  avgRttMs: number | null
}

export interface WireMetricsBackend {
  backend: string
  calls: number
  reads: number
  writes: number
  totalMs: number
  avgMs: number
}

export interface WireMcpToolStat {
  tool: string
  calls: number
  errors: number
  totalMs: number
  avgMs: number
}

/** One row of the cache-hit vs. real-Postgres-read vs. real-Postgres-write timing breakdown --
 * `outcome` is 'cache_hit' | 'pg_read' | 'pg_write'. Only present for protocols/outcomes that
 * have actually happened at least once since the process started. */
export interface WireRttOutcomeStat {
  protocol: string
  outcome: string
  calls: number
  totalMs: number
  avgMs: number
}

export interface WireMetricsSummary {
  protocolCounts: Record<string, number>
  totalReads: number
  totalWrites: number
  totalOther: number
  readsPerSec: number
  writesPerSec: number
  avgRttMs: number | null
  rttSamples: number
  topSql: WireMetricsSql[]
  byBackend: WireMetricsBackend[]
  mcpTools: WireMcpToolStat[]
  rttByOutcome: WireRttOutcomeStat[]
}

export async function getWireMetrics(): Promise<WireMetricsSummary> {
  return api('/api/metrics/summary')
}

// --- Backends + data explorer: /api/backends/... ---

export interface BackendInfo {
  name: string
  jdbcUrl: string
  dialect: string | null
}

export interface TableInfo {
  schema: string
  name: string
  type: string
}

export interface ColumnInfo {
  name: string
  type: string
  nullable: boolean
}

export interface QueryResult {
  columns: string[]
  rows: unknown[][]
  rowCount: number
  truncated: boolean
  tookMs: number
}

export async function listBackends(): Promise<BackendInfo[]> {
  return api('/api/backends')
}

export async function listBackendTables(backend: string): Promise<TableInfo[]> {
  return api(`/api/backends/${encodeURIComponent(backend)}/tables`)
}

export async function listBackendColumns(backend: string, schema: string, table: string): Promise<ColumnInfo[]> {
  return api(`/api/backends/${encodeURIComponent(backend)}/tables/${encodeURIComponent(schema)}/${encodeURIComponent(table)}/columns`)
}

export async function runBackendQuery(backend: string, sql: string): Promise<QueryResult> {
  return api(`/api/backends/${encodeURIComponent(backend)}/query`, { method: 'POST', body: JSON.stringify({ sql }) })
}

export interface BackendTestResult {
  ok: boolean
  message: string
  tookMs: number
  serverVersion: string | null
}

export async function testBackendConnection(params: { jdbcUrl: string; user: string; password: string }): Promise<BackendTestResult> {
  return api('/api/backends/test', { method: 'POST', body: JSON.stringify(params) })
}

export async function testConfiguredBackend(name: string): Promise<BackendTestResult> {
  return api(`/api/backends/${encodeURIComponent(name)}/test`, { method: 'POST' })
}

// --- sqswire queues: /api/queues ---

export interface QueueInfo {
  name: string
  visible: number
  inFlight: number
  fifo: boolean
  visibilityTimeout: number
  dlqQueueName: string | null
  maxReceiveCount: number | null
  backend: string
}

export async function listQueues(): Promise<QueueInfo[]> {
  return api('/api/queues')
}

export async function deleteQueue(name: string): Promise<void> {
  await api(`/api/queues/${encodeURIComponent(name)}`, { method: 'DELETE' })
}

// --- LLM (SQL-dialect-translation) fallback configuration: /api/llm-config ---
// New admin endpoint, built concurrently by a separate agent -- not yet visible in
// MetricsServer.java at the time this client was written. Contract per the spec this page was
// built against: GET returns {provider, baseUrl, model, apiKeySet}; PUT accepts
// {provider, apiKey?, baseUrl, model} where omitting apiKey leaves the stored key unchanged.

export type LlmProvider = 'openai' | 'custom' | 'none'

export interface LlmConfigStatus {
  provider: LlmProvider
  baseUrl: string | null
  model: string | null
  apiKeySet: boolean
}

export async function getLlmConfig(): Promise<LlmConfigStatus> {
  return api('/api/llm-config')
}

export async function saveLlmConfig(cfg: {
  provider: LlmProvider
  apiKey?: string
  baseUrl: string | null
  model: string | null
}): Promise<LlmConfigStatus> {
  return api('/api/llm-config', { method: 'PUT', body: JSON.stringify(cfg) })
}

// --- Node topology / heartbeats: /api/nodes ---
// New admin endpoint, built concurrently by a separate agent -- not yet visible in
// MetricsServer.java at the time this client was written. Contract per the spec this page was
// built against: each PolyWire instance heartbeats its identity to the shared config Postgres
// every ~10s; a node is "stale" if it hasn't heartbeated in 30s.

export interface NodeInfo {
  nodeId: string
  host: string
  adminPort: number
  zone: string | null
  version: string
  startedAt: string
  lastHeartbeat: string
  status: 'up' | 'stale'
}

export async function listNodes(): Promise<NodeInfo[]> {
  return api('/api/nodes')
}
