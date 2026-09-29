// This SPA talks directly to Warp's own admin API (see
// Warp/src/main/java/com/sayonora/warp/http/admin/MetricsServer.java) -- there is no advisor
// backend in between. That server is explicitly documented as designed for server-to-server use
// ("no CORS handling and no session/cookie machinery on purpose"), so the browser has to supply
// its own base URL + bearer token on every request instead of relying on a cookie-backed session.
//
// Both are entered once on the Connect screen and kept in sessionStorage (NOT localStorage) --
// cleared automatically when the tab closes, rather than lingering on disk. A 401 clears the
// stored token and sends the user back to /connect.

const BASE_URL_KEY = 'warp.adminUrl'
const TOKEN_KEY = 'warp.adminToken'
const REMEMBER_KEY = 'warp.remember'
const TIMEOUT_KEY = 'warp.requestTimeoutMs'

const DEFAULT_TIMEOUT_MS = 10_000

// "Remember on this device" (advanced, off by default) trades the sessionStorage default -- gone
// the moment the tab closes -- for localStorage, so the token survives a reload/restart. Whichever
// store was actually used to save it is also where every later read/clear looks, so a stored
// connection is never split across the two.
function storageFor(remember: boolean): Storage {
  return remember ? localStorage : sessionStorage
}

export function getStoredConnection(): { baseUrl: string; token: string } | null {
  const store = localStorage.getItem(TOKEN_KEY) !== null ? localStorage : sessionStorage
  const baseUrl = store.getItem(BASE_URL_KEY)
  const token = store.getItem(TOKEN_KEY)
  if (!baseUrl || !token) return null
  return { baseUrl, token }
}

export function storeConnection(baseUrl: string, token: string, remember = false): void {
  const store = storageFor(remember)
  store.setItem(BASE_URL_KEY, baseUrl.replace(/\/+$/, ''))
  store.setItem(TOKEN_KEY, token)
  localStorage.setItem(REMEMBER_KEY, String(remember))
}

export function clearConnection(): void {
  sessionStorage.removeItem(BASE_URL_KEY)
  sessionStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(BASE_URL_KEY)
  localStorage.removeItem(TOKEN_KEY)
}

export function getRememberPreference(): boolean {
  return localStorage.getItem(REMEMBER_KEY) === 'true'
}

/** Request timeout (advanced, defaults to 10s): every `fetch()` below is wrapped in an
 * AbortController on this timer so a hung admin process fails fast with a clear message instead
 * of leaving the UI stuck on "Connecting…"/a spinner forever. Persisted alongside the connection
 * (localStorage, not session-scoped -- a slow-network preference isn't a secret). */
export function getRequestTimeoutMs(): number {
  const raw = Number(localStorage.getItem(TIMEOUT_KEY))
  return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_TIMEOUT_MS
}

export function setRequestTimeoutMs(ms: number): void {
  localStorage.setItem(TIMEOUT_KEY, String(ms))
}

async function fetchWithTimeout(input: string, options: RequestInit): Promise<Response> {
  const timeoutMs = getRequestTimeoutMs()
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await fetch(input, { ...options, signal: controller.signal })
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs}ms (Advanced options → Request timeout)`)
    }
    throw e
  } finally {
    clearTimeout(timer)
  }
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
  const res = await fetchWithTimeout(`${conn.baseUrl}${path}`, {
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
  const res = await fetchWithTimeout(`${baseUrl.replace(/\/+$/, '')}/api/metrics/summary`, {
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

/** Draft-only: never inserts a rule. Turns a plain-English prompt into a proposed
 * `/api/firewall-rules` POST body (see `MetricsServer#handleFirewallRuleDraft`) for the caller to
 * review/edit and then submit itself via `createFirewallRule`. */
export async function draftFirewallRule(prompt: string): Promise<{
  draft: {
    priority: number
    action: 'allow' | 'deny'
    statementType: string | null
    tablePattern: string | null
    sqlPattern: string | null
    enabled: boolean
    description: string | null
  }
  applied: false
  note: string
}> {
  return api('/api/firewall-rules/draft', { method: 'POST', body: JSON.stringify({ prompt }) })
}

// --- Full config: /api/config ---
// One GET/PUT(-partial) resource over every field of WarpConfig -- see
// com.sayonora.warp.config.WarpConfig and MetricsServer#handleConfig. A PUT only needs to
// carry the fields a page actually edits; everything else is carried forward from the latest
// warp_config version untouched.

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
  backendSets: string | null
  routerSchemaRules: string | null
  routerPredicateRules: string | null
  routerValueShardRules: string | null
  routerShardTables: string | null
  routerTableShards: string | null
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

/** Draft-only: never writes to `warp_config`. Proposes ONE targeted rate-limit change based on
 * current config and recent per-backend load (see `MetricsServer#handleQosSuggestionDraft`); the
 * `*IfApplied` field(s) are exactly what `saveWireConfig` needs to actually apply it. */
export async function draftQosSuggestion(): Promise<{
  draft: { target: string; ratePerSecond: number; burstCapacity: number; maxWaitMillis: number; rationale: string | null }
  qosRatePerSecIfApplied?: string
  qosBurstIfApplied?: string
  qosMaxWaitMsIfApplied?: string
  qosClassLimitsIfApplied?: string
  applied: false
  note: string
}> {
  return api('/api/qos-suggestions/draft', { method: 'POST' })
}

/** Draft-only: never writes to `warp_config`. Proposes ONE new per-table hash-sharding rule based
 * on real per-backend load (see `MetricsServer#handleRouterSuggestionDraft`); when the LLM found
 * nothing worth sharding, `draft`/`routerTableShardsIfApplied` come back null and `note` explains
 * why. `routerTableShardsIfApplied` is the FULL candidate spec (existing rules plus the new one)
 * -- exactly what `saveWireConfig({ routerTableShards: ... })` needs to actually apply it. */
export async function draftRouterSuggestion(): Promise<{
  draft: { table: string; shardColumn: string; backends: string[] } | null
  routerTableShardsIfApplied?: string
  applied?: false
  note: string
}> {
  return api('/api/router-suggestions/draft', { method: 'POST' })
}

/** Draft-only: never writes to `warp_config`. Proposes ONE new RollupStage pre-aggregation
 * definition based on recent expensive/frequent SQL (see `MetricsServer#handleRollupSuggestionDraft`);
 * when the LLM found nothing worth pre-aggregating, `draft`/`rollupDefinitionsYamlIfApplied` come
 * back null and `note` explains why. `rollupDefinitionsYamlIfApplied` is the FULL candidate YAML
 * (existing definitions plus the new one) -- exactly what
 * `saveWireConfig({ rollupDefinitionsYaml: ... })` needs to actually apply it. */
export async function draftRollupSuggestion(): Promise<{
  draft: {
    name: string
    backend?: string
    sourceTable: string
    groupBy: string[]
    aggregations: string[]
    refreshIntervalMinutes: number
    maxStalenessMinutes: number
  } | null
  rollupDefinitionsYamlIfApplied?: string
  applied?: false
  note: string
}> {
  return api('/api/rollup-suggestions/draft', { method: 'POST' })
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
  type?: string
  backendSet?: string | null
  enabledStores?: string[]
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

// --- Backend sets: /api/backend-sets (the one place backends are added, edited and removed) ---

/** A store id as served by GET /api/backend-stores (never hardcode the list; read `stores` from the API). */
export type StoreId = string

export interface StoreInfo {
  id: StoreId
  label: string
  description: string
  shardable: boolean
  setEnvVar: string
  /** The set this protocol's frontend currently serves from (null: nothing hosts it anywhere yet). */
  servedSet: string | null
  /** The admin-persisted serving-set assignment (settable via PATCH /api/backend-stores/{id}),
   * or null when none is set -- falls back to the setEnvVar / the default set. */
  frontendSetOverride: string | null
}

export interface SetBackend {
  name: string
  set: string
  type: string
  family: string
  dialect: string | null
  url: string
  user: string | null
  description: string | null
  fallback: string | null
  isDefault: boolean
  /** The exact database / service name a client connects with to reach ONLY this backend. */
  connectAs: string
  enabledStores: StoreId[]
  canHostStores: boolean
  state: string
  /** Connection-pool gauge for this backend; absent until the first connection was borrowed. */
  pool?: { active: number; idle: number; total: number; max: number; waiting: number }
  health?: { ok: boolean; message: string; tookMs: number; serverVersion: string | null }
}

export interface SetStoreHosting {
  hosts: string[]
  sharded: boolean
  servedFromThisSet: boolean
  frontendSetEnvVar: string
  /** The admin-persisted serving-set assignment for this store (any set, not just this one), or
   * null when none is set. Settable via `setStoreFrontendSet`. */
  frontendSetOverride: string | null
}

export interface BackendSetInfo {
  name: string
  description: string | null
  isDefaultSet: boolean
  /** The database / service name that selects this whole set; null when a backend of the same name shadows it. */
  connectAs: string | null
  backends: SetBackend[]
  stores: Partial<Record<StoreId, SetStoreHosting>>
}

export interface ConnectionRoute {
  id: string
  protocol?: string
  database: string
  user?: string
  target: string
  /** Real resolution of `target` (ConnectionRouter#describeTarget -- the SAME code path a live
   * connection uses), not a client-side guess: 'unknown' means a real connection through this
   * route is rejected outright, fail-closed, exactly as ConnectionRouter#resolve treats it. */
  targetKind: 'backend' | 'set' | 'unknown'
  /** The real backend/set name `target` resolves to (case/prefix-normalized), null when unknown. */
  resolvedName: string | null
  /** The real backend(s) this route reaches: one name for a backend target, the set's members for
   * a set target, empty for 'unknown'. */
  resolvedHosts: string[]
  defaultBackend?: string
}

export interface ConnectionRouting {
  /** implicit (default) | strict | off -- WARP_CONNECT_ROUTING */
  mode: 'implicit' | 'strict' | 'off'
  routes: ConnectionRoute[]
}

export type ConnectionRouteDraft = Pick<ConnectionRoute, 'database' | 'target'>
  & Partial<Pick<ConnectionRoute, 'protocol' | 'user' | 'defaultBackend'>>

export interface BackendSetsResponse {
  connectionRouting: ConnectionRouting
  sets: BackendSetInfo[]
  maxBackends: number
  backendCount: number
  stores: StoreInfo[]
}

export interface RebalanceNotice { store: StoreId; before: string[]; after: string[]; message: string }

export interface BackendWriteResult {
  ok: boolean
  version: number
  rebalanceRequired: RebalanceNotice[]
  warnings: string[]
}

export interface BackendDraft {
  name: string
  url: string
  user: string
  password: string
  description: string
  enabledStores: StoreId[]
}

const setPath = (set: string) => `/api/backend-sets/${encodeURIComponent(set)}`
const backendPath = (set: string, name: string) => `${setPath(set)}/backends/${encodeURIComponent(name)}`

export async function listBackendSets(health = false): Promise<BackendSetsResponse> {
  return api(`/api/backend-sets${health ? '?health=true' : ''}`)
}

export async function createBackendSet(name: string, description: string): Promise<BackendWriteResult> {
  return api('/api/backend-sets', { method: 'POST', body: JSON.stringify({ name, description: description || null }) })
}

/** GET /api/backend-stores: the stores a Postgres backend can host (single source of truth for the UI). */
export async function listBackendStores(): Promise<StoreInfo[]> {
  const r = await api<{ stores: StoreInfo[] }>('/api/backend-stores')
  return r.stores
}

/** Sets (or, with `set: null`, clears) which backend set a store's frontend serves -- the UI
 * alternative to hand-setting that protocol's WARP_<PROTO>WIRE_SET env var. */
export async function setStoreFrontendSet(storeId: StoreId, set: string | null): Promise<{ ok: boolean; version: number; servedSet: string | null }> {
  return api(`/api/backend-stores/${encodeURIComponent(storeId)}`, { method: 'PATCH', body: JSON.stringify({ set }) })
}

/** Rename and/or re-describe a set. `name` renames it (its backends follow). */
export async function updateBackendSet(set: string, patch: { name?: string; description?: string | null }): Promise<BackendWriteResult> {
  return api(setPath(set), { method: 'PATCH', body: JSON.stringify(patch) })
}

/** Move a backend to another existing set (its data is not moved: see rebalanceRequired). */
export async function moveSetBackend(set: string, name: string, toSet: string): Promise<BackendWriteResult> {
  return api(backendPath(set, name), { method: 'PATCH', body: JSON.stringify({ set: toSet }) })
}

export async function deleteBackendSet(set: string): Promise<BackendWriteResult> {
  return api(setPath(set), { method: 'DELETE' })
}

export async function addBackendToSet(set: string, draft: BackendDraft): Promise<BackendWriteResult> {
  return api(`${setPath(set)}/backends`, { method: 'POST', body: JSON.stringify(draft) })
}

/** Blank `password` keeps the stored one (the API never returns it). */
export async function updateSetBackend(set: string, name: string, draft: Partial<Omit<BackendDraft, 'name'>>): Promise<BackendWriteResult> {
  return api(backendPath(set, name), { method: 'PATCH', body: JSON.stringify(draft) })
}

export async function deleteSetBackend(set: string, name: string): Promise<BackendWriteResult> {
  return api(backendPath(set, name), { method: 'DELETE' })
}

export async function addConnectionRoute(draft: ConnectionRouteDraft): Promise<ConnectionRoute> {
  return api('/api/connection-routes', { method: 'POST', body: JSON.stringify(draft) })
}

export async function deleteConnectionRoute(id: string): Promise<{ ok: boolean; version: number }> {
  return api(`/api/connection-routes/${encodeURIComponent(id)}`, { method: 'DELETE' })
}

export async function testSetBackend(set: string, name: string): Promise<BackendTestResult> {
  return api(`${backendPath(set, name)}/test`, { method: 'POST' })
}

// --- Federation plan history: /api/federation/plans (ShardJoinExecutor/SchemaFederationStage's
// own real, captured Calcite EXPLAIN PLAN FOR history -- see MetricsServer's own javadoc on the
// route). 404s (not an error to surface as one) when WARP_FEDERATION_PLAN_HISTORY isn't set --
// the page itself renders the "not enabled" explanation for that case, same as Queues does for
// sqswire not being configured. ---

// A real, MEASURED (not estimated) leaf table scan -- see LeafScanProfiler's own javadoc.
// Calcite's own EXPLAIN PLAN FOR only ever reports the planner's pre-execution row-count
// ESTIMATE per node, never an actual post-execution measurement -- this is Warp's own
// answer to that gap: a genuinely separate re-execution of just this one leaf's own
// pushed-down SQL against its own real backend, with real wall-clock timing and a real row
// count from actually iterating the result.
export interface FederationLeafScan {
  backend: string
  sqlText: string
  elapsedMillis: number
  rowCount: number
  errorMessage: string | null
}

export interface FederationPlanEntry {
  planId: number
  capturedAt: string
  backends: string
  sqlText: string
  planText: string | null
  elapsedMillis: number
  rowCount: number
  success: boolean
  errorMessage: string | null
  leafScans: FederationLeafScan[]
}

export class FederationPlansNotEnabledError extends Error {}

export async function listFederationPlans(): Promise<FederationPlanEntry[]> {
  try {
    return await api('/api/federation/plans')
  } catch (e) {
    // api() surfaces a non-2xx response as Error(body.error) when the server sent a JSON error
    // body (see api()'s own implementation below) -- MetricsServer's own 404 for this route
    // always carries exactly this message (its own literal string, matched here verbatim) when
    // WARP_FEDERATION_PLAN_HISTORY isn't set, which means "the route doesn't exist because
    // this feature isn't configured," not a real failure.
    if (e instanceof Error && e.message.includes('federation plan history is not enabled')) {
      throw new FederationPlansNotEnabledError(e.message)
    }
    throw e
  }
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
// built against: each Warp instance heartbeats its identity to the shared config Postgres
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

// ---- A/B routing (com.sayonora.warp.ab): real cloud vs local emulation, per store ----------------------------

export interface AbPolicy {
  store: string
  mode: 'local' | 'cloud' | 'split' | 'compare'
  target: string | null
  cloudPercent: number
  stickyBy: string
  writeOwner: 'local' | 'cloud'
  dualWrite: boolean
  rules: Array<{ name: string; accessKey: string | null; ip: string | null; header: string | null; headerValue: string | null; route: string; pinWrites: boolean }>
  compare: { primary: 'local' | 'cloud'; bufferSize: number; recordValues: boolean }
  roleOverrides: Record<string, string>
}

export interface AbKill { side: 'local' | 'cloud'; reason: string | null; by: string | null; at: number }

export interface AbState {
  version: number
  policies: Record<string, AbPolicy>
  /** Secrets are never returned: only `<name>Set: true` flags inside `auth`. */
  targets: Array<{ name: string; region: string; auth: { type: string } & Record<string, unknown> }>
  killSwitch: AbKill | null
  storeKill: Record<string, AbKill>
  persistent: boolean
  secretsEncryptedAtRest: boolean
}

export interface AbCompareEntry {
  ts: number; store: string; op: string; client: string; primary: string
  localStatus: number; cloudStatus: number; localMs: number; cloudMs: number; equal: boolean; diffs: string[]
}

export interface AbStats {
  sides: Record<string, { requests: number; errors: number; errorRate: number; avgMs: number }>
  compare: Record<string, { compared: number; equal: number; differ: number; secondaryFailed: number; dualWriteOk: number; dualWriteFailed: number }>
}

export async function getAbRouting(): Promise<AbState> {
  return api<AbState>('/api/ab-routing')
}

export async function putAbPolicy(store: string, policy: Partial<AbPolicy>): Promise<AbState> {
  return api<AbState>(`/api/ab-routing/policies/${encodeURIComponent(store)}`, { method: 'PUT', body: JSON.stringify(policy) })
}

export async function deleteAbPolicy(store: string): Promise<AbState> {
  return api<AbState>(`/api/ab-routing/policies/${encodeURIComponent(store)}`, { method: 'DELETE' })
}

export async function setAbKillSwitch(side: 'local' | 'cloud', reason?: string): Promise<AbState> {
  return api<AbState>('/api/ab-routing/kill-switch', { method: 'POST', body: JSON.stringify({ side, reason }) })
}

export async function clearAbKillSwitch(): Promise<AbState> {
  return api<AbState>('/api/ab-routing/kill-switch', { method: 'DELETE' })
}

export async function getAbCompare(onlyDiff: boolean): Promise<{ entries: AbCompareEntry[] }> {
  return api<{ entries: AbCompareEntry[] }>(`/api/ab-routing/compare?limit=100${onlyDiff ? '&onlyDiff=true' : ''}`)
}

export async function getAbStats(): Promise<AbStats> {
  return api<AbStats>('/api/ab-routing/stats')
}


// --- Interfaces: /api/interfaces (every frontend this Warp is actually serving) ---

export type InterfaceKind = 'sql' | 'api' | 'mcp'
export type InterfaceMode = 'Relay' | 'Bridge' | 'Emulate'

export interface InterfaceInfo {
  id: string
  label: string
  kind: InterfaceKind
  protocol: string
  port: number
  /** Relay: native protocol to a same-engine backend. Bridge: real protocol parsed, SQL run verbatim against a pooled same-engine backend (firewall/QoS/audit still apply, no dialect translation). Emulate: dialect/API translated and executed on Postgres. null: n/a. */
  mode: InterfaceMode | null
  /** 'listening_no_store': the socket is up, but this store-backed frontend's store isn't enabled
   * on any backend, so it's silently running on the legacy implicit-default-backend fallback --
   * not what "store not enabled" in the Serves column implies at a glance. Render as a warning,
   * not the same plain "Listening" pill as a fully-configured frontend. */
  status: 'listening' | 'listening_no_store'
  /** Store id for store-backed API frontends. */
  store: string | null
  /** Backend set this store-backed frontend is served from (WARP_<PROTOCOL>_SET), and the backends hosting it. */
  set?: string | null
  hosts?: string[]
  setEnvVar?: string
  /** Statements/operations counted since process start; null when the collector keeps no counter for this frontend. */
  requests: number | null
  metricsKey: string | null
  /** Set for HTTP-style listeners (MCP, A2A and the API frontends): is a native HTTPS port served next to the plaintext one. */
  tlsEnabled?: boolean
  httpsPort?: number | null
  selfSigned?: boolean
  /** Protocol frontends (gRPC / raw TCP): off | in-band | sniff-allow | sniff-require | separate-port, and the TLS port. */
  tlsMode?: 'off' | 'in-band' | 'sniff-allow' | 'sniff-require' | 'separate-port'
  tlsPort?: number
  tlsClientAuth?: boolean
  /** Why HTTPS is off although TLS was configured (bad certificate, missing file, ...). */
  tlsError?: string
  /** Joined from GET /api/access-summary (AccessSummary#frontendAuth) -- absent means this
   * frontend has no auth summary reported at all (never guessed; render as "not reported"). */
  authMethod?: string
  authEnforced?: boolean
  authDetail?: string
  /** Joined from PolicySummary#applicablePolicies -- which control-plane policies apply to this
   * interface, as short tags: "router", "qos", "firewall:N", "acl:N" (N = rule count). Router/QoS/
   * firewall only ever appear for kind === 'sql' (they never apply outside the SQL pipeline); "acl:N"
   * always appears (even "acl:0") so "explicitly no ACL rules" reads differently from "not computed".
   * Absent entirely means the server had no WarpConfig to compute this from (never guessed). */
  policies?: string[]
}

/** Whether `tag` (e.g. "router", "qos") is present verbatim in an interface's `policies` array. */
export function policyOn(policies: string[] | undefined, tag: string): boolean {
  return !!policies?.includes(tag)
}

/** The count from a `"prefix:N"` policy tag (e.g. `policyCount(i.policies, 'acl')` for `"acl:3"`),
 * or 0 when the tag isn't present at all. */
export function policyCount(policies: string[] | undefined, prefix: string): number {
  const tag = policies?.find((p) => p.startsWith(`${prefix}:`))
  return tag ? Number(tag.slice(prefix.length + 1)) : 0
}

export interface InterfacesResponse { interfaces: InterfaceInfo[]; activeSessions: number }

export async function listInterfaces(): Promise<InterfacesResponse> {
  return api('/api/interfaces')
}

// --- MCP endpoints: /api/mcp-endpoints ---

export interface McpEndpoint {
  id: string
  name: string
  /** "all", "group:<backend set>" or "db:<backend>" */
  scope: string
  description: string | null
  createdAt: string | null
  createdBy: string | null
  expiresAt: string | null
  status: 'active' | 'expired'
  path: string
  /** Opt-in: the token may also be given as /e/<id>/t/<token> (no Authorization header). */
  urlToken?: boolean
}

export interface McpEndpointCreated extends McpEndpoint {
  token: string; mcpPort: number; note: string
  /** Best connection URL (https / WARP_MCP_PUBLIC_URL when available), without the token. */
  url?: string
  /** Only when the endpoint was created with urlToken: the header-less connector URL, token included (shown once). */
  urlWithToken?: string
  mcpHttpsPort?: number | null
}

/** GET /api/mcp-config: how a user should reach the MCP listener. */
export interface McpConfig {
  publicUrl: string | null
  tlsEnabled: boolean
  selfSigned: boolean
  httpsPort: number | null
  httpPort: number | null
  tlsError: string | null
  certSubject: string | null
  certNotAfter: string | null
  /** scheme://host[:port] to put in front of the endpoint path. */
  baseUrl: string
  https: boolean
  /** https:// and not a self-signed certificate: what a Claude custom connector requires. */
  claudeConnectorReady: boolean
  /** Built-in ACME (Let's Encrypt) is enabled and manages this listener's certificate. */
  acmeEnabled: boolean
  /** ACME is enabled but has not issued a real certificate yet (still serving the temporary self-signed placeholder). */
  acmePending: boolean
  acmeError: string | null
}

export async function getMcpConfig(): Promise<McpConfig> {
  return api('/api/mcp-config')
}

export async function setMcpEndpointUrlToken(id: string, enabled: boolean): Promise<McpEndpoint> {
  return api(`/api/mcp-endpoints/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify({ urlToken: enabled }) })
}

export interface McpTool { name: string; description: string; inputSchema?: unknown }

export async function listMcpEndpoints(): Promise<McpEndpoint[]> {
  return api('/api/mcp-endpoints')
}

export async function createMcpEndpoint(body: {
  name: string; scope: string; description?: string; ttlSeconds?: number; urlToken?: boolean
}): Promise<McpEndpointCreated> {
  return api('/api/mcp-endpoints', { method: 'POST', body: JSON.stringify(body) })
}

export async function revokeMcpEndpoint(id: string): Promise<void> {
  await api(`/api/mcp-endpoints/${encodeURIComponent(id)}`, { method: 'DELETE' })
}

export async function getMcpEndpointTools(id: string): Promise<{ id: string; scope: string; tools: McpTool[] }> {
  return api(`/api/mcp-endpoints/${encodeURIComponent(id)}/tools`)
}

// --- Anomalies + usage + config version ---

export interface AnomalyNote {
  timestamp: string; protocol: string; baselinePerSec: number; currentPerSec: number; ratio: number; narrative: string | null
}

export async function getAnomalies(): Promise<{ enabled: boolean; notes: AnomalyNote[] }> {
  return api('/api/anomalies')
}

// --- Observability: /api/observability (real OTLP exporter config + the always-on Prometheus
// scrape endpoint + the real metric catalog -- see ObservabilitySummary.java's own javadoc for
// why this never constructs a second OTLP SDK exporter just by being polled) ---

export interface ObservabilityOtlpStatus {
  enabled: boolean
  protocol: 'grpc' | 'http' | null
  endpoint: string | null
  exportIntervalMs: number | null
  headerCount: number
  /** Cumulative export attempts/successes/failures since process start, from WarpTelemetry's real
   * ExportHealthTrackingExporter (tracks each export's actual async CompletableResultCode
   * outcome) -- all zero when OTLP is disabled or this admin process hasn't constructed the live
   * exporter (e.g. a unit test). */
  exportAttempts: number
  exportSuccesses: number
  exportFailures: number
  /** ISO timestamp of the most recent export attempt (success or failure), or null if none yet. */
  lastExportAt: string | null
  /** ISO timestamp of the most recent SUCCESSFUL export, or null if none has ever succeeded. */
  lastSuccessAt: string | null
  /** The most recent failure's reason, cleared on the next success. */
  lastError: string | null
  /** A real signal now: true only when at least one export has succeeded AND the most recent
   * success is within the last 3 export intervals -- a destination that accepted data once but has
   * since gone quiet reports false again, not a stale permanent "yes". */
  exportVerified: boolean
  /** True while an admin has paused export via PATCH /api/observability (see adminOverride). A
   * paused export never reaches the real delegate -- no attempt is even recorded -- and verified
   * decays to false the same way a genuinely unreachable collector would. */
  pausedByAdmin: boolean
  /** The raw admin override value (true/false), or null if no admin has set one -- distinct from
   * `enabled`, which reflects WARP_OTEL_ENDPOINT at process boot. */
  adminOverride: boolean | null
  /** False when there's no live exporter for an admin override to act on (OTLP was never
   * configured via WARP_OTEL_ENDPOINT at boot) -- an admin can still SET the override (it's
   * remembered for the next restart), but it has no effect on this running process. */
  adminOverrideHasEffect: boolean
  /** The OTLP resource attributes actually attached to every export from this process (service
   * name/version, host name, zone) -- null when OTLP isn't live. Previously no resource attributes
   * were attached at all, so a multi-node deployment's metrics were indistinguishable on the
   * receiving end; this is the real identity now sent. Keys match the OTLP resource attribute
   * names verbatim (e.g. `"service.name"`, `"host.name"`, `"warp.zone"`). */
  resource: Record<string, string> | null
}

export interface ObservabilityMetric {
  name: string
  type: 'counter' | 'gauge' | 'histogram'
  description: string
  labels: string[]
}

export interface ObservabilityPrometheusStatus {
  /** Whether GET /metrics currently renders -- a real, unconditional live toggle (no network
   * client to construct), so an admin's PATCH takes effect on the very next scrape. */
  available: boolean
  /** The raw admin override value (true/false), or null if no admin has set one. */
  adminOverride: boolean | null
  path: string
}

export interface ObservabilityStatus {
  otlp: ObservabilityOtlpStatus
  /** Always true today: WarpTelemetry only registers metric instruments, no trace or log exporter
   * exists anywhere in this codebase. */
  metricsOnly: boolean
  prometheus: ObservabilityPrometheusStatus
  catalog: ObservabilityMetric[]
}

export async function getObservability(): Promise<ObservabilityStatus> {
  return api('/api/observability')
}

/** PATCH /api/observability: admin-settable, warp_config-persisted enable/disable for the two
 * observability destinations (ObservabilityApi.java). A field OMITTED from `toggles` leaves that
 * toggle unchanged; a field explicitly set to `null` CLEARS the override entirely (back to "no
 * admin opinion, defer to the env-var-derived default") -- `true`/`false` sets it. Returns the
 * freshly re-read ObservabilityStatus so the caller doesn't need a separate reload to reflect the
 * change. */
export async function setObservabilityToggles(
  toggles: { otlpEnabled?: boolean | null; prometheusEnabled?: boolean | null },
): Promise<{ ok: boolean; version: number; observability: ObservabilityStatus }> {
  return api('/api/observability', { method: 'PATCH', body: JSON.stringify(toggles) })
}

export interface UsageStat { calls: number; errors: number; totalMs: number; avgMs: number }

export async function getUsage(): Promise<{
  byWorkloadClass: Array<UsageStat & { workloadClass: string }>
  byTenant: Array<UsageStat & { tenant: string }>
}> {
  return api('/api/usage')
}

/** GET /config: the current warp_config version (and its creation time). */
export async function getConfigVersion(): Promise<{ configStoreEnabled: boolean; version: number | null; createdAt?: string }> {
  return api('/config')
}

// --- TLS / built-in ACME (Let's Encrypt): /api/tls/certificates, /api/tls/renew ---

export interface AcmeStatus {
  enabled: boolean
  reason?: string
  domains?: string[]
  directory?: string
  staging?: boolean
  challenge?: 'http-01' | 'dns-01'
  dnsProvider?: string | null
  renewDays?: number
  keyType?: string
  shared?: boolean
  issuing?: boolean
  lastRenewal?: string | null
  lastAttempt?: string | null
  lastError?: string | null
  lastErrorAt?: string | null
  lastOutcome?: string | null
  failures?: number
  nextCheck?: string | null
  backoffUntil?: string | null
  ordersPlaced?: number
  httpPort?: number
  httpError?: string | null
  certificate?: TlsCertificate
  renewalDue?: string | null
}

export interface TlsCertificate {
  subject: string
  issuer: string
  domainNames: string[]
  notBefore: string
  notAfter: string
  daysLeft: number
  placeholder: boolean
  sha256: string
  serial: string
  /** Where this certificate came from: built-in ACME, a file the operator pointed to, or a dev self-signed cert. */
  source: 'acme' | 'file' | 'self-signed' | 'unavailable'
  origin?: string
  /** Comma-separated listener names (ADMIN, MCP, A2A) serving this exact certificate. */
  listeners: string
  lastRenewal?: string | null
  lastError?: string | null
  nextCheck?: string | null
  challenge?: string
  directory?: string
  staging?: boolean
  issuing?: boolean
  failures?: number
}

export interface TlsCertificatesResponse {
  acme: AcmeStatus
  certificates: TlsCertificate[]
}

export async function getTlsCertificates(): Promise<TlsCertificatesResponse> {
  return api('/api/tls/certificates')
}

/** POST /api/tls/renew: admin-only, force a renewal now. Guarded (429 + Retry-After) against hammering the CA. */
export async function renewTlsCertificate(): Promise<{ status?: string; message?: string; error?: string; retryAt?: string }> {
  return api('/api/tls/renew', { method: 'POST' })
}
