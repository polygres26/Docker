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

export async function getConnection(id: string): Promise<Connection> {
  return api<Connection>(`/api/connections/${id}`)
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

export interface SummarizeResult {
  summary: string
  judge?: { approved: boolean; explanation: string }
}

export async function summarizeObject(id: string, type: string, name: string): Promise<SummarizeResult> {
  return api(`/api/connections/${id}/summarize?type=${encodeURIComponent(type)}&name=${encodeURIComponent(name)}`, { method: 'POST' })
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

export interface CapturedStatement {
  sqlId: string
  sqlText: string
  executions: number
  elapsedTimeMicros: number
  cpuTimeMicros: number
  bufferGets: number
  diskReads: number
  rowsProcessed: number
  parseCalls: number
  parsingSchema: string
  module: string | null
}

export interface FindingsResult extends ScanResult {
  workload?: CapturedStatement[]
  workloadError?: string
}

export async function runConnectionFindings(id: string): Promise<FindingsResult> {
  return api(`/api/connections/${id}/findings`, { method: 'POST' })
}

export interface WorkloadSummary {
  distinctStatements: number
  totalExecutions: number
  totalElapsedTimeMicros: number
  totalCpuTimeMicros: number
  totalBufferGets: number
  totalDiskReads: number
  topModules: Record<string, number>
  topByElapsedTime: CapturedStatement | null
}

export interface WorkloadResult {
  statements: CapturedStatement[]
  summary: WorkloadSummary
}

export async function runConnectionWorkload(id: string): Promise<WorkloadResult> {
  return api(`/api/connections/${id}/workload`, { method: 'POST' })
}

// --- LLM configuration ---

export type LlmRole = 'primary' | 'judge'
export type LlmProviderType = 'local' | 'builtin' | 'external'

export interface LlmSettings {
  role: 'PRIMARY' | 'JUDGE'
  providerType: 'LOCAL' | 'BUILTIN' | 'EXTERNAL'
  apiKey: null // never sent back by the server
  baseUrl: string | null
  modelPath: string | null
  model: string | null
  enabled: boolean
  updatedAt: string | null
}

export async function getLlmSettings(role: LlmRole): Promise<LlmSettings> {
  return api(`/api/llm-settings/${role}`)
}

export async function saveLlmSettings(role: LlmRole, settings: {
  providerType: LlmProviderType
  apiKey?: string
  baseUrl?: string
  modelPath?: string
  model: string
  enabled: boolean
}): Promise<LlmSettings> {
  return api(`/api/llm-settings/${role}`, { method: 'PUT', body: JSON.stringify(settings) })
}

export interface LocalModelPreset {
  label: string
  modelPath: string
}

export async function getLocalModelPresets(): Promise<{ qwen: LocalModelPreset; gemma: LocalModelPreset }> {
  return api('/api/llm-settings/local-presets')
}

// --- Uploaded reports (no live connection) ---

export interface UploadedReport {
  id: string
  name: string
  dialect: string
  filename: string
  textLength: number
  uploadedAt: string
  analysisJson: string | null
  analyzedAt: string | null
}

export async function listReports(): Promise<UploadedReport[]> {
  return api('/api/reports')
}

export async function getReport(id: string): Promise<UploadedReport> {
  return api(`/api/reports/${id}`)
}

export async function deleteReport(id: string): Promise<void> {
  await fetch(`/api/reports/${id}`, { method: 'DELETE' })
}

export async function uploadReport(file: File, name: string, dialect: string): Promise<UploadedReport> {
  const bytes = await file.arrayBuffer()
  const qs = new URLSearchParams({ name, dialect, filename: file.name })
  const res = await fetch(`/api/reports?${qs.toString()}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: bytes,
  })
  const body = await res.json()
  if (!res.ok) throw new Error(body?.error ?? `Upload failed (HTTP ${res.status})`)
  return body as UploadedReport
}

export interface ReportFinding {
  feature: string
  severity: 'LOW' | 'MEDIUM' | 'HIGH'
  note: string
}

export interface ReportWorkloadItem {
  description: string
  detail: string
}

export interface ReportAnalysis {
  sourceVersion: string | null
  tier: 'EASY' | 'MEDIUM' | 'HARD'
  tierReason: string
  findings: ReportFinding[]
  topWorkload: ReportWorkloadItem[]
  caveats: string[]
  judgeVerdict?: { approved: boolean; explanation: string } | null
}

export async function analyzeReport(id: string): Promise<ReportAnalysis> {
  return api(`/api/reports/${id}/analyze`, { method: 'POST' })
}

export async function analyzeReportsBatch(ids: string[]): Promise<ReportAnalysis> {
  return api('/api/reports/analyze-batch', { method: 'POST', body: JSON.stringify({ ids }) })
}

// --- Sizing ---

export interface SizingRecommendation {
  tier: 'SMALL' | 'MEDIUM' | 'LARGE' | 'XLARGE'
  vCpus: number
  memoryGB: number
  storageGB: number
  storageIops: number
  maxConnections: number
  rationale: string[]
  caveats: string[]
}

export async function runConnectionSizing(id: string): Promise<SizingRecommendation> {
  return api(`/api/connections/${id}/sizing`, { method: 'POST' })
}

export async function runReportsSizing(ids: string[]): Promise<SizingRecommendation> {
  return api('/api/reports/sizing', { method: 'POST', body: JSON.stringify({ ids }) })
}

// --- PolyWire connection + management ---
// The browser only ever talks to these Advisor routes; Advisor's own backend proxies to
// PolyWire's admin API server-to-server, so PolyWire's admin token never reaches the browser.

export interface WireConnectionStatus {
  adminUrl: string | null
  hasToken: boolean
  configured: boolean
}

export async function getWireSettings(): Promise<WireConnectionStatus> {
  return api('/api/wire-settings')
}

export async function saveWireSettings(adminUrl: string, adminToken: string): Promise<WireConnectionStatus> {
  return api('/api/wire-settings', { method: 'PUT', body: JSON.stringify({ adminUrl, adminToken }) })
}

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
  return api('/api/wire/firewall-rules')
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
  return api('/api/wire/firewall-rules', { method: 'POST', body: JSON.stringify(rule) })
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
  await api(`/api/wire/firewall-rules/${id}`, { method: 'PUT', body: JSON.stringify(rule) })
}

export async function deleteFirewallRule(id: number): Promise<void> {
  await api(`/api/wire/firewall-rules/${id}`, { method: 'DELETE' })
}

// --- PolyWire full config (backends, router rules, QoS, ACL, OAuth, ...) ---
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
  return api('/api/wire/config')
}

export async function saveWireConfig(partial: Partial<WireConfig>): Promise<{ ok: boolean; version: number }> {
  return api('/api/wire/config', { method: 'PUT', body: JSON.stringify(partial) })
}
