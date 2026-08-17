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

export async function runScan(req: ScanRequest): Promise<ScanResult> {
  const res = await fetch('/api/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  const body = await res.json()
  if (!res.ok) {
    throw new Error(body.error ?? `Scan failed (HTTP ${res.status})`)
  }
  return body as ScanResult
}
