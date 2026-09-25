import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import {
  type CapturedStatement,
  type Connection,
  type FindingsResult,
  type ParameterInfo,
  type SummarizeResult,
  type WorkloadResult,
  getConnection,
  getObjectDetail,
  getObjects,
  getParameters,
  runConnectionFindings,
  runConnectionWorkload,
  summarizeObject,
} from '../api/client'
import { tierTone } from '../lib/tone'
import {
  AnchorButton, Button, CodeBlock, DataTable, EmptyState, Input, KpiStrip, Loading, Meter, Notice, PageHeader, Section, Stack, StatusPill, Tabs, Tag, List, type Tone,
} from '../ui'
import { table } from '../ui/styles'
import styles from './ConnectionDetail.module.css'

type Tab = 'findings' | 'objects' | 'workload' | 'parameters'

export default function ConnectionDetail() {
  const { id } = useParams<{ id: string }>()
  const [tab, setTab] = useState<Tab>('findings')
  const [connection, setConnection] = useState<Connection | null>(null)

  useEffect(() => {
    if (id) getConnection(id).then(setConnection).catch(() => {})
  }, [id])

  return (
    <>
      <PageHeader
        back={{ to: '/connections', label: 'Connections' }}
        title={connection?.name ?? 'Connection detail'}
        subtitle={connection && <span className={styles.url}>{connection.jdbcUrl}</span>}
      />

      <Tabs
        label="Connection views"
        value={tab}
        onChange={setTab}
        tabs={[
          { id: 'findings', label: 'Findings' },
          { id: 'objects', label: 'Objects' },
          { id: 'workload', label: 'Workload' },
          { id: 'parameters', label: 'Parameters' },
        ]}
      />

      {id && tab === 'findings' && <FindingsTab id={id} onSeeWorkload={() => setTab('workload')} />}
      {id && tab === 'objects' && <ObjectsTab id={id} />}
      {id && tab === 'workload' && <WorkloadTab id={id} />}
      {id && tab === 'parameters' && <ParametersTab id={id} />}
    </>
  )
}

// --- Findings ---------------------------------------------------------

function severity(points: number): { tone: Tone; label: string } {
  if (points >= 15) return { tone: 'red', label: 'High' }
  if (points >= 5) return { tone: 'amber', label: 'Medium' }
  return { tone: 'green', label: 'Low' }
}

function formatMicros(micros: number): string {
  const ms = micros / 1000
  if (ms < 1) return '<1 ms'
  if (ms < 1000) return `${ms.toFixed(1)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}

function FindingsTab({ id, onSeeWorkload }: { id: string; onSeeWorkload: () => void }) {
  const [result, setResult] = useState<FindingsResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  async function run() {
    setLoading(true); setError(null)
    try {
      setResult(await runConnectionFindings(id))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  // oxlint-disable-next-line react-hooks/exhaustive-deps -- run() closes over id; re-run only when id changes
  useEffect(() => { run() }, [id])

  if (loading) return <Section><Loading>Profiling schema, scoring migration difficulty, capturing workload…</Loading></Section>

  if (error) {
    return (
      <Section>
        <Notice tone="error">{error}</Notice>
        <Button variant="primary" onClick={run}>Retry</Button>
      </Section>
    )
  }
  if (!result) return null

  const { snapshot, score, workload, workloadError } = result
  const sortedFindings = [...score.findings].sort((a, b) => b.points - a.points)
  const meterPct = Math.min(100, (score.totalScore / 100) * 100)
  const sortedWorkload = workload ? [...workload].sort((a, b) => b.elapsedTimeMicros - a.elapsedTimeMicros) : []
  const [tierName, tierDetail] = score.tier.split(' -- ')

  return (
    <Stack>
      {/* Overall complexity hero */}
      <Section
        title="Migration difficulty"
        actions={
          <div className={styles.row}>
            <Button variant="secondary" onClick={run}>Re-run findings</Button>
            <AnchorButton variant="primary" href={`/api/connections/${id}/report`}>Download report</AnchorButton>
          </div>
        }
      >
        <div className={styles.hero}>
          <div>
            <StatusPill tone={tierTone(tierName)}>{tierName}</StatusPill>
            <p className={styles.tierDetail}>{tierDetail}</p>
            {snapshot.sourceVersion && <p className={styles.muted}>{snapshot.sourceVersion}</p>}
          </div>
          <div className={styles.score}>
            <div className={styles.scoreValue}>{score.totalScore}</div>
            <div className={styles.muted}>overall complexity score</div>
          </div>
        </div>
        <Meter
          pct={meterPct}
          label={`Complexity score ${score.totalScore} of 100`}
          segments={[
            { flex: 20, tone: 'green', label: 'EASY (0–20)' },
            { flex: 40, tone: 'amber', label: 'MEDIUM (21–60)' },
            { flex: 40, tone: 'red', label: 'HARD (61+)' },
          ]}
        />
      </Section>

      {score.warnings.length > 0 && (
        <Notice tone="warn" title="Warnings">
          <ul className={styles.plainList}>{score.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul>
        </Notice>
      )}

      {/* Feature inventory quick stats */}
      <div>
        <KpiStrip
          label="Feature inventory"
          items={[
            { label: 'Tables', value: snapshot.tableCount }, { label: 'Views', value: snapshot.viewCount },
            { label: 'Mat. views', value: snapshot.materializedViewCount }, { label: 'Sequences', value: snapshot.sequenceCount },
            { label: 'Triggers', value: snapshot.simpleTriggerCount + snapshot.complexTriggerCount },
            { label: 'Packages', value: snapshot.packageCount }, { label: 'Procedures', value: snapshot.standaloneProcedureCount },
            { label: 'Functions', value: snapshot.standaloneFunctionCount }, { label: 'DB links', value: snapshot.dbLinkCount },
            { label: 'Scheduled jobs', value: snapshot.scheduledJobCount }, { label: 'Partitioned tables', value: snapshot.partitionedTableCount },
          ]}
        />
      </div>

      {/* Per-item complexity */}
      <Section title="Migration complexity by item" flush={sortedFindings.length > 0}>
        {sortedFindings.length === 0 && <p className={styles.muted}>No difficulty-scoring findings -- looks like a clean schema+data migration.</p>}
        {sortedFindings.length > 0 && (
          <List>
            {sortedFindings.map((f, i) => (
              <li key={i} className={styles.finding}>
                <Tag tone={severity(f.points).tone}>{severity(f.points).label}</Tag>
                <div className={styles.findingBody}>
                  <div className={styles.findingName}>{f.feature} <span className={styles.count}>× {f.count}</span></div>
                  <div className={styles.muted}>{f.note}</div>
                </div>
                <div className={styles.points}>{f.points} pts</div>
              </li>
            ))}
          </List>
        )}
      </Section>

      {/* Workload captured -- compact pointer; the full summary/stats/table live in the Workload tab */}
      <Section title="Workload captured" actions={<Button size="sm" onClick={onSeeWorkload}>View workload →</Button>}>
        {workloadError && <p className={styles.warnText}>{workloadError}</p>}
        {!workloadError && sortedWorkload.length === 0 && <p className={styles.muted}>No cached SQL captured.</p>}
        {sortedWorkload.length > 0 && (
          <p className={styles.muted}>
            {sortedWorkload.length} statement{sortedWorkload.length === 1 ? '' : 's'} captured from V$SQL, top by elapsed time:{' '}
            <span className={table.mono}>
              {sortedWorkload[0].sqlText.slice(0, 60)}{sortedWorkload[0].sqlText.length > 60 ? '…' : ''}
            </span>{' '}
            ({formatMicros(sortedWorkload[0].elapsedTimeMicros)})
          </p>
        )}
      </Section>
    </Stack>
  )
}

// --- Workload -------------------------------------------------------------

type SortKey = 'elapsedTimeMicros' | 'cpuTimeMicros' | 'bufferGets' | 'diskReads' | 'executions' | 'avgElapsed'

const SORT_LABELS: Record<SortKey, string> = {
  elapsedTimeMicros: 'Elapsed Time',
  cpuTimeMicros: 'CPU Time',
  bufferGets: 'Buffer Gets',
  diskReads: 'Disk Reads',
  executions: 'Executions',
  avgElapsed: 'Avg Elapsed / Exec',
}

function avgElapsed(s: CapturedStatement): number {
  return s.executions === 0 ? 0 : s.elapsedTimeMicros / s.executions
}

function WorkloadTab({ id }: { id: string }) {
  const [result, setResult] = useState<WorkloadResult | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('elapsedTimeMicros')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  async function run() {
    setLoading(true); setError(null)
    try {
      setResult(await runConnectionWorkload(id))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  // oxlint-disable-next-line react-hooks/exhaustive-deps -- run() closes over id; re-run only when id changes
  useEffect(() => { run() }, [id])

  if (loading) return <Section><Loading>Capturing V$SQL snapshot…</Loading></Section>
  if (error) {
    return (
      <Section>
        <Notice tone="error">{error}</Notice>
        <Button variant="primary" onClick={run}>Retry</Button>
      </Section>
    )
  }
  if (!result) return null

  const { statements, summary } = result

  function sortValue(s: CapturedStatement): number {
    return sortKey === 'avgElapsed' ? avgElapsed(s) : s[sortKey]
  }

  const filtered = statements.filter((s) =>
    s.sqlText.toLowerCase().includes(filter.toLowerCase()) ||
    (s.module ?? '').toLowerCase().includes(filter.toLowerCase()))
  const sorted = [...filtered].sort((a, b) => (sortValue(a) - sortValue(b)) * (sortDir === 'desc' ? -1 : 1))

  function toggleSort(key: SortKey) {
    if (key === sortKey) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'))
    } else {
      setSortKey(key); setSortDir('desc')
    }
  }

  return (
    <Stack>
      {/* Summary, in the source database's own vocabulary -- same terms an AWR/Statspack report uses */}
      <div>
        <KpiStrip
          label="Workload summary"
          items={[
            { label: 'Distinct SQL Statements', value: summary.distinctStatements.toLocaleString() },
            { label: 'Executions', value: summary.totalExecutions.toLocaleString() },
            { label: 'Elapsed Time', value: formatMicros(summary.totalElapsedTimeMicros) },
            { label: 'CPU Time', value: formatMicros(summary.totalCpuTimeMicros) },
            { label: 'Buffer Gets', value: summary.totalBufferGets.toLocaleString() },
            { label: 'Disk Reads', value: summary.totalDiskReads.toLocaleString() },
          ]}
        />
        <div className={styles.summaryFoot}>
          <span className={styles.muted}>Point-in-time snapshot of Oracle's shared-pool cursor cache (V$SQL), scoped to this connection's schema.</span>
          <Button size="sm" variant="primary" onClick={run}>Re-capture</Button>
        </div>
      </div>

      {summary.topByElapsedTime && (
        <Section title="Top SQL by Elapsed Time">
          <CodeBlock>{summary.topByElapsedTime.sqlText}</CodeBlock>
          <div className={styles.facts}>
            <span>SQL_ID: <strong>{summary.topByElapsedTime.sqlId}</strong></span>
            <span>Elapsed: <strong>{formatMicros(summary.topByElapsedTime.elapsedTimeMicros)}</strong></span>
            <span>Executions: <strong>{summary.topByElapsedTime.executions.toLocaleString()}</strong></span>
            <span>Module: <strong>{summary.topByElapsedTime.module || '—'}</strong></span>
          </div>
        </Section>
      )}

      {Object.keys(summary.topModules).length > 0 && (
        <Section title="Workload by module">
          <div className={styles.chips}>
            {Object.entries(summary.topModules).map(([module, count]) => (
              <span key={module} className={styles.chip}>{module} <span className={styles.chipCount}>× {count}</span></span>
            ))}
          </div>
        </Section>
      )}

      {/* Full per-statement statistics -- every application SQL captured, sortable/filterable */}
      <Section
        title="All application SQL & statistics"
        actions={
          <Input
            className={styles.filter}
            aria-label="Filter by SQL text or module"
            placeholder="Filter by SQL text or module…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        }
        flush
      >
        <div className={styles.sorts} role="group" aria-label="Sort by">
          {(Object.keys(SORT_LABELS) as SortKey[]).map((key) => (
            <button
              key={key}
              type="button"
              className={`${styles.sort} ${sortKey === key ? styles.sortOn : ''}`}
              aria-pressed={sortKey === key}
              onClick={() => toggleSort(key)}
            >
              {SORT_LABELS[key]} {sortKey === key ? (sortDir === 'desc' ? '↓' : '↑') : ''}
            </button>
          ))}
        </div>

        {sorted.length === 0 && <EmptyState title="No statements match" />}
        {sorted.length > 0 && (
          <DataTable caption="Captured SQL statements and statistics" minWidth={false}>
            <thead>
              <tr>
                <th scope="col">SQL_ID</th><th scope="col">SQL Text</th><th scope="col" className={table.num}>Executions</th><th scope="col" className={table.num}>Elapsed Time</th>
                <th scope="col" className={table.num}>CPU Time</th><th scope="col" className={table.num}>Buffer Gets</th><th scope="col" className={table.num}>Disk Reads</th><th scope="col" className={table.num}>Avg Elapsed / Exec</th><th scope="col">Module</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((s) => (
                <tr key={s.sqlId}>
                  <td className={`${table.mono} ${styles.muted} ${styles.sqlId}`} title={s.sqlId}>{s.sqlId}</td>
                  <td className={`${table.mono} ${styles.sqlText}`} title={s.sqlText}>
                    {s.sqlText.length > 90 ? s.sqlText.slice(0, 90) + '…' : s.sqlText}
                  </td>
                  <td className={table.num}>{s.executions.toLocaleString()}</td>
                  <td className={table.num}>{formatMicros(s.elapsedTimeMicros)}</td>
                  <td className={table.num}>{formatMicros(s.cpuTimeMicros)}</td>
                  <td className={table.num}>{s.bufferGets.toLocaleString()}</td>
                  <td className={table.num}>{s.diskReads.toLocaleString()}</td>
                  <td className={table.num}>{formatMicros(avgElapsed(s))}</td>
                  <td className={styles.muted}>{s.module || '—'}</td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </Section>
    </Stack>
  )
}

// --- Objects ------------------------------------------------------------

function ObjectsTab({ id }: { id: string }) {
  const [objects, setObjects] = useState<Record<string, string[]> | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<{ type: string; name: string } | null>(null)
  const [detail, setDetail] = useState<{ columns?: unknown[]; source?: string } | null>(null)
  const [summarizing, setSummarizing] = useState(false)
  const [summary, setSummary] = useState<SummarizeResult | null>(null)
  const [summaryError, setSummaryError] = useState<string | null>(null)

  useEffect(() => {
    getObjects(id).then(setObjects).catch((e) => setError(String(e.message ?? e)))
  }, [id])

  async function select(type: string, name: string) {
    setSelected({ type, name })
    setDetail(null)
    setSummary(null)
    setSummaryError(null)
    try {
      setDetail(await getObjectDetail(id, type, name))
    } catch (e) {
      setDetail({ source: `Error: ${e instanceof Error ? e.message : String(e)}` })
    }
  }

  async function handleSummarize() {
    if (!selected) return
    setSummarizing(true); setSummaryError(null); setSummary(null)
    try {
      setSummary(await summarizeObject(id, selected.type, selected.name))
    } catch (e) {
      setSummaryError(e instanceof Error ? e.message : String(e))
    } finally {
      setSummarizing(false)
    }
  }

  if (error) return <Notice tone="error">{error}</Notice>
  if (!objects) return <Section><Loading /></Section>

  return (
    <div className={styles.objects}>
      <Section title="Objects" className={styles.objectList}>
        {Object.entries(objects).length === 0 && <p className={styles.muted}>No objects found.</p>}
        {Object.entries(objects).map(([type, names]) => (
          <div key={type} className={styles.objectGroup}>
            <h3 className={styles.objectType}>{type} ({names.length})</h3>
            <ul className={styles.objectNames}>
              {names.map((n) => {
                const active = selected?.type === type && selected?.name === n
                return (
                  <li key={n}>
                    <button
                      type="button"
                      className={`${styles.objectBtn} ${active ? styles.objectBtnOn : ''}`}
                      aria-current={active ? 'true' : undefined}
                      onClick={() => select(type, n)}
                    >
                      {n}
                    </button>
                  </li>
                )
              })}
            </ul>
          </div>
        ))}
      </Section>

      <Section title={selected ? `${selected.type}: ${selected.name}` : 'Details'} flush={!!detail?.columns} className={styles.objectDetail}>
        {!selected && <p className={styles.muted}>Select an object to inspect it.</p>}
        {selected && (
          <>
            {detail?.columns && (
              <DataTable caption="Columns" minWidth={false}>
                <thead><tr><th scope="col">Column</th><th scope="col">Type</th><th scope="col">Nullable</th><th scope="col">Default</th></tr></thead>
                <tbody>
                  {(detail.columns as { name: string; dataType: string; nullable: boolean; defaultValue: string | null }[]).map((c) => (
                    <tr key={c.name}>
                      <td>{c.name}</td><td className={table.mono}>{c.dataType}</td><td>{c.nullable ? 'Y' : 'N'}</td><td>{c.defaultValue ?? ''}</td>
                    </tr>
                  ))}
                </tbody>
              </DataTable>
            )}
            {detail?.source !== undefined && (
              <>
                <CodeBlock maxHeight={320}>{detail.source || '(empty)'}</CodeBlock>
                {detail.source && (
                  <div className={styles.summarize}>
                    <Button variant="primary" onClick={handleSummarize} disabled={summarizing}>
                      {summarizing ? 'Summarizing…' : 'Summarize with LLM'}
                    </Button>
                  </div>
                )}
                {summaryError && <Notice tone="error">{summaryError}</Notice>}
                {summary && (
                  <div className={styles.summary}>
                    {summary.judge && (
                      <div className={styles.judge}>
                        <StatusPill tone={summary.judge.approved ? 'green' : 'amber'}>Judge: {summary.judge.approved ? 'Approved' : 'Flagged'}</StatusPill>
                      </div>
                    )}
                    {summary.judge && !summary.judge.approved && (
                      <p className={styles.warnText}>{summary.judge.explanation}</p>
                    )}
                    <div className={styles.summaryText}>{summary.summary}</div>
                  </div>
                )}
              </>
            )}
          </>
        )}
      </Section>
    </div>
  )
}

// --- Parameters -----------------------------------------------------------

function ParametersTab({ id }: { id: string }) {
  const [params, setParams] = useState<ParameterInfo[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState('')

  useEffect(() => {
    getParameters(id).then(setParams).catch((e) => setError(String(e.message ?? e)))
  }, [id])

  if (error) return <Notice tone="error">{error}</Notice>
  if (!params) return <Section><Loading /></Section>

  const filtered = params.filter((p) => p.name.toLowerCase().includes(filter.toLowerCase()))

  return (
    <Section
      title="Parameters"
      meta={`${filtered.length} of ${params.length}`}
      actions={<Input className={styles.filter} aria-label="Filter parameters" placeholder="Filter parameters…" value={filter} onChange={(e) => setFilter(e.target.value)} />}
      flush
    >
      <DataTable caption="Database parameters" minWidth={false}>
        <thead><tr><th scope="col">Name</th><th scope="col">Value</th><th scope="col">Default?</th></tr></thead>
        <tbody>
          {filtered.map((p) => (
            <tr key={p.name}>
              <td className={table.mono}>{p.name}</td>
              <td className={table.mono}>{p.value}</td>
              <td>{p.isDefault ? 'Y' : <Tag tone="amber">customized</Tag>}</td>
            </tr>
          ))}
        </tbody>
      </DataTable>
    </Section>
  )
}
