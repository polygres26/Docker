import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
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

type Tab = 'findings' | 'objects' | 'workload' | 'parameters'

export default function ConnectionDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [tab, setTab] = useState<Tab>('findings')
  const [connection, setConnection] = useState<Connection | null>(null)

  useEffect(() => {
    if (id) getConnection(id).then(setConnection).catch(() => {})
  }, [id])

  return (
    <div style={{ maxWidth: 1100 }}>
      <button onClick={() => navigate('/connections')} style={{ marginBottom: 16, background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer', fontSize: 13 }}>
        ← Connections
      </button>
      <h1 style={{ marginBottom: 2, fontSize: 22 }}>{connection?.name ?? 'Connection detail'}</h1>
      {connection && <p style={{ color: 'var(--muted)', marginTop: 0, fontSize: 13, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>{connection.jdbcUrl}</p>}

      <div style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        {(['findings', 'objects', 'workload', 'parameters'] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              background: tab === t ? 'var(--accent)' : 'none',
              color: tab === t ? 'white' : 'var(--text)',
              border: '1px solid var(--border)',
              borderRadius: 8,
              padding: '8px 16px',
              cursor: 'pointer',
              textTransform: 'capitalize',
            }}
          >
            {t}
          </button>
        ))}
      </div>

      {id && tab === 'findings' && <FindingsTab id={id} onSeeWorkload={() => setTab('workload')} />}
      {id && tab === 'objects' && <ObjectsTab id={id} />}
      {id && tab === 'workload' && <WorkloadTab id={id} />}
      {id && tab === 'parameters' && <ParametersTab id={id} />}
    </div>
  )
}

// --- Findings ---------------------------------------------------------

/** Soft-background + solid-text badge, matching Omnigate's badgePass/badgeWarn/badgeFail pattern (Admin.module.css) rather than a solid fill with dark text. */
function severityStyle(points: number): { bg: string; fg: string; label: string } {
  if (points >= 15) return { bg: 'var(--hard-soft)', fg: 'var(--hard)', label: 'High' }
  if (points >= 5) return { bg: 'var(--medium-soft)', fg: 'var(--medium)', label: 'Medium' }
  return { bg: 'var(--easy-soft)', fg: 'var(--easy)', label: 'Low' }
}

function tierStyle(tier: string): { bg: string; fg: string } {
  if (tier.startsWith('EASY')) return { bg: 'var(--easy-soft)', fg: 'var(--accent-strong)' }
  if (tier.startsWith('MEDIUM')) return { bg: 'var(--medium-soft)', fg: 'var(--medium)' }
  return { bg: 'var(--hard-soft)', fg: 'var(--hard)' }
}

function formatMicros(micros: number): string {
  const ms = micros / 1000
  if (ms < 1) return '<1 ms'
  if (ms < 1000) return `${ms.toFixed(1)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}

function FindingsTab({ id, onSeeWorkload }: { id: string; onSeeWorkload: () => void }) {
  const [result, setResult] = useState<FindingsResult | null>(null)
  const [loading, setLoading] = useState(true);
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

  useEffect(() => { run() }, [id])

  if (loading) {
    return (
      <div className="panel" style={{ textAlign: 'center', padding: 48 }}>
        <p style={{ color: 'var(--muted)' }}>Profiling schema, scoring migration difficulty, capturing workload…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="panel" style={{ borderColor: 'var(--hard)' }}>
        <p style={{ color: 'var(--hard)' }}>{error}</p>
        <button className="primary" onClick={run} style={{ marginTop: 12 }}>Retry</button>
      </div>
    )
  }
  if (!result) return null

  const { snapshot, score, workload, workloadError } = result
  const sortedFindings = [...score.findings].sort((a, b) => b.points - a.points)
  const meterPct = Math.min(100, (score.totalScore / 100) * 100)
  const sortedWorkload = workload ? [...workload].sort((a, b) => b.elapsedTimeMicros - a.elapsedTimeMicros) : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Overall complexity hero */}
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 16 }}>
          <div>
            <span className="tier-badge" style={{ background: tierStyle(score.tier).bg, color: tierStyle(score.tier).fg }}>
              {score.tier.split(' -- ')[0]}
            </span>
            <p style={{ marginTop: 10, marginBottom: 4, maxWidth: 560 }}>{score.tier.split(' -- ')[1]}</p>
            {snapshot.sourceVersion && <p style={{ color: 'var(--muted)', fontSize: 13, margin: 0 }}>{snapshot.sourceVersion}</p>}
          </div>
          <div style={{ textAlign: 'right' }}>
            <div style={{ fontSize: 36, fontWeight: 700, lineHeight: 1 }}>{score.totalScore}</div>
            <div style={{ color: 'var(--muted)', fontSize: 12 }}>overall complexity score</div>
          </div>
        </div>

        <div style={{ marginTop: 18 }}>
          <div style={{ position: 'relative', height: 10, borderRadius: 999, overflow: 'hidden', display: 'flex' }}>
            <div style={{ flex: 20, background: 'var(--easy)' }} />
            <div style={{ flex: 40, background: 'var(--medium)' }} />
            <div style={{ flex: 40, background: 'var(--hard)' }} />
            <div
              style={{
                position: 'absolute', top: -3, left: `calc(${meterPct}% - 2px)`,
                width: 4, height: 16, background: 'var(--text)', borderRadius: 2,
              }}
            />
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--muted)', marginTop: 4 }}>
            <span>EASY (0–20)</span><span>MEDIUM (21–60)</span><span>HARD (61+)</span>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 10, marginTop: 16 }}>
          <button className="primary" onClick={run}>Re-run findings</button>
          <a
            href={`/api/connections/${id}/report`}
            className="primary"
            style={{ textDecoration: 'none', display: 'inline-flex', alignItems: 'center' }}
          >
            Download report
          </a>
        </div>
      </div>

      {score.warnings.length > 0 && (
        <div className="panel" style={{ borderColor: 'var(--medium)' }}>
          <strong>Warnings</strong>
          <ul style={{ marginBottom: 0 }}>
            {score.warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </div>
      )}

      {/* Feature inventory quick stats */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Feature inventory</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))', gap: 12 }}>
          {[
            ['Tables', snapshot.tableCount], ['Views', snapshot.viewCount],
            ['Mat. views', snapshot.materializedViewCount], ['Sequences', snapshot.sequenceCount],
            ['Triggers', snapshot.simpleTriggerCount + snapshot.complexTriggerCount],
            ['Packages', snapshot.packageCount], ['Procedures', snapshot.standaloneProcedureCount],
            ['Functions', snapshot.standaloneFunctionCount], ['DB links', snapshot.dbLinkCount],
            ['Scheduled jobs', snapshot.scheduledJobCount], ['Partitioned tables', snapshot.partitionedTableCount],
          ].map(([label, value]) => (
            <div key={label as string} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 10, padding: '10px 14px' }}>
              <div style={{ fontSize: 20, fontWeight: 700 }}>{value as number}</div>
              <div style={{ color: 'var(--muted)', fontSize: 12 }}>{label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Per-item complexity */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Migration complexity by item</h3>
        {sortedFindings.length === 0 && <p style={{ color: 'var(--muted)' }}>No difficulty-scoring findings -- looks like a clean schema+data migration.</p>}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {sortedFindings.map((f, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 12px', background: 'var(--bg)', borderRadius: 10, border: '1px solid var(--border)' }}>
              <span style={{
                background: severityStyle(f.points).bg, color: severityStyle(f.points).fg, fontSize: 11, fontWeight: 650,
                borderRadius: 999, padding: '3px 10px', flexShrink: 0, width: 64, textAlign: 'center',
              }}>
                {severityStyle(f.points).label}
              </span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontWeight: 600 }}>{f.feature} <span style={{ color: 'var(--muted)', fontWeight: 400 }}>× {f.count}</span></div>
                <div style={{ color: 'var(--muted)', fontSize: 13 }}>{f.note}</div>
              </div>
              <div style={{ fontWeight: 700, flexShrink: 0 }}>{f.points} pts</div>
            </div>
          ))}
        </div>
      </div>

      {/* Workload captured -- compact pointer; the full summary/stats/table live in the Workload tab */}
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ margin: 0 }}>Workload captured</h3>
          <button className="btn-ghost-link" onClick={onSeeWorkload} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, padding: '6px 12px', color: 'var(--accent)', cursor: 'pointer', fontSize: 13, fontWeight: 600 }}>
            View workload →
          </button>
        </div>
        {workloadError && <p style={{ color: 'var(--medium)' }}>{workloadError}</p>}
        {!workloadError && sortedWorkload.length === 0 && <p style={{ color: 'var(--muted)' }}>No cached SQL captured.</p>}
        {sortedWorkload.length > 0 && (
          <p style={{ color: 'var(--muted)', fontSize: 13, marginBottom: 0 }}>
            {sortedWorkload.length} statement{sortedWorkload.length === 1 ? '' : 's'} captured from V$SQL, top by elapsed time:{' '}
            <span style={{ fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>
              {sortedWorkload[0].sqlText.slice(0, 60)}{sortedWorkload[0].sqlText.length > 60 ? '…' : ''}
            </span>{' '}
            ({formatMicros(sortedWorkload[0].elapsedTimeMicros)})
          </p>
        )}
      </div>
    </div>
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

  useEffect(() => { run() }, [id])

  if (loading) {
    return (
      <div className="panel" style={{ textAlign: 'center', padding: 48 }}>
        <p style={{ color: 'var(--muted)' }}>Capturing V$SQL snapshot…</p>
      </div>
    )
  }
  if (error) {
    return (
      <div className="panel" style={{ borderColor: 'var(--hard)' }}>
        <p style={{ color: 'var(--hard)' }}>{error}</p>
        <button className="primary" onClick={run} style={{ marginTop: 12 }}>Retry</button>
      </div>
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
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Summary, in the source database's own vocabulary -- same terms an AWR/Statspack report uses */}
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
          <h3 style={{ margin: 0 }}>Workload summary</h3>
          <button className="primary" onClick={run}>Re-capture</button>
        </div>
        <p style={{ color: 'var(--muted)', fontSize: 13, marginTop: 4 }}>
          Point-in-time snapshot of Oracle's shared-pool cursor cache (V$SQL), scoped to this connection's schema.
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))', gap: 12, marginTop: 12 }}>
          {[
            ['Distinct SQL Statements', summary.distinctStatements.toLocaleString()],
            ['Executions', summary.totalExecutions.toLocaleString()],
            ['Elapsed Time', formatMicros(summary.totalElapsedTimeMicros)],
            ['CPU Time', formatMicros(summary.totalCpuTimeMicros)],
            ['Buffer Gets', summary.totalBufferGets.toLocaleString()],
            ['Disk Reads', summary.totalDiskReads.toLocaleString()],
          ].map(([label, value]) => (
            <div key={label} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 10, padding: '10px 14px' }}>
              <div style={{ fontSize: 19, fontWeight: 700, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>{value}</div>
              <div style={{ color: 'var(--muted)', fontSize: 11.5 }}>{label}</div>
            </div>
          ))}
        </div>
      </div>

      {summary.topByElapsedTime && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Top SQL by Elapsed Time</h3>
          <p style={{ fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fontSize: 13, background: 'var(--bg)', padding: 12, borderRadius: 8, border: '1px solid var(--border)', whiteSpace: 'pre-wrap' }}>
            {summary.topByElapsedTime.sqlText}
          </p>
          <div style={{ display: 'flex', gap: 20, fontSize: 13, color: 'var(--muted)', flexWrap: 'wrap' }}>
            <span>SQL_ID: <strong style={{ color: 'var(--text)' }}>{summary.topByElapsedTime.sqlId}</strong></span>
            <span>Elapsed: <strong style={{ color: 'var(--text)' }}>{formatMicros(summary.topByElapsedTime.elapsedTimeMicros)}</strong></span>
            <span>Executions: <strong style={{ color: 'var(--text)' }}>{summary.topByElapsedTime.executions.toLocaleString()}</strong></span>
            <span>Module: <strong style={{ color: 'var(--text)' }}>{summary.topByElapsedTime.module || '—'}</strong></span>
          </div>
        </div>
      )}

      {Object.keys(summary.topModules).length > 0 && (
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>Workload by module</h3>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {Object.entries(summary.topModules).map(([module, count]) => (
              <span key={module} style={{ background: 'var(--accent-soft)', color: 'var(--accent-strong)', borderRadius: 999, padding: '5px 12px', fontSize: 12.5, fontWeight: 600 }}>
                {module} <span style={{ fontWeight: 400, opacity: 0.8 }}>× {count}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Full per-statement statistics -- every application SQL captured, sortable/filterable */}
      <div className="panel">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12, flexWrap: 'wrap', gap: 10 }}>
          <h3 style={{ margin: 0 }}>All application SQL &amp; statistics</h3>
          <input
            placeholder="Filter by SQL text or module…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '7px 10px', color: 'var(--text)', fontSize: 13, minWidth: 240 }}
          />
        </div>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 12 }}>
          {(Object.keys(SORT_LABELS) as SortKey[]).map((key) => (
            <button
              key={key}
              onClick={() => toggleSort(key)}
              style={{
                fontSize: 12, padding: '5px 10px', borderRadius: 999,
                border: '1px solid var(--border)',
                background: sortKey === key ? 'var(--accent)' : 'none',
                color: sortKey === key ? '#fff' : 'var(--muted)',
                cursor: 'pointer',
              }}
            >
              {SORT_LABELS[key]} {sortKey === key ? (sortDir === 'desc' ? '↓' : '↑') : ''}
            </button>
          ))}
        </div>

        {sorted.length === 0 && <p style={{ color: 'var(--muted)' }}>No statements match.</p>}
        {sorted.length > 0 && (
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>SQL_ID</th><th>SQL Text</th><th>Executions</th><th>Elapsed Time</th>
                  <th>CPU Time</th><th>Buffer Gets</th><th>Disk Reads</th><th>Avg Elapsed / Exec</th><th>Module</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((s) => (
                  <tr key={s.sqlId}>
                    <td style={{ fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fontSize: 11.5, color: 'var(--muted)' }}>{s.sqlId}</td>
                    <td style={{ maxWidth: 360, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fontSize: 12 }} title={s.sqlText}>
                      {s.sqlText.length > 90 ? s.sqlText.slice(0, 90) + '…' : s.sqlText}
                    </td>
                    <td>{s.executions.toLocaleString()}</td>
                    <td>{formatMicros(s.elapsedTimeMicros)}</td>
                    <td>{formatMicros(s.cpuTimeMicros)}</td>
                    <td>{s.bufferGets.toLocaleString()}</td>
                    <td>{s.diskReads.toLocaleString()}</td>
                    <td>{formatMicros(avgElapsed(s))}</td>
                    <td style={{ color: 'var(--muted)' }}>{s.module || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
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

  if (error) return <p style={{ color: 'var(--hard)' }}>{error}</p>
  if (!objects) return <p style={{ color: 'var(--muted)' }}>Loading…</p>

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '280px 1fr', gap: 16 }}>
      <div className="panel" style={{ maxHeight: 560, overflowY: 'auto' }}>
        {Object.entries(objects).length === 0 && <p style={{ color: 'var(--muted)' }}>No objects found.</p>}
        {Object.entries(objects).map(([type, names]) => (
          <div key={type} style={{ marginBottom: 12 }}>
            <div style={{ color: 'var(--muted)', fontSize: 12, textTransform: 'uppercase', marginBottom: 4 }}>
              {type} ({names.length})
            </div>
            {names.map((n) => (
              <div
                key={n}
                onClick={() => select(type, n)}
                style={{
                  padding: '4px 8px',
                  borderRadius: 6,
                  cursor: 'pointer',
                  background: selected?.type === type && selected?.name === n ? 'var(--accent)' : 'transparent',
                  color: selected?.type === type && selected?.name === n ? 'white' : 'var(--text)',
                  fontSize: 14,
                }}
              >
                {n}
              </div>
            ))}
          </div>
        ))}
      </div>

      <div className="panel">
        {!selected && <p style={{ color: 'var(--muted)' }}>Select an object to inspect it.</p>}
        {selected && (
          <>
            <h3>{selected.type}: {selected.name}</h3>
            {detail?.columns && (
              <table>
                <thead><tr><th>Column</th><th>Type</th><th>Nullable</th><th>Default</th></tr></thead>
                <tbody>
                  {(detail.columns as { name: string; dataType: string; nullable: boolean; defaultValue: string | null }[]).map((c) => (
                    <tr key={c.name}>
                      <td>{c.name}</td><td>{c.dataType}</td><td>{c.nullable ? 'Y' : 'N'}</td><td>{c.defaultValue ?? ''}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {detail?.source !== undefined && (
              <>
                <pre style={{ whiteSpace: 'pre-wrap', fontSize: 13, background: 'var(--bg)', padding: 12, borderRadius: 8, maxHeight: 320, overflowY: 'auto' }}>
                  {detail.source || '(empty)'}
                </pre>
                {detail.source && (
                  <button className="primary" onClick={handleSummarize} disabled={summarizing} style={{ marginTop: 12 }}>
                    {summarizing ? 'Summarizing…' : 'Summarize with LLM'}
                  </button>
                )}
                {summaryError && <p style={{ color: 'var(--hard)', marginTop: 10, fontSize: 13 }}>{summaryError}</p>}
                {summary && (
                  <div style={{ marginTop: 16 }}>
                    {summary.judge && (
                      <div style={{
                        display: 'inline-flex', alignItems: 'center', gap: 6, marginBottom: 10,
                        background: summary.judge.approved ? 'var(--easy-soft)' : 'var(--medium-soft)',
                        color: summary.judge.approved ? 'var(--accent-strong)' : 'var(--medium)',
                        borderRadius: 999, padding: '4px 12px', fontSize: 12, fontWeight: 650,
                      }}>
                        Judge: {summary.judge.approved ? 'Approved' : 'Flagged'}
                      </div>
                    )}
                    {summary.judge && !summary.judge.approved && (
                      <p style={{ fontSize: 13, color: 'var(--medium)', marginTop: 0, marginBottom: 10 }}>{summary.judge.explanation}</p>
                    )}
                    <div style={{ whiteSpace: 'pre-wrap', fontSize: 14, lineHeight: 1.6, background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: 14 }}>
                      {summary.summary}
                    </div>
                  </div>
                )}
              </>
            )}
          </>
        )}
      </div>
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

  if (error) return <p style={{ color: 'var(--hard)' }}>{error}</p>
  if (!params) return <p style={{ color: 'var(--muted)' }}>Loading…</p>

  const filtered = params.filter((p) => p.name.toLowerCase().includes(filter.toLowerCase()))

  return (
    <div className="panel">
      <input
        placeholder="Filter parameters…"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        style={{ marginBottom: 12, width: '100%', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '8px 12px', color: 'var(--text)' }}
      />
      <table>
        <thead><tr><th>Name</th><th>Value</th><th>Default?</th></tr></thead>
        <tbody>
          {filtered.map((p) => (
            <tr key={p.name}>
              <td>{p.name}</td>
              <td>{p.value}</td>
              <td>{p.isDefault ? 'Y' : 'N (customized)'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
