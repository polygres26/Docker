import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  type Connection,
  type FindingsResult,
  type ParameterInfo,
  getConnection,
  getObjectDetail,
  getObjects,
  getParameters,
  runConnectionFindings,
} from '../api/client'

type Tab = 'findings' | 'objects' | 'parameters'

export default function ConnectionDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [tab, setTab] = useState<Tab>('findings')
  const [connection, setConnection] = useState<Connection | null>(null)

  useEffect(() => {
    if (id) getConnection(id).then(setConnection).catch(() => {})
  }, [id])

  return (
    <div className="app-shell" style={{ maxWidth: 1100 }}>
      <button onClick={() => navigate('/connections')} style={{ marginBottom: 16, background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer' }}>
        ← Connections
      </button>
      <h1 style={{ marginBottom: 2 }}>{connection?.name ?? 'Connection detail'}</h1>
      {connection && <p style={{ color: 'var(--muted)', marginTop: 0, fontSize: 14 }}>{connection.jdbcUrl}</p>}

      <div style={{ display: 'flex', gap: 8, marginBottom: 20 }}>
        {(['findings', 'objects', 'parameters'] as Tab[]).map((t) => (
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

      {id && tab === 'findings' && <FindingsTab id={id} />}
      {id && tab === 'objects' && <ObjectsTab id={id} />}
      {id && tab === 'parameters' && <ParametersTab id={id} />}
    </div>
  )
}

// --- Findings ---------------------------------------------------------

function severityColor(points: number): string {
  if (points >= 15) return 'var(--hard)'
  if (points >= 5) return 'var(--medium)'
  return 'var(--easy)'
}

function severityLabel(points: number): string {
  if (points >= 15) return 'High'
  if (points >= 5) return 'Medium'
  return 'Low'
}

function tierColor(tier: string): string {
  if (tier.startsWith('EASY')) return 'var(--easy)'
  if (tier.startsWith('MEDIUM')) return 'var(--medium)'
  return 'var(--hard)'
}

function formatMicros(micros: number): string {
  const ms = micros / 1000
  if (ms < 1) return '<1 ms'
  if (ms < 1000) return `${ms.toFixed(1)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}

function FindingsTab({ id }: { id: string }) {
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
            <span className="tier-badge" style={{ background: tierColor(score.tier), color: '#111' }}>
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

        <button className="primary" onClick={run} style={{ marginTop: 16 }}>Re-run findings</button>
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
                background: severityColor(f.points), color: '#111', fontSize: 11, fontWeight: 700,
                borderRadius: 999, padding: '3px 10px', flexShrink: 0, width: 64, textAlign: 'center',
              }}>
                {severityLabel(f.points)}
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

      {/* Workload captured */}
      <div className="panel">
        <h3 style={{ marginTop: 0 }}>Workload captured</h3>
        {workloadError && <p style={{ color: 'var(--medium)' }}>{workloadError}</p>}
        {!workloadError && sortedWorkload.length === 0 && <p style={{ color: 'var(--muted)' }}>No cached SQL captured.</p>}
        {sortedWorkload.length > 0 && (
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead><tr><th>SQL</th><th>Executions</th><th>Elapsed</th><th>Module</th></tr></thead>
              <tbody>
                {sortedWorkload.map((s) => (
                  <tr key={s.sqlId}>
                    <td style={{ maxWidth: 480, fontFamily: 'monospace', fontSize: 12 }} title={s.sqlText}>
                      {s.sqlText.length > 100 ? s.sqlText.slice(0, 100) + '…' : s.sqlText}
                    </td>
                    <td>{s.executions.toLocaleString()}</td>
                    <td>{formatMicros(s.elapsedTimeMicros)}</td>
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

  useEffect(() => {
    getObjects(id).then(setObjects).catch((e) => setError(String(e.message ?? e)))
  }, [id])

  async function select(type: string, name: string) {
    setSelected({ type, name })
    setDetail(null)
    try {
      setDetail(await getObjectDetail(id, type, name))
    } catch (e) {
      setDetail({ source: `Error: ${e instanceof Error ? e.message : String(e)}` })
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
              <pre style={{ whiteSpace: 'pre-wrap', fontSize: 13, background: 'var(--bg)', padding: 12, borderRadius: 8, maxHeight: 480, overflowY: 'auto' }}>
                {detail.source || '(empty)'}
              </pre>
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
