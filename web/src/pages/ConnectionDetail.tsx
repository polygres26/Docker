import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  type ParameterInfo,
  type ScanResult,
  getObjectDetail,
  getObjects,
  getParameters,
  runConnectionScan,
} from '../api/client'

type Tab = 'objects' | 'parameters' | 'assessment'

export default function ConnectionDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [tab, setTab] = useState<Tab>('objects')

  return (
    <div className="app-shell">
      <button onClick={() => navigate('/connections')} style={{ marginBottom: 16, background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer' }}>
        ← Connections
      </button>
      <h1>Connection detail</h1>

      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        {(['objects', 'parameters', 'assessment'] as Tab[]).map((t) => (
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

      {id && tab === 'objects' && <ObjectsTab id={id} />}
      {id && tab === 'parameters' && <ParametersTab id={id} />}
      {id && tab === 'assessment' && <AssessmentTab id={id} />}
    </div>
  )
}

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

function AssessmentTab({ id }: { id: string }) {
  const [result, setResult] = useState<ScanResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function runScan() {
    setLoading(true); setError(null)
    try {
      setResult(await runConnectionScan(id))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <button className="primary" onClick={runScan} disabled={loading}>
        {loading ? 'Scanning…' : 'Run migration assessment'}
      </button>
      {error && <p style={{ color: 'var(--hard)', marginTop: 12 }}>{error}</p>}
      {result && (
        <div className="panel" style={{ marginTop: 16 }}>
          <p><strong>{result.score.tier}</strong></p>
          <p style={{ color: 'var(--muted)' }}>Total score: {result.score.totalScore}</p>
          <table>
            <thead><tr><th>Feature</th><th>Count</th><th>Points</th></tr></thead>
            <tbody>
              {result.score.findings.map((f, i) => (
                <tr key={i}><td>{f.feature}</td><td>{f.count}</td><td>{f.points}</td></tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
