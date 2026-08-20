import { useEffect, useState } from 'react'
import {
  type Connection, type SizingRecommendation, type UploadedReport,
  listConnections, listReports, runConnectionSizing, runReportsSizing,
} from '../api/client'
import AdvisorTabs from '../components/AdvisorTabs'

function tierStyle(tier: string): { bg: string; fg: string } {
  if (tier === 'SMALL') return { bg: 'var(--easy-soft)', fg: 'var(--accent-strong)' }
  if (tier === 'MEDIUM') return { bg: 'var(--medium-soft)', fg: 'var(--medium)' }
  return { bg: 'var(--hard-soft)', fg: 'var(--hard)' }
}

export default function Sizing() {
  const [connections, setConnections] = useState<Connection[]>([])
  const [reports, setReports] = useState<UploadedReport[]>([])
  const [selectedConnection, setSelectedConnection] = useState('')
  const [selectedReports, setSelectedReports] = useState<Set<string>>(new Set())

  const [result, setResult] = useState<SizingRecommendation | null>(null)
  const [resultSource, setResultSource] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    listConnections().then(setConnections).catch(() => {})
    listReports().then(setReports).catch(() => {})
  }, [])

  function toggleReport(id: string) {
    setSelectedReports((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }

  async function handleConnectionSizing() {
    if (!selectedConnection) return
    setLoading(true); setError(null); setResult(null)
    try {
      setResult(await runConnectionSizing(selectedConnection))
      setResultSource(connections.find((c) => c.id === selectedConnection)?.name ?? 'connection')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  async function handleReportSizing() {
    if (selectedReports.size === 0) return
    setLoading(true); setError(null); setResult(null)
    try {
      setResult(await runReportsSizing([...selectedReports]))
      setResultSource(`${selectedReports.size} uploaded report${selectedReports.size === 1 ? '' : 's'}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ maxWidth: 780 }}>
      <AdvisorTabs />
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Sizing</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 20 }}>
        A starting-point Postgres instance shape (vCPUs, memory, storage, IOPS, connections) built
        from whatever signal is available: schema size and captured workload from a live
        connection, or CPU/memory/data-size hints pulled from an uploaded report. This is a
        rules-of-thumb calculator, not a substitute for real load testing before go-live — every
        number below comes with the reasoning that produced it.
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 20 }}>
        <div className="panel">
          <h3 style={{ marginTop: 0 }}>From a connection</h3>
          <p style={{ color: 'var(--muted)', fontSize: 13 }}>Uses a fresh schema-size scan + workload capture.</p>
          <select
            value={selectedConnection}
            onChange={(e) => setSelectedConnection(e.target.value)}
            style={{ width: '100%', background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 12px', color: 'var(--text)', fontSize: 14, marginBottom: 12 }}
          >
            <option value="">Select a connection…</option>
            {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
          </select>
          <button className="primary" onClick={handleConnectionSizing} disabled={!selectedConnection || loading}>
            {loading ? 'Calculating…' : 'Calculate sizing'}
          </button>
        </div>

        <div className="panel">
          <h3 style={{ marginTop: 0 }}>From uploaded reports</h3>
          <p style={{ color: 'var(--muted)', fontSize: 13 }}>Select one or more; combined if several.</p>
          <div style={{ maxHeight: 130, overflowY: 'auto', marginBottom: 12 }}>
            {reports.length === 0 && <p style={{ color: 'var(--muted)', fontSize: 13 }}>No reports uploaded yet.</p>}
            {reports.map((r) => (
              <label key={r.id} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, padding: '4px 0' }}>
                <input type="checkbox" checked={selectedReports.has(r.id)} onChange={() => toggleReport(r.id)} />
                {r.name}
              </label>
            ))}
          </div>
          <button className="primary" onClick={handleReportSizing} disabled={selectedReports.size === 0 || loading}>
            {loading ? 'Calculating…' : 'Calculate sizing'}
          </button>
        </div>
      </div>

      {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}

      {result && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <div className="panel">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', flexWrap: 'wrap', gap: 10 }}>
              <div>
                <span className="tier-badge" style={{ background: tierStyle(result.tier).bg, color: tierStyle(result.tier).fg }}>
                  {result.tier}
                </span>
                {resultSource && <span style={{ color: 'var(--muted)', fontSize: 13, marginLeft: 10 }}>from {resultSource}</span>}
              </div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))', gap: 12, marginTop: 16 }}>
              {[
                ['vCPUs', result.vCpus],
                ['Memory', `${result.memoryGB} GB`],
                ['Storage', `${result.storageGB} GB`],
                ['Storage IOPS', result.storageIops.toLocaleString()],
                ['max_connections', result.maxConnections],
              ].map(([label, value]) => (
                <div key={label as string} style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 10, padding: '12px 14px' }}>
                  <div style={{ fontSize: 22, fontWeight: 800, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace' }}>{value}</div>
                  <div style={{ color: 'var(--muted)', fontSize: 12 }}>{label}</div>
                </div>
              ))}
            </div>
          </div>

          {result.caveats.length > 0 && (
            <div className="panel" style={{ borderColor: 'var(--medium)' }}>
              <strong style={{ color: 'var(--medium)' }}>Caveats</strong>
              <ul style={{ marginBottom: 0, fontSize: 13, color: 'var(--muted)' }}>
                {result.caveats.map((c, i) => <li key={i}>{c}</li>)}
              </ul>
            </div>
          )}

          <div className="panel">
            <h3 style={{ marginTop: 0 }}>Rationale</h3>
            <ul style={{ marginBottom: 0, fontSize: 13.5, lineHeight: 1.7 }}>
              {result.rationale.map((r, i) => <li key={i}>{r}</li>)}
            </ul>
          </div>
        </div>
      )}
    </div>
  )
}
