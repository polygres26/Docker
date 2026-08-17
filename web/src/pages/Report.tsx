import { useLocation, useNavigate } from 'react-router-dom'
import type { ScanResult } from '../api/client'

function tierColor(tier: string): string {
  if (tier.startsWith('EASY')) return 'var(--easy)'
  if (tier.startsWith('MEDIUM')) return 'var(--medium)'
  return 'var(--hard)'
}

export default function Report() {
  const location = useLocation()
  const navigate = useNavigate()
  const result = location.state as ScanResult | undefined

  if (!result) {
    return (
      <div className="app-shell">
        <p>No scan result to show. <a href="/">Run a scan</a> first.</p>
      </div>
    )
  }

  const { snapshot, score } = result

  return (
    <div className="app-shell">
      <button onClick={() => navigate('/')} style={{ marginBottom: 16, background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer' }}>
        ← New scan
      </button>

      <h1>Migration Assessment</h1>
      {snapshot.sourceVersion && <p style={{ color: 'var(--muted)' }}>{snapshot.sourceVersion}</p>}

      <div className="panel" style={{ marginBottom: 20 }}>
        <span className="tier-badge" style={{ background: tierColor(score.tier), color: '#111' }}>
          {score.tier.split(' -- ')[0]}
        </span>
        <p style={{ marginTop: 12 }}>{score.tier.split(' -- ')[1]}</p>
        <p style={{ color: 'var(--muted)' }}>Total difficulty score: <strong>{score.totalScore}</strong></p>
      </div>

      {score.warnings.length > 0 && (
        <div className="panel" style={{ marginBottom: 20, borderColor: 'var(--medium)' }}>
          <strong>Warnings</strong>
          <ul>
            {score.warnings.map((w, i) => <li key={i}>{w}</li>)}
          </ul>
        </div>
      )}

      <div className="panel" style={{ marginBottom: 20 }}>
        <h3>Feature inventory</h3>
        <table>
          <tbody>
            <tr><td>Tables</td><td>{snapshot.tableCount}</td></tr>
            <tr><td>Views</td><td>{snapshot.viewCount}</td></tr>
            <tr><td>Materialized views</td><td>{snapshot.materializedViewCount}</td></tr>
            <tr><td>Sequences</td><td>{snapshot.sequenceCount}</td></tr>
            <tr><td>Triggers (simple / complex)</td><td>{snapshot.simpleTriggerCount} / {snapshot.complexTriggerCount}</td></tr>
            <tr><td>Packages</td><td>{snapshot.packageCount}</td></tr>
            <tr><td>Standalone procedures / functions</td><td>{snapshot.standaloneProcedureCount} / {snapshot.standaloneFunctionCount}</td></tr>
            <tr><td>Database links</td><td>{snapshot.dbLinkCount}</td></tr>
            <tr><td>Scheduled jobs</td><td>{snapshot.scheduledJobCount}</td></tr>
            <tr><td>Partitioned tables</td><td>{snapshot.partitionedTableCount}</td></tr>
          </tbody>
        </table>
      </div>

      <div className="panel">
        <h3>Scoring detail</h3>
        <table>
          <thead>
            <tr><th>Feature</th><th>Count</th><th>Weight</th><th>Points</th><th>Note</th></tr>
          </thead>
          <tbody>
            {score.findings.map((f, i) => (
              <tr key={i}>
                <td>{f.feature}</td>
                <td>{f.count}</td>
                <td>{f.weightPerUnit}</td>
                <td>{f.points}</td>
                <td style={{ color: 'var(--muted)' }}>{f.note}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
