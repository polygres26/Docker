import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { type ReportAnalysis, type UploadedReport, analyzeReport, getReport } from '../api/client'

function tierStyle(tier: string): { bg: string; fg: string } {
  if (tier === 'EASY') return { bg: 'var(--easy-soft)', fg: 'var(--accent-strong)' }
  if (tier === 'MEDIUM') return { bg: 'var(--medium-soft)', fg: 'var(--medium)' }
  return { bg: 'var(--hard-soft)', fg: 'var(--hard)' }
}

function severityStyle(severity: string): { bg: string; fg: string } {
  if (severity === 'HIGH') return { bg: 'var(--hard-soft)', fg: 'var(--hard)' }
  if (severity === 'MEDIUM') return { bg: 'var(--medium-soft)', fg: 'var(--medium)' }
  return { bg: 'var(--easy-soft)', fg: 'var(--easy)' }
}

export default function ReportDetail() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const [report, setReport] = useState<UploadedReport | null>(null)
  const [analysis, setAnalysis] = useState<ReportAnalysis | null>(null)
  const [analyzing, setAnalyzing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    getReport(id).then((r) => {
      setReport(r)
      if (r.analysisJson) setAnalysis(JSON.parse(r.analysisJson))
    }).catch((e) => setError(e instanceof Error ? e.message : String(e)))
  }, [id])

  async function handleAnalyze() {
    if (!id) return
    setAnalyzing(true); setError(null)
    try {
      setAnalysis(await analyzeReport(id))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setAnalyzing(false)
    }
  }

  if (error && !report) return <p style={{ color: 'var(--hard)' }}>{error}</p>
  if (!report) return <p style={{ color: 'var(--muted)' }}>Loading…</p>

  return (
    <div style={{ maxWidth: 780 }}>
      <button onClick={() => navigate('/reports')} style={{ marginBottom: 16, background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer', fontSize: 13 }}>
        ← Reports
      </button>
      <h1 style={{ marginBottom: 2, fontSize: 22 }}>{report.name}</h1>
      <p style={{ color: 'var(--muted)', marginTop: 0, fontSize: 13 }}>
        {report.filename} · {report.dialect.replace('_', ' ')} · {(report.textLength / 1024).toFixed(1)} KB · uploaded {new Date(report.uploadedAt).toLocaleString()}
      </p>

      <div className="panel" style={{ marginBottom: 20, borderColor: 'var(--medium)' }}>
        <strong style={{ color: 'var(--medium)' }}>Heuristic, not deterministic</strong>
        <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--muted)' }}>
          This analysis comes from a model reading the uploaded report's text -- there's no live
          database to run catalog queries against, unlike the Connections flow. Treat findings
          here as a starting point for a deeper look, not a final migration score.
        </p>
      </div>

      <button className="primary" onClick={handleAnalyze} disabled={analyzing} style={{ marginBottom: 20 }}>
        {analyzing ? 'Analyzing…' : analysis ? 'Re-analyze' : 'Analyze report'}
      </button>

      {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}

      {analysis && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <div className="panel">
            <span className="tier-badge" style={{ background: tierStyle(analysis.tier).bg, color: tierStyle(analysis.tier).fg }}>
              {analysis.tier}
            </span>
            <p style={{ marginTop: 10, marginBottom: 4 }}>{analysis.tierReason}</p>
            {analysis.sourceVersion && <p style={{ color: 'var(--muted)', fontSize: 13, margin: 0 }}>{analysis.sourceVersion}</p>}
            {analysis.judgeVerdict && (
              <div style={{
                display: 'inline-flex', alignItems: 'center', gap: 6, marginTop: 10,
                background: analysis.judgeVerdict.approved ? 'var(--easy-soft)' : 'var(--medium-soft)',
                color: analysis.judgeVerdict.approved ? 'var(--accent-strong)' : 'var(--medium)',
                borderRadius: 999, padding: '4px 12px', fontSize: 12, fontWeight: 650,
              }}>
                Judge: {analysis.judgeVerdict.approved ? 'Approved' : 'Flagged'}
              </div>
            )}
          </div>

          {analysis.findings.length > 0 && (
            <div className="panel">
              <h3 style={{ marginTop: 0 }}>Findings</h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {analysis.findings.map((f, i) => (
                  <div key={i} style={{ display: 'flex', alignItems: 'flex-start', gap: 12, padding: '10px 12px', background: 'var(--bg)', borderRadius: 10, border: '1px solid var(--border)' }}>
                    <span style={{ background: severityStyle(f.severity).bg, color: severityStyle(f.severity).fg, fontSize: 11, fontWeight: 650, borderRadius: 999, padding: '3px 10px', flexShrink: 0, width: 64, textAlign: 'center' }}>
                      {f.severity}
                    </span>
                    <div>
                      <div style={{ fontWeight: 600 }}>{f.feature}</div>
                      <div style={{ color: 'var(--muted)', fontSize: 13 }}>{f.note}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {analysis.topWorkload.length > 0 && (
            <div className="panel">
              <h3 style={{ marginTop: 0 }}>Workload observed in the report</h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {analysis.topWorkload.map((w, i) => (
                  <div key={i} style={{ padding: '10px 12px', background: 'var(--bg)', borderRadius: 10, border: '1px solid var(--border)' }}>
                    <div style={{ fontSize: 14 }}>{w.description}</div>
                    <div style={{ color: 'var(--muted)', fontSize: 12.5, fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', marginTop: 2 }}>{w.detail}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {analysis.caveats.length > 0 && (
            <div className="panel" style={{ borderColor: 'var(--border)' }}>
              <h3 style={{ marginTop: 0 }}>Caveats</h3>
              <ul style={{ marginBottom: 0, color: 'var(--muted)', fontSize: 13 }}>
                {analysis.caveats.map((c, i) => <li key={i}>{c}</li>)}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
