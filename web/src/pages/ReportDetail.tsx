import { useEffect, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { type ReportAnalysis, type UploadedReport, analyzeReport, getReport } from '../api/client'
import ReportAnalysisView from './ReportAnalysisView'

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

      {analysis && <ReportAnalysisView analysis={analysis} />}
    </div>
  )
}
