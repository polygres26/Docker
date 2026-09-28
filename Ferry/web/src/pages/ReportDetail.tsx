import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { type ReportAnalysis, type UploadedReport, analyzeReport, getReport } from '../api/client'
import { Button, Loading, Notice, PageHeader, Section } from '../ui'
import ReportAnalysisView from './ReportAnalysisView'

export default function ReportDetail() {
  const { id } = useParams<{ id: string }>()
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

  if (error && !report) return <><PageHeader back={{ to: '/reports', label: 'Reports' }} title="Report" /><Notice tone="error">{error}</Notice></>
  if (!report) return <Section><Loading /></Section>

  return (
    <>
      <PageHeader
        back={{ to: '/reports', label: 'Reports' }}
        title={report.name}
        subtitle={`${report.filename} · ${report.dialect.replace('_', ' ')} · ${(report.textLength / 1024).toFixed(1)} KB · uploaded ${new Date(report.uploadedAt).toLocaleString()}`}
        actions={
          <Button variant="primary" onClick={handleAnalyze} disabled={analyzing}>
            {analyzing ? 'Analyzing…' : analysis ? 'Re-analyze' : 'Analyze report'}
          </Button>
        }
      />

      <Notice tone="warn" title="Heuristic, not deterministic">
        This analysis comes from a model reading the uploaded report's text -- there's no live
        database to run catalog queries against, unlike the Connections flow. Treat findings
        here as a starting point for a deeper look, not a final migration score.
      </Notice>

      {error && <Notice tone="error">{error}</Notice>}

      {analysis && <ReportAnalysisView analysis={analysis} />}
    </>
  )
}
