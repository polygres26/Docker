import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  type ReportAnalysis, type UploadedReport,
  analyzeReportsBatch, deleteReport, listReports, uploadReport,
} from '../api/client'
import ReportAnalysisView from './ReportAnalysisView'

const DIALECTS = ['ORACLE', 'MYSQL', 'MARIADB', 'SQL_SERVER']

export default function Reports() {
  const [reports, setReports] = useState<UploadedReport[]>([])
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [showForm, setShowForm] = useState(false)
  const [name, setName] = useState('')
  const [dialect, setDialect] = useState('ORACLE')
  const [files, setFiles] = useState<File[]>([])
  const [uploading, setUploading] = useState(false)
  const [uploadProgress, setUploadProgress] = useState<{ done: number; total: number } | null>(null)
  const [error, setError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [batchAnalysis, setBatchAnalysis] = useState<ReportAnalysis | null>(null)
  const [batchAnalyzing, setBatchAnalyzing] = useState(false)
  const [batchError, setBatchError] = useState<string | null>(null)

  async function refresh() {
    setReports(await listReports())
  }

  useEffect(() => { refresh() }, [])

  async function handleUpload(e: React.FormEvent) {
    e.preventDefault()
    if (files.length === 0) { setError('Choose at least one file.'); return }
    setUploading(true); setError(null); setUploadProgress({ done: 0, total: files.length })
    try {
      // Sequential, not parallel -- keeps the "N of M uploaded" progress meaningful and avoids
      // hammering the server with a burst of large-file writes at once.
      for (let i = 0; i < files.length; i++) {
        const file = files[i]
        const label = files.length === 1 ? (name || file.name) : `${name ? name + ' — ' : ''}${file.name}`
        await uploadReport(file, label, dialect)
        setUploadProgress({ done: i + 1, total: files.length })
      }
      setShowForm(false); setName(''); setFiles([])
      if (fileInputRef.current) fileInputRef.current.value = ''
      await refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setUploading(false); setUploadProgress(null)
    }
  }

  async function handleDelete(id: string) {
    await deleteReport(id)
    setSelected((prev) => { const next = new Set(prev); next.delete(id); return next })
    await refresh()
  }

  function toggleSelected(id: string) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
    setBatchAnalysis(null)
  }

  async function handleAnalyzeSelected() {
    setBatchAnalyzing(true); setBatchError(null); setBatchAnalysis(null)
    try {
      setBatchAnalysis(await analyzeReportsBatch([...selected]))
    } catch (e) {
      setBatchError(e instanceof Error ? e.message : String(e))
    } finally {
      setBatchAnalyzing(false)
    }
  }

  return (
    <div style={{ maxWidth: 780 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Reports</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 20 }}>
        For customers who won't share a live connect string: upload a performance/workload report
        instead -- an Oracle AWR report, a MySQL performance report, or a SQL Server DMV/Query
        Store export -- and get an LLM-assisted migration read on it. Upload several at once (e.g.
        one per system, or several snapshots over time) and select multiple below for one combined
        analysis. This is a different kind of signal than the Connections flow: there's no live
        database to query, so findings here come from a model reading the report's text, not
        deterministic catalog scans. Treat it as a starting point, not a final assessment.
      </p>

      <div className="panel" style={{ marginBottom: 16 }}>
        {reports.length === 0 && <p style={{ color: 'var(--muted)' }}>No reports uploaded yet.</p>}
        {reports.map((r) => (
          <div key={r.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: '1px solid var(--border)' }}>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
              <input
                type="checkbox"
                checked={selected.has(r.id)}
                onChange={() => toggleSelected(r.id)}
                style={{ marginTop: 4 }}
              />
              <div>
                <Link to={`/reports/${r.id}`} style={{ color: 'var(--accent)', fontWeight: 600, textDecoration: 'none' }}>
                  {r.name}
                </Link>
                <span style={{ fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace', fontSize: 10, letterSpacing: '0.04em', textTransform: 'uppercase', background: 'var(--accent-soft)', color: 'var(--accent-strong)', borderRadius: 5, padding: '3px 7px', marginLeft: 8 }}>
                  {r.dialect.toLowerCase().replace('_', ' ')}
                </span>
                <div style={{ color: 'var(--muted)', fontSize: 13 }}>
                  {r.filename} · {(r.textLength / 1024).toFixed(1)} KB · uploaded {new Date(r.uploadedAt).toLocaleString()}
                  {r.analyzedAt && <> · analyzed</>}
                </div>
              </div>
            </div>
            <button onClick={() => handleDelete(r.id)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 10px', color: 'var(--hard)', cursor: 'pointer', flexShrink: 0 }}>
              Delete
            </button>
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 10, marginBottom: 20, flexWrap: 'wrap' }}>
        {!showForm && <button className="primary" onClick={() => setShowForm(true)}>Upload reports</button>}
        {selected.size > 0 && (
          <button className="primary" onClick={handleAnalyzeSelected} disabled={batchAnalyzing}>
            {batchAnalyzing ? 'Analyzing…' : `Analyze selected (${selected.size})`}
          </button>
        )}
      </div>

      {showForm && (
        <form className="panel" onSubmit={handleUpload} style={{ marginBottom: 20 }}>
          <div className="field">
            <label htmlFor="name">Name {files.length > 1 && <span style={{ color: 'var(--muted)', fontWeight: 400 }}>(prefix — each file's own name is appended)</span>}</label>
            <input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Claims DB" />
          </div>
          <div className="field">
            <label htmlFor="dialect">Source database</label>
            <select
              id="dialect"
              value={dialect}
              onChange={(e) => setDialect(e.target.value)}
              style={{ background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 12px', color: 'var(--text)', fontSize: 14 }}
            >
              {DIALECTS.map((d) => <option key={d} value={d}>{d.replace('_', ' ')}</option>)}
            </select>
          </div>
          <div className="field">
            <label htmlFor="file">Report file(s) (AWR HTML, text export, CSV, ...) — select multiple to upload them all at once</label>
            <input
              id="file"
              ref={fileInputRef}
              type="file"
              multiple
              onChange={(e) => setFiles(e.target.files ? Array.from(e.target.files) : [])}
            />
            {files.length > 0 && (
              <p style={{ color: 'var(--muted)', fontSize: 12.5, marginTop: 6, marginBottom: 0 }}>
                {files.length} file{files.length === 1 ? '' : 's'} selected: {files.map((f) => f.name).join(', ')}
              </p>
            )}
          </div>
          {uploadProgress && (
            <p style={{ color: 'var(--muted)', fontSize: 13 }}>Uploading {uploadProgress.done} of {uploadProgress.total}…</p>
          )}
          {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}
          <div style={{ display: 'flex', gap: 10 }}>
            <button className="primary" type="submit" disabled={uploading}>
              {uploading ? 'Uploading…' : files.length > 1 ? `Upload ${files.length} files` : 'Upload'}
            </button>
            <button type="button" onClick={() => setShowForm(false)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 18px', color: 'var(--text)', cursor: 'pointer' }}>
              Cancel
            </button>
          </div>
        </form>
      )}

      {batchError && <p style={{ color: 'var(--hard)' }}>{batchError}</p>}
      {batchAnalysis && (
        <div>
          <h2 style={{ fontSize: 18, marginBottom: 12 }}>Combined analysis — {selected.size} report{selected.size === 1 ? '' : 's'}</h2>
          <div className="panel" style={{ marginBottom: 20, borderColor: 'var(--medium)' }}>
            <strong style={{ color: 'var(--medium)' }}>Heuristic, not deterministic</strong>
            <p style={{ margin: '4px 0 0', fontSize: 13, color: 'var(--muted)' }}>
              Synthesized by the model across the selected reports' text -- a starting point, not a
              final migration score.
            </p>
          </div>
          <ReportAnalysisView analysis={batchAnalysis} />
        </div>
      )}
    </div>
  )
}
