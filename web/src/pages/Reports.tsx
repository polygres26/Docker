import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { type UploadedReport, deleteReport, listReports, uploadReport } from '../api/client'

const DIALECTS = ['ORACLE', 'MYSQL', 'MARIADB', 'SQL_SERVER']

export default function Reports() {
  const [reports, setReports] = useState<UploadedReport[]>([])
  const [showForm, setShowForm] = useState(false)
  const [name, setName] = useState('')
  const [dialect, setDialect] = useState('ORACLE')
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  async function refresh() {
    setReports(await listReports())
  }

  useEffect(() => { refresh() }, [])

  async function handleUpload(e: React.FormEvent) {
    e.preventDefault()
    if (!file) { setError('Choose a file first.'); return }
    setUploading(true); setError(null)
    try {
      await uploadReport(file, name || file.name, dialect)
      setShowForm(false); setName(''); setFile(null)
      if (fileInputRef.current) fileInputRef.current.value = ''
      await refresh()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setUploading(false)
    }
  }

  async function handleDelete(id: string) {
    await deleteReport(id)
    await refresh()
  }

  return (
    <div style={{ maxWidth: 720 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>Reports</h1>
      <p style={{ color: 'var(--muted)', fontSize: 14, marginTop: 0, marginBottom: 20 }}>
        For customers who won't share a live connect string: upload a performance/workload report
        instead -- an Oracle AWR report, a MySQL performance report, or a SQL Server DMV/Query
        Store export -- and get an LLM-assisted migration read on it. This is a different kind of
        signal than the Connections flow: there's no live database to query, so findings here come
        from a model reading the report's text, not deterministic catalog scans. Treat it as a
        starting point, not a final assessment.
      </p>

      <div className="panel" style={{ marginBottom: 20 }}>
        {reports.length === 0 && <p style={{ color: 'var(--muted)' }}>No reports uploaded yet.</p>}
        {reports.map((r) => (
          <div key={r.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '10px 0', borderBottom: '1px solid var(--border)' }}>
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
            <button onClick={() => handleDelete(r.id)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 10px', color: 'var(--hard)', cursor: 'pointer' }}>
              Delete
            </button>
          </div>
        ))}
      </div>

      {!showForm && <button className="primary" onClick={() => setShowForm(true)}>Upload report</button>}

      {showForm && (
        <form className="panel" onSubmit={handleUpload}>
          <div className="field">
            <label htmlFor="name">Name</label>
            <input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Claims DB AWR — Aug 2026" />
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
            <label htmlFor="file">Report file (AWR HTML, text export, CSV, ...)</label>
            <input id="file" ref={fileInputRef} type="file" onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
          </div>
          {error && <p style={{ color: 'var(--hard)' }}>{error}</p>}
          <div style={{ display: 'flex', gap: 10 }}>
            <button className="primary" type="submit" disabled={uploading}>{uploading ? 'Uploading…' : 'Upload'}</button>
            <button type="button" onClick={() => setShowForm(false)} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 8, padding: '10px 18px', color: 'var(--text)', cursor: 'pointer' }}>
              Cancel
            </button>
          </div>
        </form>
      )}
    </div>
  )
}
