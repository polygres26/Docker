import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  type ReportAnalysis, type UploadedReport,
  analyzeReportsBatch, deleteReport, listReports, uploadReport,
} from '../api/client'
import ReportAnalysisView from './ReportAnalysisView'
import DmsTabs from '../components/DmsTabs'
import {
  Button, Check, DataTable, EmptyState, Field, FormActions, Input, Notice, PageHeader, Section, Select, StatusPill, Tag,
} from '../ui'
import { table } from '../ui/styles'
import { FileText, Upload } from 'lucide-react'
import styles from './Reports.module.css'

const DIALECTS = ['ORACLE', 'MYSQL', 'MARIADB', 'SQL_SERVER']

/**
 * Reports are uploaded under a database name (e.g. "Orders DB") with one or more files each --
 * AWR snapshots for Oracle, a MySQL performance report, a SQL Server DMV/Query Store export, or
 * several time-spaced snapshots of any of those. There's no separate "group" row in the backend
 * (UploadedReport is flat, one row per file) -- the group is the shared name prefix that
 * handleUpload writes as "<database name> — <file.name>" for multi-file uploads. This groups the
 * flat list back into that shape for display so a customer sees "Orders DB (2 files)" rather than
 * two unrelated-looking rows.
 */
function groupByDatabase(reports: UploadedReport[]): { database: string; dialect: string; reports: UploadedReport[] }[] {
  const groups = new Map<string, { database: string; dialect: string; reports: UploadedReport[] }>()
  for (const r of reports) {
    const sepIndex = r.name.indexOf(' — ')
    const database = sepIndex === -1 ? r.name : r.name.slice(0, sepIndex)
    const key = database + ' ' + r.dialect
    if (!groups.has(key)) groups.set(key, { database, dialect: r.dialect, reports: [] })
    groups.get(key)!.reports.push(r)
  }
  return [...groups.values()].sort((a, b) =>
    Math.max(...b.reports.map((r) => +new Date(r.uploadedAt))) - Math.max(...a.reports.map((r) => +new Date(r.uploadedAt))))
}

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

  const groups = groupByDatabase(reports)

  return (
    <>
      <DmsTabs />
      <PageHeader
        title="Reports"
        subtitle="For customers who won't share a live connect string: upload an AWR/performance/DMV report instead, named after its source database, for an LLM-assisted migration read."
        actions={
          <>
            {selected.size > 0 && (
              <Button variant="secondary" onClick={handleAnalyzeSelected} disabled={batchAnalyzing}>
                {batchAnalyzing ? 'Analyzing…' : `Analyze selected (${selected.size})`}
              </Button>
            )}
            {!showForm && <Button variant="primary" onClick={() => setShowForm(true)}><Upload size={15} strokeWidth={2} aria-hidden />Upload reports</Button>}
          </>
        }
      />

      {showForm && (
        <Section title="Upload reports">
          <form onSubmit={handleUpload}>
            <Field label={<>Database name {files.length > 1 && <span className={styles.subtle}>(groups these {files.length} files together)</span>}</>} htmlFor="name">
              <Input id="name" value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. Orders DB" />
            </Field>
            <Field label="Source database" htmlFor="dialect">
              <Select id="dialect" value={dialect} onChange={(e) => setDialect(e.target.value)}>
                {DIALECTS.map((d) => <option key={d} value={d}>{d.replace('_', ' ')}</option>)}
              </Select>
            </Field>
            <Field
              label="Report file(s) (AWR HTML, text export, CSV, ...) — select multiple to upload them all at once"
              htmlFor="file"
              hint={files.length > 0 ? `${files.length} file${files.length === 1 ? '' : 's'} selected: ${files.map((f) => f.name).join(', ')}` : undefined}
            >
              <Input
                id="file"
                ref={fileInputRef}
                type="file"
                multiple
                onChange={(e) => setFiles(e.target.files ? Array.from(e.target.files) : [])}
              />
            </Field>
            {uploadProgress && <Notice>Uploading {uploadProgress.done} of {uploadProgress.total}…</Notice>}
            {error && <Notice tone="error">{error}</Notice>}
            <FormActions>
              <Button variant="primary" type="submit" disabled={uploading}>
                {uploading ? 'Uploading…' : files.length > 1 ? `Upload ${files.length} files` : 'Upload'}
              </Button>
              <Button onClick={() => setShowForm(false)}>Cancel</Button>
            </FormActions>
          </form>
        </Section>
      )}

      {groups.length === 0 && (
        <Section flush>
          <EmptyState icon={FileText} title="No reports uploaded yet" action={!showForm && <Button variant="primary" onClick={() => setShowForm(true)}>Upload reports</Button>}>
            Uploaded AWR / performance reports are grouped by database here.
          </EmptyState>
        </Section>
      )}

      {groups.map((group) => (
        <Section
          key={group.database + ' ' + group.dialect}
          title={<span className={styles.groupTitle}>{group.database} <Tag>{group.dialect.toLowerCase().replace('_', ' ')}</Tag></span>}
          meta={`${group.reports.length} file${group.reports.length === 1 ? '' : 's'}`}
          flush
        >
          <DataTable caption={`Reports for ${group.database}`}>
            <thead>
              <tr>
                <th scope="col" className={table.narrow}><span className="sr-only">Select</span></th>
                <th scope="col">File</th>
                <th scope="col" className={table.num}>Size</th>
                <th scope="col">Uploaded</th>
                <th scope="col">Status</th>
                <th scope="col"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {group.reports.map((r) => (
                <tr key={r.id}>
                  <td><Check checked={selected.has(r.id)} onChange={() => toggleSelected(r.id)} aria-label={`Select ${r.filename}`}>{null}</Check></td>
                  <td><Link to={`/reports/${r.id}`} className={table.main}>{r.filename}</Link></td>
                  <td className={table.num}>{(r.textLength / 1024).toFixed(1)} KB</td>
                  <td className={styles.when}>{new Date(r.uploadedAt).toLocaleString()}</td>
                  <td>{r.analyzedAt ? <StatusPill tone="green">Analyzed</StatusPill> : <StatusPill>Not analyzed</StatusPill>}</td>
                  <td><div className={styles.actions}><Button variant="danger" size="sm" onClick={() => handleDelete(r.id)} aria-label={`Delete ${r.filename}`}>Delete</Button></div></td>
                </tr>
              ))}
            </tbody>
          </DataTable>
        </Section>
      ))}

      {batchError && <Notice tone="error">{batchError}</Notice>}
      {batchAnalysis && (
        <>
          <h2 className={styles.combined}>Combined analysis — {selected.size} report{selected.size === 1 ? '' : 's'}</h2>
          <Notice tone="warn" title="Heuristic, not deterministic">
            Synthesized by the model across the selected reports' text -- a starting point, not a
            final migration score.
          </Notice>
          <ReportAnalysisView analysis={batchAnalysis} />
        </>
      )}
    </>
  )
}
