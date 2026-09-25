import { useEffect, useState } from 'react'
import {
  type Connection, type SizingRecommendation, type UploadedReport,
  listConnections, listReports, runConnectionSizing, runReportsSizing,
} from '../api/client'
import DmsTabs from '../components/DmsTabs'
import { tierTone } from '../lib/tone'
import {
  Bullets, Button, Check, KpiStrip, Notice, PageHeader, Section, Select, Split, Stack, StatusPill,
} from '../ui'
import { ui } from '../ui/styles'
import styles from './Sizing.module.css'

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
    <>
      <DmsTabs />
      <PageHeader
        title="Sizing"
        subtitle="A starting-point Postgres instance shape (vCPUs, memory, storage, IOPS, connections) built from whatever signal is available: schema size and captured workload from a live connection, or CPU/memory/data-size hints pulled from an uploaded report. This is a rules-of-thumb calculator, not a substitute for real load testing before go-live — every number below comes with the reasoning that produced it."
      />

      <Split>
        <Section title="From a connection" meta="Uses a fresh schema-size scan + workload capture.">
          <div className={styles.body}>
            <label className="sr-only" htmlFor="sizing-connection">Connection</label>
            <Select id="sizing-connection" value={selectedConnection} onChange={(e) => setSelectedConnection(e.target.value)}>
              <option value="">Select a connection…</option>
              {connections.map((c) => <option key={c.id} value={c.id}>{c.name}</option>)}
            </Select>
            <div>
              <Button variant="primary" onClick={handleConnectionSizing} disabled={!selectedConnection || loading}>
                {loading ? 'Calculating…' : 'Calculate sizing'}
              </Button>
            </div>
          </div>
        </Section>

        <Section title="From uploaded reports" meta="Select one or more; combined if several.">
          <div className={styles.body}>
            <div className={styles.reportList}>
              {reports.length === 0 && <span className={ui.muted}>No reports uploaded yet.</span>}
              {reports.map((r) => (
                <Check key={r.id} checked={selectedReports.has(r.id)} onChange={() => toggleReport(r.id)}>{r.name}</Check>
              ))}
            </div>
            <div>
              <Button variant="primary" onClick={handleReportSizing} disabled={selectedReports.size === 0 || loading}>
                {loading ? 'Calculating…' : 'Calculate sizing'}
              </Button>
            </div>
          </div>
        </Section>
      </Split>

      {error && <Notice tone="error">{error}</Notice>}

      {result && (
        <Stack>
          <KpiStrip
            label="Recommended instance"
            items={[
              { label: 'Tier', value: <StatusPill tone={tierTone(result.tier)}>{result.tier}</StatusPill>, hint: resultSource ? `from ${resultSource}` : undefined },
              { label: 'vCPUs', value: result.vCpus },
              { label: 'Memory', value: `${result.memoryGB} GB` },
              { label: 'Storage', value: `${result.storageGB} GB` },
              { label: 'Storage IOPS', value: result.storageIops.toLocaleString() },
              { label: 'max_connections', value: result.maxConnections },
            ]}
          />

          {result.caveats.length > 0 && (
            <Section title="Caveats" warn>
              <Bullets>{result.caveats.map((c, i) => <li key={i}>{c}</li>)}</Bullets>
            </Section>
          )}

          <Section title="Rationale">
            <ul className={ui.bullets}>
              {result.rationale.map((r, i) => <li key={i}>{r}</li>)}
            </ul>
          </Section>
        </Stack>
      )}
    </>
  )
}
